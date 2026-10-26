package server

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	ordersv1 "github.com/mustafaautomation/grpc-messaging-tests/gen/orders/v1"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/events"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/orders"
)

const token = "test-token"

// fakePublisher records events and can be told to fail or block.
type fakePublisher struct {
	mu     sync.Mutex
	events []events.OrderCreated
	fail   error
	block  bool
}

func (p *fakePublisher) PublishOrderCreated(ctx context.Context, e events.OrderCreated) error {
	p.mu.Lock()
	fail, block := p.fail, p.block
	p.mu.Unlock()
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	if fail != nil {
		return fail
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
	return nil
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

type harness struct {
	client ordersv1.OrderServiceClient
	health healthpb.HealthClient
	store  *orders.Store
	pub    *fakePublisher
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	store := orders.NewStore()
	pub := &fakePublisher{}
	auth := BearerAuth{Token: token}
	gs := grpc.NewServer(grpc.UnaryInterceptor(auth.Unary()), grpc.StreamInterceptor(auth.Stream()))
	ordersv1.RegisterOrderServiceServer(gs, &Service{Store: store, Publisher: pub})
	healthpb.RegisterHealthServer(gs, health.NewServer())
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &harness{client: ordersv1.NewOrderServiceClient(conn), health: healthpb.NewHealthClient(conn), store: store, pub: pub}
}

func authed(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func createReq(key string) *ordersv1.CreateOrderRequest {
	return &ordersv1.CreateOrderRequest{
		IdempotencyKey: key,
		CustomerId:     "cust-1",
		Items:          []*ordersv1.LineItem{{Sku: "BOOK-1", Quantity: 2, UnitPriceCents: 1250}},
	}
}

func wantCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if got := status.Code(err); got != code {
		t.Fatalf("code = %v, want %v (err: %v)", got, code, err)
	}
}

func TestCreateOrderReturnsPendingOrderAndPublishes(t *testing.T) {
	h := newHarness(t)
	resp, err := h.client.CreateOrder(authed(t), createReq("key-create-1"))
	if err != nil {
		t.Fatal(err)
	}
	o := resp.GetOrder()
	if o.GetStatus() != ordersv1.OrderStatus_ORDER_STATUS_PENDING || o.GetTotalCents() != 2500 || resp.GetReplayed() {
		t.Fatalf("unexpected response: %v", resp)
	}
	if h.pub.count() != 1 || h.pub.events[0].OrderID != o.GetId() {
		t.Fatalf("published %d events, want 1 for %s", h.pub.count(), o.GetId())
	}
}

func TestCreateOrderRetryIsReplayedWithoutSecondEvent(t *testing.T) {
	h := newHarness(t)
	first, err := h.client.CreateOrder(authed(t), createReq("key-retry-1"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.client.CreateOrder(authed(t), createReq("key-retry-1"))
	if err != nil {
		t.Fatal(err)
	}
	if !second.GetReplayed() || second.GetOrder().GetId() != first.GetOrder().GetId() {
		t.Fatalf("retry not replayed: %v", second)
	}
	if h.pub.count() != 1 {
		t.Fatalf("published %d events, want 1", h.pub.count())
	}
}

func TestCreateOrderKeyReuseWithDifferentPayloadIsFailedPrecondition(t *testing.T) {
	h := newHarness(t)
	if _, err := h.client.CreateOrder(authed(t), createReq("key-reuse-1")); err != nil {
		t.Fatal(err)
	}
	changed := createReq("key-reuse-1")
	changed.Items[0].Quantity = 3
	_, err := h.client.CreateOrder(authed(t), changed)
	wantCode(t, err, codes.FailedPrecondition)
}

func TestCreateOrderConcurrentRetriesProduceOneOrderAndOneEvent(t *testing.T) {
	h := newHarness(t)
	const n = 20
	var wg sync.WaitGroup
	ids := make(chan string, n)
	for range n {
		wg.Go(func() {
			resp, err := h.client.CreateOrder(authed(t), createReq("key-concurrent"))
			if err != nil {
				t.Error(err)
				return
			}
			ids <- resp.GetOrder().GetId()
		})
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 || h.pub.count() != 1 {
		t.Fatalf("distinct orders = %d, events = %d; want 1 and 1", len(seen), h.pub.count())
	}
}

func TestCreateOrderInvalidArgument(t *testing.T) {
	h := newHarness(t)
	bad := createReq("key-bad-1")
	bad.Items[0].Quantity = 0
	_, err := h.client.CreateOrder(authed(t), bad)
	wantCode(t, err, codes.InvalidArgument)
	if msg := status.Convert(err).Message(); msg != "items[0].quantity: must be 1-1000" {
		t.Fatalf("message = %q", msg)
	}
	if h.pub.count() != 0 {
		t.Fatal("invalid request must not publish")
	}
}

func TestCreateOrderPublishFailureIsUnavailableAndRetryable(t *testing.T) {
	h := newHarness(t)
	h.pub.fail = errors.New("nats: no responders")
	_, err := h.client.CreateOrder(authed(t), createReq("key-unavail"))
	wantCode(t, err, codes.Unavailable)

	h.pub.mu.Lock()
	h.pub.fail = nil
	h.pub.mu.Unlock()
	resp, err := h.client.CreateOrder(authed(t), createReq("key-unavail"))
	if err != nil {
		t.Fatalf("retry after outage: %v", err)
	}
	if resp.GetReplayed() {
		t.Fatal("the failed attempt must not count as the original")
	}
}

func TestCreateOrderClientDeadlineIsDeadlineExceeded(t *testing.T) {
	h := newHarness(t)
	h.pub.block = true
	ctx, cancel := context.WithTimeout(authed(t), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := h.client.CreateOrder(ctx, createReq("key-deadline"))
	wantCode(t, err, codes.DeadlineExceeded)
	if time.Since(start) > 2*time.Second {
		t.Fatal("deadline was not honoured promptly")
	}
}

func TestGetOrder(t *testing.T) {
	h := newHarness(t)
	created, err := h.client.CreateOrder(authed(t), createReq("key-get-1"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.client.GetOrder(authed(t), &ordersv1.GetOrderRequest{Id: created.GetOrder().GetId()})
	if err != nil || got.GetOrder().GetId() != created.GetOrder().GetId() {
		t.Fatalf("get: %v %v", got, err)
	}
	_, err = h.client.GetOrder(authed(t), &ordersv1.GetOrderRequest{Id: "ord_missing"})
	wantCode(t, err, codes.NotFound)
	_, err = h.client.GetOrder(authed(t), &ordersv1.GetOrderRequest{})
	wantCode(t, err, codes.InvalidArgument)
}

func TestAuthentication(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := h.client.GetOrder(ctx, &ordersv1.GetOrderRequest{Id: "x"})
	wantCode(t, err, codes.Unauthenticated)

	wrong := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer nope")
	_, err = h.client.CreateOrder(wrong, createReq("key-auth-1"))
	wantCode(t, err, codes.Unauthenticated)

	basic := metadata.AppendToOutgoingContext(ctx, "authorization", "Basic "+token)
	_, err = h.client.GetOrder(basic, &ordersv1.GetOrderRequest{Id: "x"})
	wantCode(t, err, codes.Unauthenticated)

	stream, err := h.client.WatchOrder(ctx, &ordersv1.WatchOrderRequest{Id: "x"})
	if err == nil {
		_, err = stream.Recv()
	}
	wantCode(t, err, codes.Unauthenticated)

	// Health checks are public.
	hc, err := h.health.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || hc.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health: %v %v", hc, err)
	}
}

func TestWatchOrderStreamsUntilTerminal(t *testing.T) {
	h := newHarness(t)
	created, err := h.client.CreateOrder(authed(t), createReq("key-watch-1"))
	if err != nil {
		t.Fatal(err)
	}
	id := created.GetOrder().GetId()
	stream, err := h.client.WatchOrder(authed(t), &ordersv1.WatchOrderRequest{Id: id})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil || first.GetStatus() != ordersv1.OrderStatus_ORDER_STATUS_PENDING {
		t.Fatalf("first message: %v %v", first, err)
	}
	if err := h.store.SetStatus(id, orders.StatusRejected, "out of stock: BOOK-1"); err != nil {
		t.Fatal(err)
	}
	second, err := stream.Recv()
	if err != nil || second.GetStatus() != ordersv1.OrderStatus_ORDER_STATUS_REJECTED || second.GetRejectReason() != "out of stock: BOOK-1" {
		t.Fatalf("second message: %v %v", second, err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("stream should end after a terminal status, got %v", err)
	}
}

func TestWatchOrderAlreadyTerminalSendsOneMessage(t *testing.T) {
	h := newHarness(t)
	created, _ := h.client.CreateOrder(authed(t), createReq("key-watch-2"))
	id := created.GetOrder().GetId()
	_ = h.store.SetStatus(id, orders.StatusFulfilled, "")
	stream, err := h.client.WatchOrder(authed(t), &ordersv1.WatchOrderRequest{Id: id})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.Recv()
	if err != nil || msg.GetStatus() != ordersv1.OrderStatus_ORDER_STATUS_FULFILLED {
		t.Fatalf("got %v %v", msg, err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
}

func TestWatchOrderClientCancelReleasesWatcher(t *testing.T) {
	h := newHarness(t)
	created, _ := h.client.CreateOrder(authed(t), createReq("key-watch-3"))
	id := created.GetOrder().GetId()
	ctx, cancel := context.WithCancel(authed(t))
	stream, err := h.client.WatchOrder(ctx, &ordersv1.WatchOrderRequest{Id: id})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	if h.store.WatcherCount(id) != 1 {
		t.Fatal("expected one watcher while streaming")
	}
	cancel()
	if _, err := stream.Recv(); status.Code(err) != codes.Canceled {
		t.Fatalf("want Canceled, got %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for h.store.WatcherCount(id) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("server did not release the watcher after the client cancelled")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWatchOrderNotFound(t *testing.T) {
	h := newHarness(t)
	stream, err := h.client.WatchOrder(authed(t), &ordersv1.WatchOrderRequest{Id: "ord_missing"})
	if err == nil {
		_, err = stream.Recv()
	}
	wantCode(t, err, codes.NotFound)
}
