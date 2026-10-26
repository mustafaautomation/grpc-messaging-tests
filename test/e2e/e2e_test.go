// Package e2e runs the gRPC service, JetStream and the fulfilment worker together.
package e2e

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	ordersv1 "github.com/mustafaautomation/grpc-messaging-tests/gen/orders/v1"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/events"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/fulfilment"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/natsutil"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/orders"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/server"
)

type system struct {
	client ordersv1.OrderServiceClient
	stream jetstream.Stream
}

func start(t *testing.T, stock map[string]int32) *system {
	t.Helper()
	ns, err := natsutil.StartEmbedded(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := events.EnsureStream(context.Background(), js, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	store := orders.NewStore()
	cfg := fulfilment.Config{Durable: "fulfilment-e2e", MaxDeliver: 3, AckWait: 2 * time.Second, RetryDelay: 20 * time.Millisecond}
	w := fulfilment.NewWorker(js, cfg, store, fulfilment.NewMemoryInventory(stock))
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)

	lis := bufconn.Listen(1 << 20)
	auth := server.BearerAuth{Token: "e2e"}
	gs := grpc.NewServer(grpc.UnaryInterceptor(auth.Unary()), grpc.StreamInterceptor(auth.Stream()))
	ordersv1.RegisterOrderServiceServer(gs, &server.Service{Store: store, Publisher: events.JetStreamPublisher{JS: js}})
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &system{client: ordersv1.NewOrderServiceClient(conn), stream: stream}
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return metadata.AppendToOutgoingContext(c, "authorization", "Bearer e2e")
}

func order(key string, qty int32) *ordersv1.CreateOrderRequest {
	return &ordersv1.CreateOrderRequest{IdempotencyKey: key, CustomerId: "cust-e2e",
		Items: []*ordersv1.LineItem{{Sku: "BOOK-1", Quantity: qty, UnitPriceCents: 1250}}}
}

// statuses collects every status the watch stream reports until it ends.
func statuses(t *testing.T, s ordersv1.OrderService_WatchOrderClient) []ordersv1.OrderStatus {
	t.Helper()
	var out []ordersv1.OrderStatus
	for {
		msg, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		out = append(out, msg.GetStatus())
	}
}

func TestOrderIsFulfilledEndToEnd(t *testing.T) {
	sys := start(t, map[string]int32{"BOOK-1": 10})
	created, err := sys.client.CreateOrder(ctx(t), order("e2e-fulfil-1", 2))
	if err != nil {
		t.Fatal(err)
	}
	watch, err := sys.client.WatchOrder(ctx(t), &ordersv1.WatchOrderRequest{Id: created.GetOrder().GetId()})
	if err != nil {
		t.Fatal(err)
	}
	got := statuses(t, watch)
	last := got[len(got)-1]
	if last != ordersv1.OrderStatus_ORDER_STATUS_FULFILLED {
		t.Fatalf("statuses = %v, want to end with FULFILLED", got)
	}
	// The worker may finish before the watch starts, so PENDING is optional, but
	// nothing other than PENDING may come before the terminal status.
	for _, s := range got[:len(got)-1] {
		if s != ordersv1.OrderStatus_ORDER_STATUS_PENDING {
			t.Fatalf("unexpected intermediate status in %v", got)
		}
	}
}

func TestOrderIsRejectedEndToEndWhenStockIsShort(t *testing.T) {
	sys := start(t, map[string]int32{"BOOK-1": 1})
	created, err := sys.client.CreateOrder(ctx(t), order("e2e-reject-1", 5))
	if err != nil {
		t.Fatal(err)
	}
	watch, err := sys.client.WatchOrder(ctx(t), &ordersv1.WatchOrderRequest{Id: created.GetOrder().GetId()})
	if err != nil {
		t.Fatal(err)
	}
	got := statuses(t, watch)
	if got[len(got)-1] != ordersv1.OrderStatus_ORDER_STATUS_REJECTED {
		t.Fatalf("statuses = %v, want to end with REJECTED", got)
	}
	final, err := sys.client.GetOrder(ctx(t), &ordersv1.GetOrderRequest{Id: created.GetOrder().GetId()})
	if err != nil || final.GetOrder().GetRejectReason() == "" {
		t.Fatalf("rejected order should carry a reason: %v %v", final, err)
	}
}

func TestClientRetriesStoreOneEvent(t *testing.T) {
	sys := start(t, map[string]int32{"BOOK-1": 10})
	for range 5 {
		if _, err := sys.client.CreateOrder(ctx(t), order("e2e-retry-1", 1)); err != nil {
			t.Fatal(err)
		}
	}
	info, err := sys.stream.Info(context.Background(), jetstream.WithSubjectFilter(events.SubjectCreated))
	if err != nil {
		t.Fatal(err)
	}
	if n := info.State.Subjects[events.SubjectCreated]; n != 1 {
		t.Fatalf("%s has %d messages, want 1", events.SubjectCreated, n)
	}
}
