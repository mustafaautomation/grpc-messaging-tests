package fulfilment

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mustafaautomation/grpc-messaging-tests/internal/events"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/natsutil"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/orders"
)

// env is one embedded JetStream server with the ORDERS stream.
type env struct {
	js     jetstream.JetStream
	stream jetstream.Stream
	store  *orders.Store
}

func newEnv(t *testing.T) *env {
	t.Helper()
	srv, err := natsutil.StartEmbedded(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
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
	return &env{js: js, stream: stream, store: orders.NewStore()}
}

func testConfig() Config {
	return Config{Durable: "fulfilment-test", MaxDeliver: 3, AckWait: 2 * time.Second, RetryDelay: 20 * time.Millisecond}
}

func (e *env) order(t *testing.T, key string, items ...orders.LineItem) orders.Order {
	t.Helper()
	r := orders.CreateRequest{IdempotencyKey: key, CustomerID: "cust-1", Items: items}
	total, err := orders.Validate(r)
	if err != nil {
		t.Fatal(err)
	}
	o, _, err := e.store.Create(r, total)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func (e *env) publish(t *testing.T, o orders.Order) {
	t.Helper()
	err := events.JetStreamPublisher{JS: e.js}.PublishOrderCreated(context.Background(),
		events.OrderCreated{OrderID: o.ID, CustomerID: o.CustomerID, Items: o.Items, TotalCents: o.TotalCents})
	if err != nil {
		t.Fatal(err)
	}
}

func (e *env) start(t *testing.T, inv Inventory) {
	t.Helper()
	w := NewWorker(e.js, testConfig(), e.store, inv)
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
}

func waitStatus(t *testing.T, s *orders.Store, id string, want orders.Status) orders.Order {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		o, err := s.Get(id)
		if err == nil && o.Status == want {
			return o
		}
		if time.Now().After(deadline) {
			t.Fatalf("order %s: status %v, want %v", id, o.Status, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// dlqMessages reads everything currently on the dead-letter subject.
func (e *env) dlqMessages(t *testing.T, want int) []jetstream.Msg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cons, err := e.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{FilterSubject: events.SubjectDLQ, AckPolicy: jetstream.AckNonePolicy})
	if err != nil {
		t.Fatal(err)
	}
	var out []jetstream.Msg
	for len(out) < want {
		batch, err := cons.Fetch(want-len(out), jetstream.FetchMaxWait(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		for m := range batch.Messages() {
			out = append(out, m)
		}
		if ctx.Err() != nil {
			break
		}
	}
	return out
}

var book = orders.LineItem{SKU: "BOOK-1", Quantity: 2, UnitPriceCents: 1250}

func TestPublishIsDeduplicatedByOrderID(t *testing.T) {
	e := newEnv(t)
	o := e.order(t, "key-dedup-1", book)
	e.publish(t, o)
	e.publish(t, o) // e.g. a retry after a lost publish ack
	info, err := e.stream.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream has %d messages, want 1", info.State.Msgs)
	}
}

func TestWorkerFulfilsAndTakesStockOnce(t *testing.T) {
	e := newEnv(t)
	inv := NewMemoryInventory(map[string]int32{"BOOK-1": 5})
	e.start(t, inv)
	o := e.order(t, "key-fulfil-1", book)
	e.publish(t, o)
	waitStatus(t, e.store, o.ID, orders.StatusFulfilled)
	if got := inv.Available("BOOK-1"); got != 3 {
		t.Fatalf("stock = %d, want 3", got)
	}
}

func TestWorkerRejectsWhenOutOfStock(t *testing.T) {
	e := newEnv(t)
	inv := NewMemoryInventory(map[string]int32{"BOOK-1": 1})
	e.start(t, inv)
	o := e.order(t, "key-oos-1", book)
	e.publish(t, o)
	got := waitStatus(t, e.store, o.ID, orders.StatusRejected)
	if got.RejectReason != "out of stock: BOOK-1 requested 2, available 1" {
		t.Fatalf("reason = %q", got.RejectReason)
	}
	if inv.Available("BOOK-1") != 1 {
		t.Fatal("a rejected order must not take stock")
	}
}

// flakyInventory fails the first n reservations with a transient error.
type flakyInventory struct {
	Inventory
	failures atomic.Int32
	calls    atomic.Int32
}

func (f *flakyInventory) Reserve(ctx context.Context, id string, items []orders.LineItem) error {
	f.calls.Add(1)
	if f.failures.Add(-1) >= 0 {
		return errors.New("inventory service timeout")
	}
	return f.Inventory.Reserve(ctx, id, items)
}

func TestWorkerRetriesTransientFailures(t *testing.T) {
	e := newEnv(t)
	inv := &flakyInventory{Inventory: NewMemoryInventory(map[string]int32{"BOOK-1": 5})}
	inv.failures.Store(2) // fails on deliveries 1 and 2, succeeds on 3 (MaxDeliver = 3)
	e.start(t, inv)
	o := e.order(t, "key-retry-1", book)
	e.publish(t, o)
	waitStatus(t, e.store, o.ID, orders.StatusFulfilled)
	if inv.calls.Load() != 3 {
		t.Fatalf("reserve called %d times, want 3", inv.calls.Load())
	}
}

func TestWorkerDeadLettersAfterMaxDeliver(t *testing.T) {
	e := newEnv(t)
	inv := &flakyInventory{Inventory: NewMemoryInventory(nil)}
	inv.failures.Store(1000)
	e.start(t, inv)
	o := e.order(t, "key-dlq-1", book)
	e.publish(t, o)

	msgs := e.dlqMessages(t, 1)
	if len(msgs) != 1 {
		t.Fatalf("dead-letter messages = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Headers().Get(events.HeaderDeliveries) != "3" || m.Headers().Get(events.HeaderError) != "inventory service timeout" {
		t.Fatalf("headers = %v", m.Headers())
	}
	var ev events.OrderCreated
	if err := json.Unmarshal(m.Data(), &ev); err != nil || ev.OrderID != o.ID {
		t.Fatalf("payload not preserved: %s", m.Data())
	}
	if inv.calls.Load() != 3 {
		t.Fatalf("reserve called %d times, want exactly MaxDeliver (3)", inv.calls.Load())
	}
	cur, _ := e.store.Get(o.ID)
	if cur.Status != orders.StatusPending {
		t.Fatalf("a dead-lettered order should stay pending for manual handling, got %v", cur.Status)
	}
}

func TestWorkerDeadLettersMalformedEventImmediately(t *testing.T) {
	e := newEnv(t)
	inv := &flakyInventory{Inventory: NewMemoryInventory(nil)}
	e.start(t, inv)
	if _, err := e.js.Publish(context.Background(), events.SubjectCreated, []byte("{not json")); err != nil {
		t.Fatal(err)
	}
	msgs := e.dlqMessages(t, 1)
	if len(msgs) != 1 || msgs[0].Headers().Get(events.HeaderDeliveries) != "1" || string(msgs[0].Data()) != "{not json" {
		t.Fatalf("unexpected dead letter: %d messages", len(msgs))
	}
	if inv.calls.Load() != 0 {
		t.Fatal("malformed event must not reach inventory")
	}
}

func TestWorkerProcessesABacklogAfterRestart(t *testing.T) {
	e := newEnv(t)
	inv := NewMemoryInventory(map[string]int32{"BOOK-1": 100})
	var ids []string
	for i := range 10 {
		o := e.order(t, "key-backlog-"+string(rune('a'+i))+"xxxx", book)
		e.publish(t, o) // published while no worker is running
		ids = append(ids, o.ID)
	}
	e.start(t, inv)
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Go(func() { waitStatus(t, e.store, id, orders.StatusFulfilled) })
	}
	wg.Wait()
	if inv.Available("BOOK-1") != 100-10*2 {
		t.Fatalf("stock = %d, want 80", inv.Available("BOOK-1"))
	}
}
