// Package fulfilment consumes OrderCreated events and fulfils or rejects orders.
package fulfilment

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mustafaautomation/grpc-messaging-tests/internal/events"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/orders"
)

// StatusSetter is the part of the order store the worker needs.
type StatusSetter interface {
	SetStatus(id string, status orders.Status, reason string) error
}

// Config controls delivery behaviour.
type Config struct {
	Durable    string
	MaxDeliver int           // attempts before the event goes to the dead-letter subject
	AckWait    time.Duration // redelivery if neither acked nor nacked within this time
	RetryDelay time.Duration // delay before redelivery after a transient failure
}

// DefaultConfig is used by the demo binary.
func DefaultConfig() Config {
	return Config{Durable: "fulfilment", MaxDeliver: 5, AckWait: 30 * time.Second, RetryDelay: time.Second}
}

// Worker handles one event at a time from a durable pull consumer.
type Worker struct {
	js        jetstream.JetStream
	cfg       Config
	store     StatusSetter
	inventory Inventory
	consume   jetstream.ConsumeContext
}

// NewWorker returns a worker; call Start to begin consuming.
func NewWorker(js jetstream.JetStream, cfg Config, store StatusSetter, inv Inventory) *Worker {
	return &Worker{js: js, cfg: cfg, store: store, inventory: inv}
}

// Start creates (or updates) the durable consumer and begins consuming.
func (w *Worker) Start(ctx context.Context) error {
	cons, err := w.js.CreateOrUpdateConsumer(ctx, events.StreamName, jetstream.ConsumerConfig{
		Durable:       w.cfg.Durable,
		FilterSubject: events.SubjectCreated,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    w.cfg.MaxDeliver,
		AckWait:       w.cfg.AckWait,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return err
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) { w.handle(ctx, msg) })
	if err != nil {
		return err
	}
	w.consume = cc
	return nil
}

// Stop stops consuming. In-flight messages that are not acked will be redelivered.
func (w *Worker) Stop() {
	if w.consume != nil {
		w.consume.Stop()
	}
}

func (w *Worker) handle(ctx context.Context, msg jetstream.Msg) {
	meta, err := msg.Metadata()
	if err != nil {
		_ = msg.Term()
		return
	}
	var ev events.OrderCreated
	if err := json.Unmarshal(msg.Data(), &ev); err != nil || ev.OrderID == "" {
		// A malformed event will never succeed: dead-letter it straight away.
		w.deadLetter(ctx, msg, meta.NumDelivered, errors.New("malformed event"))
		return
	}

	err = w.inventory.Reserve(ctx, ev.OrderID, ev.Items)
	var oos *OutOfStockError
	switch {
	case err == nil:
		w.finish(msg, ev.OrderID, orders.StatusFulfilled, "")
	case errors.As(err, &oos):
		w.finish(msg, ev.OrderID, orders.StatusRejected, oos.Error())
	case meta.NumDelivered >= uint64(max(w.cfg.MaxDeliver, 1)):
		w.deadLetter(ctx, msg, meta.NumDelivered, err)
	default:
		_ = msg.NakWithDelay(w.cfg.RetryDelay)
	}
}

func (w *Worker) finish(msg jetstream.Msg, orderID string, status orders.Status, reason string) {
	if err := w.store.SetStatus(orderID, status, reason); err != nil && !errors.Is(err, orders.ErrNotFound) {
		_ = msg.NakWithDelay(w.cfg.RetryDelay)
		return
	}
	_ = msg.Ack()
}

func (w *Worker) deadLetter(ctx context.Context, msg jetstream.Msg, deliveries uint64, cause error) {
	dlq := &nats.Msg{Subject: events.SubjectDLQ, Data: msg.Data(), Header: nats.Header{}}
	dlq.Header.Set(events.HeaderError, cause.Error())
	dlq.Header.Set(events.HeaderDeliveries, strconv.FormatUint(deliveries, 10))
	if _, err := w.js.PublishMsg(ctx, dlq); err != nil {
		// Could not park it: leave it for redelivery rather than lose it.
		_ = msg.NakWithDelay(w.cfg.RetryDelay)
		return
	}
	_ = msg.Term()
}
