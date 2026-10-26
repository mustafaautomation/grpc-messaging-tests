// Package events publishes order events to NATS JetStream.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mustafaautomation/grpc-messaging-tests/internal/orders"
)

// Subjects and stream names.
const (
	StreamName       = "ORDERS"
	SubjectCreated   = "orders.created"
	SubjectDLQ       = "orders.dlq"
	HeaderError      = "Orders-Last-Error"
	HeaderDeliveries = "Orders-Deliveries"
)

// OrderCreated is the payload of SubjectCreated.
type OrderCreated struct {
	OrderID    string            `json:"order_id"`
	CustomerID string            `json:"customer_id"`
	Items      []orders.LineItem `json:"items"`
	TotalCents int64             `json:"total_cents"`
}

// Publisher sends order events.
type Publisher interface {
	PublishOrderCreated(ctx context.Context, e OrderCreated) error
}

// StreamConfig returns the ORDERS stream definition. The duplicate window makes
// JetStream drop a second publish with the same Nats-Msg-Id inside that window.
func StreamConfig(duplicateWindow time.Duration) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:       StreamName,
		Subjects:   []string{"orders.>"},
		Storage:    jetstream.FileStorage,
		Duplicates: duplicateWindow,
	}
}

// EnsureStream creates or updates the ORDERS stream.
func EnsureStream(ctx context.Context, js jetstream.JetStream, duplicateWindow time.Duration) (jetstream.Stream, error) {
	return js.CreateOrUpdateStream(ctx, StreamConfig(duplicateWindow))
}

// JetStreamPublisher publishes with the order ID as the message ID, so a retried
// publish of the same order is stored once.
type JetStreamPublisher struct {
	JS jetstream.JetStream
}

// PublishOrderCreated implements Publisher.
func (p JetStreamPublisher) PublishOrderCreated(ctx context.Context, e OrderCreated) error {
	if e.OrderID == "" {
		return errors.New("order id is required")
	}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	msg := &nats.Msg{Subject: SubjectCreated, Data: data, Header: nats.Header{}}
	if _, err := p.JS.PublishMsg(ctx, msg, jetstream.WithMsgID(e.OrderID)); err != nil {
		return fmt.Errorf("publish %s: %w", SubjectCreated, err)
	}
	return nil
}
