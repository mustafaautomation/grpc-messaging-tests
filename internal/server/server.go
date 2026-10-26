// Package server implements orders.v1.OrderService.
package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ordersv1 "github.com/mustafaautomation/grpc-messaging-tests/gen/orders/v1"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/events"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/orders"
)

// Service is the gRPC OrderService.
type Service struct {
	ordersv1.UnimplementedOrderServiceServer
	Store     *orders.Store
	Publisher events.Publisher
}

// CreateOrder validates, stores and publishes. If publishing fails the order is
// removed again and Unavailable is returned, so a retry with the same key is safe.
func (s *Service) CreateOrder(ctx context.Context, req *ordersv1.CreateOrderRequest) (*ordersv1.CreateOrderResponse, error) {
	cr := orders.CreateRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		CustomerID:     req.GetCustomerId(),
		Items:          fromProtoItems(req.GetItems()),
	}
	total, err := orders.Validate(cr)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	o, replayed, err := s.Store.Create(cr, total)
	if errors.Is(err, orders.ErrIdempotencyMismatch) {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "could not store order")
	}
	if !replayed {
		pubErr := s.Publisher.PublishOrderCreated(ctx, events.OrderCreated{
			OrderID: o.ID, CustomerID: o.CustomerID, Items: o.Items, TotalCents: o.TotalCents,
		})
		if pubErr != nil {
			s.Store.Forget(o.ID, cr.IdempotencyKey)
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, status.FromContextError(ctxErr).Err()
			}
			return nil, status.Error(codes.Unavailable, "order not accepted: event could not be published")
		}
	}
	return &ordersv1.CreateOrderResponse{Order: toProto(o), Replayed: replayed}, nil
}

// GetOrder returns one order.
func (s *Service) GetOrder(_ context.Context, req *ordersv1.GetOrderRequest) (*ordersv1.GetOrderResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	o, err := s.Store.Get(req.GetId())
	if errors.Is(err, orders.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "order %q not found", req.GetId())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "could not read order")
	}
	return &ordersv1.GetOrderResponse{Order: toProto(o)}, nil
}

// WatchOrder streams the current status and every change until a terminal status,
// or until the client cancels.
func (s *Service) WatchOrder(req *ordersv1.WatchOrderRequest, stream ordersv1.OrderService_WatchOrderServer) error {
	if req.GetId() == "" {
		return status.Error(codes.InvalidArgument, "id is required")
	}
	current, changes, cancel, err := s.Store.Watch(req.GetId())
	if errors.Is(err, orders.ErrNotFound) {
		return status.Errorf(codes.NotFound, "order %q not found", req.GetId())
	}
	if err != nil {
		return status.Error(codes.Internal, "could not watch order")
	}
	defer cancel()

	if err := stream.Send(toWatch(current)); err != nil {
		return err
	}
	if current.Status.Terminal() {
		return nil
	}
	for {
		select {
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		case o := <-changes:
			if err := stream.Send(toWatch(o)); err != nil {
				return err
			}
			if o.Status.Terminal() {
				return nil
			}
		}
	}
}

func fromProtoItems(items []*ordersv1.LineItem) []orders.LineItem {
	out := make([]orders.LineItem, 0, len(items))
	for _, it := range items {
		out = append(out, orders.LineItem{SKU: it.GetSku(), Quantity: it.GetQuantity(), UnitPriceCents: it.GetUnitPriceCents()})
	}
	return out
}

func toProto(o orders.Order) *ordersv1.Order {
	items := make([]*ordersv1.LineItem, 0, len(o.Items))
	for _, it := range o.Items {
		items = append(items, &ordersv1.LineItem{Sku: it.SKU, Quantity: it.Quantity, UnitPriceCents: it.UnitPriceCents})
	}
	return &ordersv1.Order{
		Id: o.ID, CustomerId: o.CustomerID, Items: items, TotalCents: o.TotalCents,
		Status: toProtoStatus(o.Status), RejectReason: o.RejectReason,
	}
}

func toWatch(o orders.Order) *ordersv1.WatchOrderResponse {
	return &ordersv1.WatchOrderResponse{OrderId: o.ID, Status: toProtoStatus(o.Status), RejectReason: o.RejectReason}
}

func toProtoStatus(s orders.Status) ordersv1.OrderStatus {
	switch s {
	case orders.StatusPending:
		return ordersv1.OrderStatus_ORDER_STATUS_PENDING
	case orders.StatusFulfilled:
		return ordersv1.OrderStatus_ORDER_STATUS_FULFILLED
	case orders.StatusRejected:
		return ordersv1.OrderStatus_ORDER_STATUS_REJECTED
	default:
		return ordersv1.OrderStatus_ORDER_STATUS_UNSPECIFIED
	}
}
