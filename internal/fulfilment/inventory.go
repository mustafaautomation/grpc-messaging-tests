package fulfilment

import (
	"context"
	"fmt"
	"sync"

	"github.com/mustafaautomation/grpc-messaging-tests/internal/orders"
)

// OutOfStockError means the order can never be fulfilled as requested. It is a
// business outcome (the order is rejected), not a delivery failure.
type OutOfStockError struct {
	SKU       string
	Requested int32
	Available int32
}

func (e *OutOfStockError) Error() string {
	return fmt.Sprintf("out of stock: %s requested %d, available %d", e.SKU, e.Requested, e.Available)
}

// Inventory reserves stock for an order. Errors other than *OutOfStockError are
// treated as transient and the event is redelivered.
type Inventory interface {
	Reserve(ctx context.Context, orderID string, items []orders.LineItem) error
}

// MemoryInventory is an all-or-nothing stock counter. Reserving the same order twice
// is a no-op, so redelivered events do not take stock twice.
type MemoryInventory struct {
	mu       sync.Mutex
	stock    map[string]int32
	reserved map[string]bool
}

// NewMemoryInventory returns an inventory with the given stock levels.
func NewMemoryInventory(stock map[string]int32) *MemoryInventory {
	s := make(map[string]int32, len(stock))
	for k, v := range stock {
		s[k] = v
	}
	return &MemoryInventory{stock: s, reserved: map[string]bool{}}
}

// Reserve implements Inventory.
func (m *MemoryInventory) Reserve(_ context.Context, orderID string, items []orders.LineItem) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reserved[orderID] {
		return nil
	}
	for _, it := range items {
		if m.stock[it.SKU] < it.Quantity {
			return &OutOfStockError{SKU: it.SKU, Requested: it.Quantity, Available: m.stock[it.SKU]}
		}
	}
	for _, it := range items {
		m.stock[it.SKU] -= it.Quantity
	}
	m.reserved[orderID] = true
	return nil
}

// Available returns the current stock for a SKU.
func (m *MemoryInventory) Available(sku string) int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stock[sku]
}
