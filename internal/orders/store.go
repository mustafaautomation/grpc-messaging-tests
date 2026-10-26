package orders

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"slices"
	"sync"
)

// Errors returned by the store.
var (
	ErrNotFound            = errors.New("order not found")
	ErrIdempotencyMismatch = errors.New("idempotency key was already used with a different request")
	ErrIllegalTransition   = errors.New("illegal status transition")
)

type idem struct {
	orderID     string
	fingerprint string
}

// Store keeps orders in memory. It is safe for concurrent use.
type Store struct {
	mu       sync.Mutex
	orders   map[string]*Order
	keys     map[string]idem
	watchers map[string][]chan Order
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{
		orders:   map[string]*Order{},
		keys:     map[string]idem{},
		watchers: map[string][]chan Order{},
	}
}

// Create stores a new pending order, or returns the existing one if the idempotency
// key was seen before with the same payload (replayed = true). The caller must have
// validated the request.
func (s *Store) Create(r CreateRequest, total int64) (o Order, replayed bool, err error) {
	fp := Fingerprint(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.keys[r.IdempotencyKey]; ok {
		if prev.fingerprint != fp {
			return Order{}, false, ErrIdempotencyMismatch
		}
		return clone(s.orders[prev.orderID]), true, nil
	}
	order := &Order{
		ID:         newID(),
		CustomerID: r.CustomerID,
		Items:      slices.Clone(r.Items),
		TotalCents: total,
		Status:     StatusPending,
	}
	s.orders[order.ID] = order
	s.keys[r.IdempotencyKey] = idem{orderID: order.ID, fingerprint: fp}
	return clone(order), false, nil
}

// Forget removes an order created by Create, used when publishing its event failed so
// the client can retry with the same key.
func (s *Store) Forget(id, idempotencyKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.orders, id)
	if k, ok := s.keys[idempotencyKey]; ok && k.orderID == id {
		delete(s.keys, idempotencyKey)
	}
}

// Get returns a copy of the order.
func (s *Store) Get(id string) (Order, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.orders[id]
	if !ok {
		return Order{}, ErrNotFound
	}
	return clone(o), nil
}

// SetStatus moves a pending order to a terminal status and notifies watchers.
// Repeating the same terminal status is a no-op, so redelivered events are harmless.
func (s *Store) SetStatus(id string, status Status, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.orders[id]
	if !ok {
		return ErrNotFound
	}
	if o.Status == status {
		return nil
	}
	if o.Status != StatusPending || !status.Terminal() {
		return ErrIllegalTransition
	}
	o.Status = status
	o.RejectReason = reason
	snapshot := clone(o)
	for _, ch := range s.watchers[id] {
		select {
		case ch <- snapshot:
		default: // watcher buffer full; it will read the latest state from Get
		}
	}
	return nil
}

// Watch returns the current order and a channel of later changes. Call cancel to stop.
func (s *Store) Watch(id string) (current Order, changes <-chan Order, cancel func(), err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.orders[id]
	if !ok {
		return Order{}, nil, nil, ErrNotFound
	}
	ch := make(chan Order, 4)
	s.watchers[id] = append(s.watchers[id], ch)
	var once sync.Once
	cancel = func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.watchers[id] = slices.DeleteFunc(s.watchers[id], func(c chan Order) bool { return c == ch })
			if len(s.watchers[id]) == 0 {
				delete(s.watchers, id)
			}
		})
	}
	return clone(o), ch, cancel, nil
}

// WatcherCount reports how many watchers are registered for an order (for tests and metrics).
func (s *Store) WatcherCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.watchers[id])
}

func clone(o *Order) Order {
	c := *o
	c.Items = slices.Clone(o.Items)
	return c
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "ord_" + hex.EncodeToString(b)
}
