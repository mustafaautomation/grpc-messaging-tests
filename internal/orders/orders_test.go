package orders

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

func validRequest() CreateRequest {
	return CreateRequest{
		IdempotencyKey: "key-0001",
		CustomerID:     "cust-1",
		Items:          []LineItem{{SKU: "BOOK-1", Quantity: 2, UnitPriceCents: 1250}, {SKU: "PEN-2", Quantity: 1, UnitPriceCents: 199}},
	}
}

func TestValidateTotal(t *testing.T) {
	total, err := Validate(validRequest())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 2*1250+199 {
		t.Fatalf("total = %d, want %d", total, 2*1250+199)
	}
}

func TestValidateRejects(t *testing.T) {
	many := make([]LineItem, MaxItems+1)
	for i := range many {
		many[i] = LineItem{SKU: "SKU-" + strings.Repeat("A", i%5+1) + string(rune('A'+i%26)), Quantity: 1, UnitPriceCents: 1}
	}
	cases := []struct {
		name  string
		edit  func(*CreateRequest)
		field string
	}{
		{"short key", func(r *CreateRequest) { r.IdempotencyKey = "abc" }, "idempotency_key"},
		{"key with space", func(r *CreateRequest) { r.IdempotencyKey = "key 00001" }, "idempotency_key"},
		{"key too long", func(r *CreateRequest) { r.IdempotencyKey = strings.Repeat("k", 65) }, "idempotency_key"},
		{"blank customer", func(r *CreateRequest) { r.CustomerID = "   " }, "customer_id"},
		{"no items", func(r *CreateRequest) { r.Items = nil }, "items"},
		{"too many items", func(r *CreateRequest) { r.Items = many }, "items"},
		{"lower-case sku", func(r *CreateRequest) { r.Items[0].SKU = "book-1" }, "items[0].sku"},
		{"duplicate sku", func(r *CreateRequest) { r.Items[1].SKU = "BOOK-1" }, "items[1].sku"},
		{"zero quantity", func(r *CreateRequest) { r.Items[0].Quantity = 0 }, "items[0].quantity"},
		{"negative quantity", func(r *CreateRequest) { r.Items[1].Quantity = -3 }, "items[1].quantity"},
		{"quantity over limit", func(r *CreateRequest) { r.Items[0].Quantity = MaxQuantity + 1 }, "items[0].quantity"},
		{"zero price", func(r *CreateRequest) { r.Items[0].UnitPriceCents = 0 }, "items[0].unit_price_cents"},
		{"price over limit", func(r *CreateRequest) { r.Items[1].UnitPriceCents = MaxUnitPrice + 1 }, "items[1].unit_price_cents"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := validRequest()
			tc.edit(&r)
			_, err := Validate(r)
			var fe *FieldError
			if !errors.As(err, &fe) {
				t.Fatalf("want *FieldError, got %v", err)
			}
			if fe.Field != tc.field {
				t.Fatalf("field = %q, want %q (%v)", fe.Field, tc.field, err)
			}
		})
	}
}

func TestFingerprintIgnoresItemOrderButNotContent(t *testing.T) {
	a := validRequest()
	b := validRequest()
	b.Items[0], b.Items[1] = b.Items[1], b.Items[0]
	if Fingerprint(a) != Fingerprint(b) {
		t.Fatal("same items in a different order should match")
	}
	b.Items[0].Quantity++
	if Fingerprint(a) == Fingerprint(b) {
		t.Fatal("different quantity should not match")
	}
	c := validRequest()
	c.CustomerID = "cust-2"
	if Fingerprint(a) == Fingerprint(c) {
		t.Fatal("different customer should not match")
	}
}

func TestStoreIdempotency(t *testing.T) {
	s := NewStore()
	r := validRequest()
	first, replayed, err := s.Create(r, 100)
	if err != nil || replayed {
		t.Fatalf("first create: replayed=%v err=%v", replayed, err)
	}
	again, replayed, err := s.Create(r, 100)
	if err != nil || !replayed || again.ID != first.ID {
		t.Fatalf("retry: id=%s replayed=%v err=%v", again.ID, replayed, err)
	}
	r.Items[0].Quantity = 9
	if _, _, err := s.Create(r, 100); !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("changed payload: want ErrIdempotencyMismatch, got %v", err)
	}
}

func TestStoreConcurrentCreatesWithOneKeyMakeOneOrder(t *testing.T) {
	s := NewStore()
	const n = 50
	ids := make(chan string, n)
	var fresh sync.WaitGroup
	var created int
	var mu sync.Mutex
	for range n {
		fresh.Go(func() {
			o, replayed, err := s.Create(validRequest(), 100)
			if err != nil {
				t.Error(err)
				return
			}
			if !replayed {
				mu.Lock()
				created++
				mu.Unlock()
			}
			ids <- o.ID
		})
	}
	fresh.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("got two order ids: %s and %s", first, id)
		}
	}
	if created != 1 {
		t.Fatalf("created = %d, want 1", created)
	}
}

func TestStoreReturnsCopies(t *testing.T) {
	s := NewStore()
	o, _, _ := s.Create(validRequest(), 100)
	o.Items[0].Quantity = 999
	got, _ := s.Get(o.ID)
	if got.Items[0].Quantity == 999 {
		t.Fatal("caller mutated stored order")
	}
}

func TestStatusTransitions(t *testing.T) {
	s := NewStore()
	o, _, _ := s.Create(validRequest(), 100)
	if err := s.SetStatus(o.ID, StatusPending, ""); err != nil {
		t.Fatalf("pending -> pending should be a no-op, got %v", err)
	}
	if err := s.SetStatus(o.ID, StatusFulfilled, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStatus(o.ID, StatusFulfilled, ""); err != nil {
		t.Fatalf("repeating a terminal status should be a no-op, got %v", err)
	}
	if err := s.SetStatus(o.ID, StatusRejected, "late"); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("fulfilled -> rejected: want ErrIllegalTransition, got %v", err)
	}
	if err := s.SetStatus("ord_missing", StatusFulfilled, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing order: want ErrNotFound, got %v", err)
	}
}

func TestWatchDeliversChangesAndCancelUnregisters(t *testing.T) {
	s := NewStore()
	o, _, _ := s.Create(validRequest(), 100)
	cur, ch, cancel, err := s.Watch(o.ID)
	if err != nil || cur.Status != StatusPending {
		t.Fatalf("watch: %v %v", cur.Status, err)
	}
	if s.WatcherCount(o.ID) != 1 {
		t.Fatal("watcher not registered")
	}
	if err := s.SetStatus(o.ID, StatusRejected, "out of stock"); err != nil {
		t.Fatal(err)
	}
	got := <-ch
	if got.Status != StatusRejected || got.RejectReason != "out of stock" {
		t.Fatalf("got %+v", got)
	}
	cancel()
	cancel() // safe to call twice
	if s.WatcherCount(o.ID) != 0 {
		t.Fatal("watcher not removed")
	}
}

// FuzzValidate checks that validation never panics and that an accepted request's
// total equals the sum of its lines and stays within the documented maximum.
func FuzzValidate(f *testing.F) {
	f.Add("key-0001", "cust", "BOOK-1", int32(2), int64(1250))
	f.Add("", "", "", int32(0), int64(0))
	f.Add("key-0001", "c", "A", int32(-1), int64(-5))
	f.Fuzz(func(t *testing.T, key, customer, sku string, qty int32, price int64) {
		r := CreateRequest{IdempotencyKey: key, CustomerID: customer, Items: []LineItem{{SKU: sku, Quantity: qty, UnitPriceCents: price}}}
		total, err := Validate(r)
		if err != nil {
			return
		}
		if total != int64(qty)*price {
			t.Fatalf("total %d != %d * %d", total, qty, price)
		}
		if total <= 0 || total > MaxQuantity*MaxUnitPrice {
			t.Fatalf("accepted total %d out of range", total)
		}
	})
}
