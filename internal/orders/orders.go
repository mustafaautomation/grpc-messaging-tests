// Package orders holds the order model, request validation and an in-memory store
// with idempotency and status watchers.
package orders

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Status mirrors orders.v1.OrderStatus without depending on generated code.
type Status int

// Order statuses. Pending is the only non-terminal status.
const (
	StatusUnspecified Status = iota
	StatusPending
	StatusFulfilled
	StatusRejected
)

// Terminal reports whether no further status change is expected.
func (s Status) Terminal() bool { return s == StatusFulfilled || s == StatusRejected }

func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusFulfilled:
		return "fulfilled"
	case StatusRejected:
		return "rejected"
	default:
		return "unspecified"
	}
}

// LineItem is one SKU and quantity at a unit price in minor units (cents).
type LineItem struct {
	SKU            string `json:"sku"`
	Quantity       int32  `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

// Order is the stored order.
type Order struct {
	ID           string     `json:"id"`
	CustomerID   string     `json:"customer_id"`
	Items        []LineItem `json:"items"`
	TotalCents   int64      `json:"total_cents"`
	Status       Status     `json:"status"`
	RejectReason string     `json:"reject_reason,omitempty"`
}

// CreateRequest is the validated input for a new order.
type CreateRequest struct {
	IdempotencyKey string
	CustomerID     string
	Items          []LineItem
}

// Limits on a single order.
const (
	MaxItems      = 50
	MaxQuantity   = 1000
	MaxUnitPrice  = 10_000_000 // 100,000.00 in cents
	maxCustomerID = 64
)

var (
	keyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{8,64}$`)
	skuPattern = regexp.MustCompile(`^[A-Z0-9][A-Z0-9-]{0,31}$`)
)

// FieldError describes one invalid field; the server maps it to InvalidArgument.
type FieldError struct {
	Field  string
	Reason string
}

func (e *FieldError) Error() string { return e.Field + ": " + e.Reason }

// Validate checks a create request and returns the order total in cents.
func Validate(r CreateRequest) (int64, error) {
	if !keyPattern.MatchString(r.IdempotencyKey) {
		return 0, &FieldError{"idempotency_key", "must be 8-64 characters of A-Z, a-z, 0-9, _ or -"}
	}
	if strings.TrimSpace(r.CustomerID) == "" || len(r.CustomerID) > maxCustomerID {
		return 0, &FieldError{"customer_id", "is required and at most 64 characters"}
	}
	if len(r.Items) == 0 || len(r.Items) > MaxItems {
		return 0, &FieldError{"items", fmt.Sprintf("must contain 1-%d line items", MaxItems)}
	}
	seen := make(map[string]bool, len(r.Items))
	var total int64
	for i, it := range r.Items {
		field := "items[" + strconv.Itoa(i) + "]"
		if !skuPattern.MatchString(it.SKU) {
			return 0, &FieldError{field + ".sku", "must be upper-case letters, digits or -, at most 32 characters"}
		}
		if seen[it.SKU] {
			return 0, &FieldError{field + ".sku", "appears more than once"}
		}
		seen[it.SKU] = true
		if it.Quantity < 1 || it.Quantity > MaxQuantity {
			return 0, &FieldError{field + ".quantity", fmt.Sprintf("must be 1-%d", MaxQuantity)}
		}
		if it.UnitPriceCents < 1 || it.UnitPriceCents > MaxUnitPrice {
			return 0, &FieldError{field + ".unit_price_cents", fmt.Sprintf("must be 1-%d", MaxUnitPrice)}
		}
		// With the limits above the largest possible total is 50 * 1000 * 10,000,000
		// = 5e11 cents, far below the int64 range, so the sum cannot overflow.
		total += int64(it.Quantity) * it.UnitPriceCents
	}
	return total, nil
}

// Fingerprint identifies the payload of a request so a reused idempotency key with a
// different payload can be detected. Item order does not matter.
func Fingerprint(r CreateRequest) string {
	parts := make([]string, 0, len(r.Items))
	for _, it := range r.Items {
		parts = append(parts, fmt.Sprintf("%s:%d:%d", it.SKU, it.Quantity, it.UnitPriceCents))
	}
	slices.Sort(parts)
	sum := sha256.Sum256([]byte(r.CustomerID + "|" + strings.Join(parts, ",")))
	return hex.EncodeToString(sum[:])
}
