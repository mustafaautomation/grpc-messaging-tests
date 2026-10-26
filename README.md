# grpc-messaging-tests

[![CI](https://github.com/mustafaautomation/grpc-messaging-tests/actions/workflows/ci.yml/badge.svg)](https://github.com/mustafaautomation/grpc-messaging-tests/actions/workflows/ci.yml)

A small Go order service with a gRPC API and NATS JetStream messaging, built to practise testing
the things that usually break in event-driven systems: idempotent retries, duplicate events,
redelivery, dead-lettering, streaming RPCs, deadlines and cancellation.

This is a personal portfolio project. It is not based on any employer's or client's system.

## What the system does

```
client ──gRPC──▶ OrderService ──publish──▶ JetStream stream ORDERS ──▶ fulfilment worker
                    │  ▲                     (orders.created)              │
                    │  └──── WatchOrder ◀──── status change ◀──────────────┘
                    ▼                                                       │
               order store                         orders.dlq ◀── after MaxDeliver attempts
```

- `CreateOrder` validates the request, stores a pending order and publishes `orders.created`.
  It is idempotent per `idempotency_key`; reusing a key with a different payload fails.
- The event uses the order ID as its JetStream message ID, so a re-published event is stored once.
- The fulfilment worker reserves stock. Out of stock rejects the order (a business outcome).
  Any other error is treated as transient: the event is redelivered, and after `MaxDeliver`
  attempts it is copied to `orders.dlq` with the last error and delivery count in headers.
  Malformed events go to the dead-letter subject straight away.
- `WatchOrder` is a server-streaming RPC that sends the current status, then every change, and
  ends at a terminal status.
- A bearer-token interceptor protects the API; the standard gRPC health service is public.

The order store and inventory are in memory. That keeps the project about the gRPC and messaging
behaviour; a database would not change what is being tested here.

## Tests

31 test functions plus one fuzz target (and table-driven subtests), all run with the race detector:

| Package | Tests | What they cover |
|---|---:|---|
| `internal/orders` | 8 + fuzz | Validation table (13 cases), fingerprinting, idempotent create, 50 concurrent creates with one key, status transitions, watchers. `FuzzValidate` checks validation never panics and accepted totals are consistent. |
| `internal/server` | 13 | gRPC over an in-memory `bufconn`: status codes (`InvalidArgument`, `NotFound`, `FailedPrecondition`, `Unauthenticated`, `Unavailable`, `DeadlineExceeded`, `Canceled`), replayed retries, 20 concurrent retries giving one order and one event, publish failure leaving the key reusable, streaming until a terminal status, and the server releasing a watcher when the client cancels. |
| `internal/fulfilment` | 7 | Against an in-process NATS server with JetStream: publish de-duplication, fulfil and stock taken once, out-of-stock rejection, two transient failures then success, dead-lettering after exactly `MaxDeliver` attempts, malformed events, and a backlog processed after the worker starts. |
| `test/e2e` | 3 | gRPC, JetStream and the worker together: fulfilled and rejected flows observed through `WatchOrder`, and five client retries storing one event. |

Statement coverage across `internal/...` is about 89% (CI prints the exact figure).

No Docker is needed: the NATS server runs in-process (`internal/natsutil`), which makes the
messaging tests fast and deterministic. The trade-off is that they do not exercise a networked,
clustered NATS deployment.

## Run it

Requires Go 1.27+.

```bash
go test -race ./...                       # all tests
go test -run='^$' -fuzz=FuzzValidate -fuzztime=30s ./internal/orders

# demo server with an embedded NATS server
ORDERS_TOKEN=dev go run ./cmd/ordersd
grpcurl -plaintext -H 'authorization: Bearer dev' \
  -d '{"idempotency_key":"demo-0001","customer_id":"c1","items":[{"sku":"BOOK-1","quantity":1,"unit_price_cents":1250}]}' \
  127.0.0.1:50051 orders.v1.OrderService/CreateOrder
```

Pass `-nats nats://host:4222` to use an external NATS server instead.

## Protobuf

The API is defined in `proto/orders/v1/orders.proto` and generated with [buf](https://buf.build)
(`buf generate`, using `protoc-gen-go` and `protoc-gen-go-grpc`). Generated code is committed
under `gen/`.

## CI

`.github/workflows/ci.yml` runs on every push and pull request:

- **golangci-lint** (v2, with gosec, errorlint, revive and others; see `.golangci.yml`)
- **proto**: `buf lint`, `buf breaking` against `main` on pull requests, and a check that the
  committed generated code matches `buf generate`
- **test**: `go vet`, `go test -race` with coverage, and a 30-second fuzz run

## Stack

Go 1.27, grpc-go 1.84, protobuf-go 1.36, NATS server 2.15 (embedded) and nats.go 1.55 with the
`jetstream` package, buf, golangci-lint 2.

## License

MIT
