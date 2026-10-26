// Command ordersd runs the order service, the fulfilment worker and (unless -nats is
// given) an embedded NATS server. It is a demo; the interesting part is the tests.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	ordersv1 "github.com/mustafaautomation/grpc-messaging-tests/gen/orders/v1"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/events"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/fulfilment"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/natsutil"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/orders"
	"github.com/mustafaautomation/grpc-messaging-tests/internal/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC listen address")
	natsURL := flag.String("nats", "", "NATS URL; empty starts an embedded server")
	flag.Parse()
	token := os.Getenv("ORDERS_TOKEN")
	if token == "" {
		log.Fatal("ORDERS_TOKEN must be set")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	url := *natsURL
	if url == "" {
		dir, err := os.MkdirTemp("", "ordersd-js-")
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = os.RemoveAll(dir) }()
		ns, err := natsutil.StartEmbedded(dir)
		if err != nil {
			log.Fatal(err)
		}
		defer ns.Shutdown()
		url = ns.ClientURL()
	}
	nc, err := nats.Connect(url)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := events.EnsureStream(ctx, js, 2*time.Minute); err != nil {
		log.Fatal(err)
	}

	store := orders.NewStore()
	inv := fulfilment.NewMemoryInventory(map[string]int32{"BOOK-1": 100, "PEN-2": 500})
	worker := fulfilment.NewWorker(js, fulfilment.DefaultConfig(), store, inv)
	if err := worker.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer worker.Stop()

	auth := server.BearerAuth{Token: token}
	gs := grpc.NewServer(grpc.UnaryInterceptor(auth.Unary()), grpc.StreamInterceptor(auth.Stream()))
	ordersv1.RegisterOrderServiceServer(gs, &server.Service{Store: store, Publisher: events.JetStreamPublisher{JS: js}})
	hs := health.NewServer()
	healthpb.RegisterHealthServer(gs, hs)
	reflection.Register(gs)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		<-ctx.Done()
		hs.Shutdown()
		gs.GracefulStop()
	}()
	log.Printf("ordersd listening on %s (NATS %s)", lis.Addr(), url)
	if err := gs.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
