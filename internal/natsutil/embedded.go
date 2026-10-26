// Package natsutil starts an in-process NATS server with JetStream, used by the tests
// and by the demo binary when no external server is given.
package natsutil

import (
	"errors"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// StartEmbedded starts a NATS server with JetStream on a random local port, storing
// data under storeDir. Call Shutdown on the result when done.
func StartEmbedded(storeDir string) (*server.Server, error) {
	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      server.RANDOM_PORT,
		JetStream: true,
		StoreDir:  storeDir,
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		return nil, err
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		return nil, errors.New("embedded NATS server did not start")
	}
	return srv, nil
}
