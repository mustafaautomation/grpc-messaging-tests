package server

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// publicMethods skip authentication (health checks).
var publicMethods = map[string]bool{
	"/grpc.health.v1.Health/Check": true,
	"/grpc.health.v1.Health/Watch": true,
	"/grpc.health.v1.Health/List":  true,
}

// BearerAuth checks "authorization: Bearer <token>" metadata against a static token.
// It is deliberately simple; the point is to test the Unauthenticated paths.
type BearerAuth struct {
	Token string
}

func (a BearerAuth) check(ctx context.Context, method string) error {
	if publicMethods[method] {
		return nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) == 0 {
		return status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	token, ok := strings.CutPrefix(values[0], "Bearer ")
	if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(a.Token)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid bearer token")
	}
	return nil
}

// Unary returns a unary server interceptor.
func (a BearerAuth) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if err := a.check(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return h(ctx, req)
	}
}

// Stream returns a stream server interceptor.
func (a BearerAuth) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if err := a.check(ss.Context(), info.FullMethod); err != nil {
			return err
		}
		return h(srv, ss)
	}
}
