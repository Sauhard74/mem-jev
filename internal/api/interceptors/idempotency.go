package interceptors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"connectrpc.com/connect"
	"github.com/sauhard74/mem-jev/internal/security"
)

func NewIdempotency() connect.Interceptor {
	return newIdempotency("")
}

func NewIdempotencyFor(procedure string) connect.Interceptor {
	return newIdempotency(procedure)
}

func newIdempotency(procedure string) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
			if procedure != "" && request.Spec().Procedure != procedure {
				return next(ctx, request)
			}
			key := request.Header().Get("Idempotency-Key")
			if !validIdempotencyKey(key) {
				return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("a valid idempotency key is required"))
			}
			sum := sha256.Sum256([]byte(key))
			metadata := security.RequestMetadata{IdempotencyKeyHash: hex.EncodeToString(sum[:])}
			return next(security.WithRequestMetadata(ctx, metadata), request)
		}
	})
}

func validIdempotencyKey(key string) bool {
	if len(key) < 16 || len(key) > 128 {
		return false
	}
	for index := 0; index < len(key); index++ {
		if key[index] < 0x21 || key[index] > 0x7e {
			return false
		}
	}
	return true
}
