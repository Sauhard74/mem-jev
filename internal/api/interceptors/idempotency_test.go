package interceptors

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/security"
)

func TestIdempotencyInterceptorRejectsMissingAndMalformedKeys(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{name: "missing"},
		{name: "short", key: "too-short"},
		{name: "space", key: "0123456789abc def"},
		{name: "non ASCII", key: "0123456789abcdeé"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := connect.NewRequest(&memjevv1.IngestTraceRequest{})
			req.Header().Set("Idempotency-Key", test.key)
			_, err := NewIdempotency().WrapUnary(successUnary)(context.Background(), req)
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("code = %v, error = %v", connect.CodeOf(err), err)
			}
		})
	}
}

func TestIdempotencyInterceptorStoresOnlySHA256(t *testing.T) {
	const key = "0123456789abcdef"
	const wantHash = "9f9f5111f7b27a781f1f1ddde5ebc2dd2b796bfc7365c9c28b548e564176929f"
	next := func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		metadata, ok := security.RequestMetadataFromContext(ctx)
		if !ok {
			t.Fatal("request metadata missing")
		}
		if metadata.IdempotencyKeyHash != wantHash {
			t.Fatalf("hash = %q; want %q", metadata.IdempotencyKeyHash, wantHash)
		}
		if contains(metadata.IdempotencyKeyHash, key) {
			t.Fatal("raw key survived in request metadata")
		}
		return connect.NewResponse(&memjevv1.IngestTraceResponse{}), nil
	}
	req := connect.NewRequest(&memjevv1.IngestTraceRequest{})
	req.Header().Set("Idempotency-Key", key)

	if _, err := NewIdempotency().WrapUnary(next)(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}
