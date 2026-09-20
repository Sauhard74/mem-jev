package api

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/ingest"
	"github.com/sauhard74/mem-jev/internal/store"
)

func TestMapDomainErrorClassifiesClientAndOperationalFailures(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		code      connect.Code
		reason    string
		retryable bool
	}{
		{name: "forbidden field", err: ingest.ErrForbiddenField, code: connect.CodeInvalidArgument, reason: "forbidden_field"},
		{name: "request too large", err: ingest.ErrTraceTooLarge, code: connect.CodeResourceExhausted, reason: "request_too_large"},
		{name: "trace conflict", err: store.ErrTraceConflict, code: connect.CodeAlreadyExists, reason: "trace_conflict"},
		{name: "database unavailable", err: &store.OpError{Operation: "commit", Retryable: true, Err: errors.New("sentinel")}, code: connect.CodeUnavailable, reason: "database_unavailable", retryable: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapped := mapDomainError(withRequestID(context.Background(), "request-id"), tt.err)
			var connectErr *connect.Error
			if !errors.As(mapped, &connectErr) || connectErr.Code() != tt.code {
				t.Fatalf("error = %#v, want code %v", mapped, tt.code)
			}
			detail := errorDetail(t, connectErr)
			if detail.GetReasonCode() != tt.reason || detail.GetRetryable() != tt.retryable {
				t.Fatalf("detail = %#v", detail)
			}
		})
	}
}

func TestErrorDetailsInterceptorPreservesTrustedDomainDetail(t *testing.T) {
	ctx := withRequestID(context.Background(), "request-id")
	archiveFailure := &archive.OpError{Op: "put", Retryable: true, Err: errors.New("sensitive")}
	next := func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		return nil, mapDomainError(ctx, archiveFailure)
	}
	_, err := errorDetailsInterceptor().WrapUnary(next)(ctx, nil)
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		t.Fatalf("error = %#v", err)
	}
	detail := errorDetail(t, connectErr)
	if detail.GetReasonCode() != "archive_unavailable" || !detail.GetRetryable() {
		t.Fatalf("detail = %#v", detail)
	}
}

func errorDetail(t *testing.T, connectErr *connect.Error) *memjevv1.ErrorDetail {
	t.Helper()
	for _, detail := range connectErr.Details() {
		value, err := detail.Value()
		if err != nil {
			t.Fatal(err)
		}
		if typed, ok := value.(*memjevv1.ErrorDetail); ok {
			return typed
		}
	}
	t.Fatal("error lacks memjev ErrorDetail")
	return nil
}
