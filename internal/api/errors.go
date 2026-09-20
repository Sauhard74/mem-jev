package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/ingest"
	"github.com/sauhard74/mem-jev/internal/policy"
	"github.com/sauhard74/mem-jev/internal/store"
)

type requestIDContextKey struct{}

var requestIDFallback atomic.Uint64

func withRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDContextKey{}, requestID)
}

func requestIDFromContext(ctx context.Context) string {
	value, _ := ctx.Value(requestIDContextKey{}).(string)
	if value == "" {
		return newRequestID()
	}
	return value
}

func newRequestID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err == nil {
		return hex.EncodeToString(value[:])
	}
	return fmt.Sprintf("fallback-%x-%x", time.Now().UnixNano(), requestIDFallback.Add(1))
}

func mapDomainError(ctx context.Context, err error) error {
	code, reason, retryable := connect.CodeInternal, "internal_error", false
	switch {
	case errors.Is(err, context.Canceled):
		code, reason = connect.CodeCanceled, "request_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		code, reason, retryable = connect.CodeDeadlineExceeded, "deadline_exceeded", true
	case errors.Is(err, ingest.ErrInvalidCommand), errors.Is(err, ingest.ErrInvalidTrace), errors.Is(err, store.ErrInvalidCommit):
		code, reason = connect.CodeInvalidArgument, "invalid_request"
	case errors.Is(err, ingest.ErrValueTooLarge), errors.Is(err, ingest.ErrTraceTooLarge):
		code, reason = connect.CodeResourceExhausted, "request_too_large"
	case errors.Is(err, ingest.ErrPermissionDenied), errors.Is(err, policy.ErrConsentDenied):
		code, reason = connect.CodePermissionDenied, "learning_not_permitted"
	case errors.Is(err, store.ErrIdempotencyConflict):
		code, reason = connect.CodeAlreadyExists, "idempotency_conflict"
	default:
		var archiveErr *archive.OpError
		if errors.As(err, &archiveErr) && archiveErr.Retryable {
			code, reason, retryable = connect.CodeUnavailable, "archive_unavailable", true
		}
	}
	return safeConnectError(ctx, code, reason, retryable)
}

func safeConnectError(ctx context.Context, code connect.Code, reason string, retryable bool) *connect.Error {
	connectErr := connect.NewError(code, errors.New("request failed"))
	detail, err := connect.NewErrorDetail(&memjevv1.ErrorDetail{
		RequestId:  requestIDFromContext(ctx),
		ReasonCode: reason,
		Retryable:  retryable,
	})
	if err == nil {
		connectErr.AddDetail(detail)
	}
	return connectErr
}

func reasonForCode(code connect.Code) (string, bool) {
	switch code {
	case connect.CodeCanceled:
		return "request_canceled", false
	case connect.CodeInvalidArgument:
		return "invalid_request", false
	case connect.CodeDeadlineExceeded:
		return "deadline_exceeded", true
	case connect.CodeNotFound:
		return "not_found", false
	case connect.CodeAlreadyExists:
		return "idempotency_conflict", false
	case connect.CodePermissionDenied:
		return "permission_denied", false
	case connect.CodeResourceExhausted:
		return "resource_exhausted", true
	case connect.CodeUnauthenticated:
		return "authentication_required", false
	case connect.CodeUnavailable:
		return "service_unavailable", true
	default:
		return "internal_error", false
	}
}

func errorDetailsInterceptor() connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
			response, err := next(ctx, request)
			if err == nil {
				return response, nil
			}
			code := connect.CodeOf(err)
			reason, retryable := reasonForCode(code)
			return nil, safeConnectError(ctx, code, reason, retryable)
		}
	})
}
