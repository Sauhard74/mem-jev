package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/ingest"
	"github.com/sauhard74/mem-jev/internal/outcome"
	"github.com/sauhard74/mem-jev/internal/policy"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store"
)

type bufferedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newBufferedResponse() *bufferedResponse    { return &bufferedResponse{header: make(http.Header)} }
func (r *bufferedResponse) Header() http.Header { return r.header }
func (r *bufferedResponse) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
}
func (r *bufferedResponse) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.body.Write(body)
}

func safeTransportErrors(next http.Handler, options ...connect.HandlerOption) http.Handler {
	errorWriter := connect.NewErrorWriter(options...)
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		buffered := newBufferedResponse()
		next.ServeHTTP(buffered, request)
		status := buffered.status
		if status == 0 {
			status = http.StatusOK
		}
		if mustReplaceUnstructuredError(status, buffered.body.Bytes()) && errorWriter.IsSupported(request) {
			copyHeaders(response.Header(), buffered.header)
			response.Header().Del("Content-Length")
			response.Header().Del("Content-Encoding")
			code, reason, retryable := safeTransportClassification(status)
			_ = errorWriter.Write(response, request, safeConnectError(request.Context(), code, reason, retryable))
			return
		}
		copyHeaders(response.Header(), buffered.header)
		response.WriteHeader(status)
		_, _ = response.Write(buffered.body.Bytes())
	})
}

func mustReplaceUnstructuredError(status int, body []byte) bool {
	return !hasStructuredError(body) && (status == http.StatusBadRequest || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError)
}

func safeTransportClassification(status int) (connect.Code, string, bool) {
	switch status {
	case http.StatusTooManyRequests:
		return connect.CodeResourceExhausted, "request_too_large", false
	case http.StatusBadRequest:
		return connect.CodeInvalidArgument, "invalid_request", false
	default:
		return connect.CodeInternal, "internal_error", false
	}
}

func hasStructuredError(body []byte) bool {
	var wire struct {
		Details []json.RawMessage `json:"details"`
	}
	return json.Unmarshal(body, &wire) == nil && len(wire.Details) > 0
}

func copyHeaders(destination, source http.Header) {
	for key, values := range source {
		destination[key] = append([]string(nil), values...)
	}
}

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
	case errors.Is(err, outcome.ErrInvalidCommand), errors.Is(err, store.ErrInvalidOutcomeCommit):
		code, reason = connect.CodeInvalidArgument, "invalid_outcome"
	case errors.Is(err, outcome.ErrSensitiveEvidence):
		code, reason = connect.CodeInvalidArgument, "sensitive_evidence"
	case errors.Is(err, ingest.ErrForbiddenField):
		code, reason = connect.CodeInvalidArgument, "forbidden_field"
	case errors.Is(err, ingest.ErrValueTooLarge), errors.Is(err, ingest.ErrTraceTooLarge):
		code, reason = connect.CodeResourceExhausted, "request_too_large"
	case errors.Is(err, ingest.ErrPermissionDenied), errors.Is(err, outcome.ErrPermissionDenied), errors.Is(err, policy.ErrConsentDenied):
		code, reason = connect.CodePermissionDenied, "learning_not_permitted"
	case errors.Is(err, store.ErrIdempotencyConflict):
		code, reason = connect.CodeAlreadyExists, "idempotency_conflict"
	case errors.Is(err, retrieval.ErrIdempotencyConflict):
		code, reason = connect.CodeAlreadyExists, "idempotency_conflict"
	case errors.Is(err, store.ErrTraceConflict):
		code, reason = connect.CodeAlreadyExists, "trace_conflict"
	case errors.Is(err, store.ErrOutcomeTraceNotFound):
		code, reason = connect.CodeNotFound, "trace_not_found"
	case errors.Is(err, store.ErrOutcomeSelectionNotFound):
		code, reason = connect.CodeNotFound, "selection_not_found"
	case errors.Is(err, store.ErrOutcomeSupersessionInvalid):
		code, reason = connect.CodeFailedPrecondition, "invalid_supersession"
	case errors.Is(err, retrieval.ErrInvalidQuery), errors.Is(err, retrieval.ErrInvalidRun):
		code, reason = connect.CodeInvalidArgument, "invalid_retrieval"
	case errors.Is(err, retrieval.ErrRunNotFound):
		code, reason = connect.CodeNotFound, "retrieval_not_found"
	case errors.Is(err, retrieval.ErrRecallDenied):
		code, reason = connect.CodePermissionDenied, "recall_not_permitted"
	default:
		var retrievalErr *retrieval.ServiceError
		if errors.As(err, &retrievalErr) {
			switch retrievalErr.Code {
			case "idempotency_conflict":
				code, reason = connect.CodeAlreadyExists, "idempotency_conflict"
			case "required_channel_unavailable", "snapshot_unavailable", "serving_config_mismatch", "policy_manifest_unavailable", "ranker_manifest_unavailable", "run_lookup_failed", "run_persistence_failed", "plan_persistence_failed", "candidate_snapshot_missing", "effect_inference_failed":
				code, reason, retryable = connect.CodeUnavailable, retrievalErr.Code, true
			default:
				code, reason = connect.CodeInternal, "retrieval_failed"
			}
			break
		}
		var archiveErr *archive.OpError
		if errors.As(err, &archiveErr) && archiveErr.Retryable {
			code, reason, retryable = connect.CodeUnavailable, "archive_unavailable", true
		}
		var storeErr *store.OpError
		if errors.As(err, &storeErr) && storeErr.Retryable {
			code, reason, retryable = connect.CodeUnavailable, "database_unavailable", true
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
			var connectErr *connect.Error
			if errors.As(err, &connectErr) && hasMemjevErrorDetail(connectErr) {
				return nil, err
			}
			code := connect.CodeOf(err)
			reason, retryable := reasonForCode(code)
			return nil, safeConnectError(ctx, code, reason, retryable)
		}
	})
}

func hasMemjevErrorDetail(connectErr *connect.Error) bool {
	for _, detail := range connectErr.Details() {
		value, err := detail.Value()
		if err == nil {
			if _, ok := value.(*memjevv1.ErrorDetail); ok {
				return true
			}
		}
	}
	return false
}
