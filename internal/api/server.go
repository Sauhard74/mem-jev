package api

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
	"github.com/sauhard74/mem-jev/internal/api/interceptors"
	"github.com/sauhard74/mem-jev/internal/buildinfo"
	"github.com/sauhard74/mem-jev/internal/ingest"
	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/outcome"
	"github.com/sauhard74/mem-jev/internal/security"
)

const (
	maxRequestBytes = 1 << 20
	defaultTimeout  = 10 * time.Second
)

type Dependencies struct {
	Ingest        *ingest.Service
	Outcome       *outcome.Service
	Authenticator security.Authenticator
	BuildInfo     buildinfo.Info
	Readiness     func(context.Context) error
	Timeout       time.Duration
	Logger        *slog.Logger
}

func NewHandler(dependencies Dependencies) http.Handler {
	timeout := dependencies.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	mux := http.NewServeMux()
	commonOptions := []connect.HandlerOption{
		connect.WithReadMaxBytes(maxRequestBytes),
		connect.WithCodec(strictJSONCodec{name: "json"}),
		connect.WithCodec(strictJSONCodec{name: "json; charset=utf-8"}),
		connect.WithCodec(strictProtoCodec{}),
		connect.WithRecover(func(ctx context.Context, _ connect.Spec, _ http.Header, _ any) error {
			return safeConnectError(ctx, connect.CodeInternal, "internal_error", false)
		}),
	}
	ingestOptions := append([]connect.HandlerOption{}, commonOptions...)
	ingestOptions = append(ingestOptions, connect.WithInterceptors(
		errorDetailsInterceptor(),
		interceptors.NewAuth(dependencies.Authenticator, security.ScopeIngestWrite),
		interceptors.NewIdempotency(),
	))
	ingestPath, ingestHTTPHandler := memjevv1connect.NewIngestServiceHandler(
		&ingestHandler{service: dependencies.Ingest}, ingestOptions...)
	mux.Handle(ingestPath, ingestHTTPHandler)
	outcomeOptions := append([]connect.HandlerOption{}, commonOptions...)
	outcomeOptions = append(outcomeOptions, connect.WithInterceptors(
		errorDetailsInterceptor(),
		interceptors.NewAuth(dependencies.Authenticator, security.ScopeOutcomeWrite),
		interceptors.NewIdempotency(),
	))
	outcomePath, outcomeHTTPHandler := memjevv1connect.NewOutcomeServiceHandler(
		&outcomeHandler{service: dependencies.Outcome}, outcomeOptions...)
	mux.Handle(outcomePath, outcomeHTTPHandler)
	healthPath, healthHTTPHandler := memjevv1connect.NewHealthServiceHandler(
		&healthHandler{info: dependencies.BuildInfo, readiness: dependencies.Readiness}, commonOptions...)
	mux.Handle(healthPath, healthHTTPHandler)

	handler := safeTransportErrors(mux, commonOptions...)
	if dependencies.Logger != nil {
		handler = requestLoggingMiddleware(handler, dependencies.Logger)
	}
	handler = requestContextMiddleware(handler, timeout)
	return http.MaxBytesHandler(handler, maxRequestBytes)
}

type statusResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusResponseWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}

func (w *statusResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func requestLoggingMiddleware(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		startedAt := time.Now()
		observed := &statusResponseWriter{ResponseWriter: response}
		next.ServeHTTP(observed, request)
		status := observed.status
		if status == 0 {
			status = http.StatusOK
		}
		requestBytes := int(request.ContentLength)
		if requestBytes < 0 {
			requestBytes = 0
		}
		observability.LogRequest(request.Context(), logger, observability.RequestFacts{
			RequestID: requestIDFromContext(request.Context()), ResultCode: strconv.Itoa(status),
			Bytes: requestBytes, LatencyMilliseconds: time.Since(startedAt).Milliseconds(),
		})
		observability.RecordHTTPRequest(request.Context(), observability.HTTPMetrics{
			ResultCode: strconv.Itoa(status), Bytes: requestBytes, Latency: time.Since(startedAt),
		})
	})
}

func requestContextMiddleware(next http.Handler, timeout time.Duration) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requestID := newRequestID()
		response.Header().Set("X-Request-ID", requestID)
		response.Header().Set("X-Content-Type-Options", "nosniff")
		response.Header().Set("X-Frame-Options", "DENY")
		response.Header().Set("Referrer-Policy", "no-referrer")
		response.Header().Set("Cache-Control", "no-store")
		response.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		ctx, cancel := context.WithTimeout(withRequestID(request.Context(), requestID), timeout)
		defer cancel()
		next.ServeHTTP(response, request.WithContext(ctx))
	})
}
