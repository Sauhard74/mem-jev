package api

import (
	"context"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
	"github.com/sauhard74/mem-jev/internal/api/interceptors"
	"github.com/sauhard74/mem-jev/internal/buildinfo"
	"github.com/sauhard74/mem-jev/internal/ingest"
	"github.com/sauhard74/mem-jev/internal/security"
)

const (
	maxRequestBytes = 1 << 20
	defaultTimeout  = 10 * time.Second
)

type Dependencies struct {
	Ingest        *ingest.Service
	Authenticator security.Authenticator
	BuildInfo     buildinfo.Info
	Readiness     func(context.Context) error
	Timeout       time.Duration
}

func NewHandler(dependencies Dependencies) http.Handler {
	timeout := dependencies.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	mux := http.NewServeMux()
	commonOptions := []connect.HandlerOption{
		connect.WithReadMaxBytes(maxRequestBytes),
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
	healthPath, healthHTTPHandler := memjevv1connect.NewHealthServiceHandler(
		&healthHandler{info: dependencies.BuildInfo, readiness: dependencies.Readiness}, commonOptions...)
	mux.Handle(healthPath, healthHTTPHandler)

	handler := requestContextMiddleware(mux, timeout)
	return http.MaxBytesHandler(handler, maxRequestBytes)
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
