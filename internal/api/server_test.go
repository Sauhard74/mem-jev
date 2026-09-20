package api

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/buildinfo"
	"github.com/sauhard74/mem-jev/internal/ingest"
	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/security"
	storememory "github.com/sauhard74/mem-jev/internal/store/memory"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRequestBoundaryLogsSuccessAndMalformedFailureWithoutSubmittedContent(t *testing.T) {
	logs := new(bytes.Buffer)
	logger := observability.NewJSONLogger(logs)
	repository := storememory.NewIngestRepository()
	service := ingest.NewService(archive.NewMemoryStore(), repository, ingest.DefaultPolicy())
	server := httptest.NewServer(NewHandler(Dependencies{
		Ingest: service, Authenticator: security.NewBearerAuthenticator(testCredentialResolver()),
		BuildInfo: buildinfo.Info{Version: "test"}, Logger: logger,
	}))
	defer server.Close()

	valid := validHTTPRequest(t, server.URL, minimalTrace())
	response, err := server.Client().Do(valid)
	if err != nil {
		t.Fatal(err)
	}
	validRequestID := response.Header.Get("X-Request-ID")
	_ = response.Body.Close()
	const sensitive = "SENSITIVE_LOG_SENTINEL_7284"
	malformed := doRawRequest(t, server.Client(), server.URL, []byte(`{"events":"`+sensitive+`"}`))
	malformedRequestID := malformed.Header.Get("X-Request-ID")
	_ = malformed.Body.Close()

	logged := logs.String()
	if strings.Count(logged, "request completed") != 2 || !strings.Contains(logged, validRequestID) || !strings.Contains(logged, malformedRequestID) {
		t.Fatalf("request logs = %s", logged)
	}
	if strings.Contains(logged, sensitive) || strings.Contains(logged, "safe task") || strings.Contains(strings.ToLower(logged), "authorization") {
		t.Fatalf("unsafe request log = %s", logged)
	}
}

func TestHealthAndSecurityHeaders(t *testing.T) {
	server := httptest.NewServer(newTestHandler(t, nil, 0))
	defer server.Close()
	client := memjevv1connect.NewHealthServiceClient(server.Client(), server.URL)
	response, err := client.Check(context.Background(), connect.NewRequest(&memjevv1.CheckRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if response.Msg.GetStatus() != memjevv1.CheckResponse_STATUS_SERVING || response.Msg.GetVersion() != "test" {
		t.Fatalf("health = %#v", response.Msg)
	}
	if response.Header().Get("X-Content-Type-Options") != "nosniff" || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("security headers = %#v", response.Header())
	}
}

func TestUnsupportedMethodAndContentType(t *testing.T) {
	server := httptest.NewServer(newTestHandler(t, nil, 0))
	defer server.Close()
	request, err := http.NewRequest(http.MethodGet, server.URL+memjevv1connect.IngestServiceIngestTraceProcedure, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d", response.StatusCode)
	}
}

func TestHealthReportsNotServingWhenDependencyCheckFails(t *testing.T) {
	repository := storememory.NewIngestRepository()
	service := ingest.NewService(archive.NewMemoryStore(), repository, ingest.DefaultPolicy())
	server := httptest.NewServer(NewHandler(Dependencies{
		Ingest:        service,
		Authenticator: security.NewBearerAuthenticator(testCredentialResolver()),
		BuildInfo:     buildinfo.Info{Version: "test"},
		Readiness:     func(context.Context) error { return errors.New("dependency unavailable") },
	}))
	defer server.Close()
	client := memjevv1connect.NewHealthServiceClient(server.Client(), server.URL)
	response, err := client.Check(context.Background(), connect.NewRequest(&memjevv1.CheckRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if response.Msg.GetStatus() != memjevv1.CheckResponse_STATUS_NOT_SERVING {
		t.Fatalf("status = %v", response.Msg.GetStatus())
	}
}

func timestampAtOne() *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(1, 0).UTC())
}
