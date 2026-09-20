package main

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/sauhard74/mem-jev/internal/config"
)

func TestBuildTemporalTLSSupportsDevelopmentAndSecureServerName(t *testing.T) {
	plain, err := buildTemporalTLS(config.TemporalConfig{})
	if err != nil || plain != nil {
		t.Fatalf("plain=%#v error=%v", plain, err)
	}
	secure, err := buildTemporalTLS(config.TemporalConfig{TLSServerName: "temporal.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	if secure.ServerName != "temporal.example.test" || secure.MinVersion != tls.VersionTLS12 {
		t.Fatalf("TLS config = %#v", secure)
	}
}

func TestWorkerHealthFailsClosedBeforeReadiness(t *testing.T) {
	var ready atomic.Bool
	server := newHealthServer(":0", &ready, nil, nil)
	liveRequest := httptest.NewRequest(http.MethodGet, "/livez", nil)
	liveResponse := httptest.NewRecorder()
	server.Handler.ServeHTTP(liveResponse, liveRequest)
	if liveResponse.Code != http.StatusOK {
		t.Fatalf("liveness status = %d", liveResponse.Code)
	}
	readyRequest := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	readyResponse := httptest.NewRecorder()
	server.Handler.ServeHTTP(readyResponse, readyRequest)
	if readyResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status = %d", readyResponse.Code)
	}
}

func TestSynthesisVersionsAreExplicit(t *testing.T) {
	versions := synthesisVersions()
	if versions.Sanitizer == "" || versions.Registry == "" || versions.Policy == "" || versions.GraphBuilder == "" || versions.Synthesizer == "" {
		t.Fatalf("versions = %#v", versions)
	}
}
