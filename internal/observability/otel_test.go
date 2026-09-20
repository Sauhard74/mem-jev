package observability

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestSetupRoutesOTLPSignalsToTheirStandardPaths(t *testing.T) {
	var mu sync.Mutex
	requests := make(map[string]int)
	payloads := make(map[string][]byte)
	collector := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		mu.Lock()
		requests[request.URL.Path]++
		payloads[request.URL.Path] = append(payloads[request.URL.Path], body...)
		mu.Unlock()
		response.WriteHeader(http.StatusOK)
	}))
	defer collector.Close()

	shutdown, err := Setup(context.Background(), TelemetryConfig{
		Endpoint: collector.URL + "/otlp", ServiceName: "test", ServiceVersion: "test", Environment: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, span := StartSpan(context.Background(), "test-span")
	span.End()
	RecordIngest(context.Background(), IngestMetrics{ResultCode: "accepted", Latency: time.Millisecond, Events: 1, Bytes: 1})
	RecordOutcome(context.Background(), OutcomeMetrics{ResultCode: "verified_success", Latency: time.Millisecond, Evidence: 1})
	RecordHTTPRequest(context.Background(), HTTPMetrics{ResultCode: "400", Latency: time.Millisecond, Bytes: 1})
	RecordArchiveCorruption(context.Background(), "hash_mismatch")
	RecordSynthesisAbstention(context.Background(), "opaque_tool")
	RecordOutboxLeaseAge(context.Background(), time.Second)
	RecordOutboxRetry(context.Background(), "temporal_unavailable", false)
	RecordProjectionLag(context.Background(), 2*time.Second)
	RecordRebuildMismatch(context.Background(), "canonical_projection")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if requests["/otlp/v1/traces"] == 0 || requests["/otlp/v1/metrics"] == 0 {
		t.Fatalf("collector requests = %#v", requests)
	}
	if requests["/otlp"] != 0 || requests["/"] != 0 {
		t.Fatalf("signals used unspecialized endpoint: %#v", requests)
	}
	if !bytes.Contains(payloads["/otlp/v1/metrics"], []byte("memjev.http.requests")) {
		t.Fatalf("HTTP boundary metric missing from payload")
	}
	if !bytes.Contains(payloads["/otlp/v1/metrics"], []byte("memjev.outcome.requests")) {
		t.Fatalf("outcome metric missing from payload")
	}
	for _, name := range []string{"memjev.archive.corruptions", "memjev.synthesis.abstentions", "memjev.outbox.lease_age", "memjev.outbox.retries", "memjev.projection.lag", "memjev.projection.rebuild_mismatches"} {
		if !bytes.Contains(payloads["/otlp/v1/metrics"], []byte(name)) {
			t.Fatalf("operational metric %q missing from payload", name)
		}
	}
}
