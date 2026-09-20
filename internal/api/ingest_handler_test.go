package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
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
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/ingest"
	"github.com/sauhard74/mem-jev/internal/outcome"
	"github.com/sauhard74/mem-jev/internal/policy"
	"github.com/sauhard74/mem-jev/internal/security"
	"github.com/sauhard74/mem-jev/internal/store"
	storememory "github.com/sauhard74/mem-jev/internal/store/memory"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	testToken       = "token-aaaaaaaaaaaaaaaa"
	recallOnlyToken = "token-bbbbbbbbbbbbbbbb"
	testIdempotency = "0123456789abcdef"
)

func TestIngestHTTPAcceptsAndDeduplicates(t *testing.T) {
	server := httptest.NewServer(newTestHandler(t, nil, 0))
	defer server.Close()
	client := memjevv1connect.NewIngestServiceClient(server.Client(), server.URL)

	first, err := client.IngestTrace(context.Background(), validConnectRequest(testIdempotency, minimalTrace()))
	if err != nil {
		t.Fatal(err)
	}
	if first.Msg.GetDisposition() != memjevv1.IngestDisposition_INGEST_DISPOSITION_ACCEPTED || first.Msg.GetReceiptId() == "" {
		t.Fatalf("response = %#v", first.Msg)
	}
	second, err := client.IngestTrace(context.Background(), validConnectRequest(testIdempotency, minimalTrace()))
	if err != nil {
		t.Fatal(err)
	}
	if second.Msg.GetDisposition() != memjevv1.IngestDisposition_INGEST_DISPOSITION_DUPLICATE || second.Msg.GetReceiptId() != first.Msg.GetReceiptId() {
		t.Fatalf("first=%#v second=%#v", first.Msg, second.Msg)
	}
}

func TestIngestHTTPMapsBoundaryFailures(t *testing.T) {
	server := httptest.NewServer(newTestHandler(t, nil, 0))
	defer server.Close()
	client := memjevv1connect.NewIngestServiceClient(server.Client(), server.URL)

	tests := []struct {
		name   string
		modify func(*connect.Request[memjevv1.IngestTraceRequest])
		code   connect.Code
	}{
		{name: "missing key", modify: func(request *connect.Request[memjevv1.IngestTraceRequest]) { request.Header().Del("Idempotency-Key") }, code: connect.CodeInvalidArgument},
		{name: "invalid token", modify: func(request *connect.Request[memjevv1.IngestTraceRequest]) {
			request.Header().Set("Authorization", "Bearer invalid-token-value")
		}, code: connect.CodeUnauthenticated},
		{name: "spoofed tenant", modify: func(request *connect.Request[memjevv1.IngestTraceRequest]) {
			request.Header().Set("X-Tenant-ID", "tenant_b_secret")
		}, code: connect.CodeInvalidArgument},
		{name: "consent", modify: func(request *connect.Request[memjevv1.IngestTraceRequest]) {
			request.Header().Set("Authorization", "Bearer "+recallOnlyToken)
		}, code: connect.CodePermissionDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := validConnectRequest(testIdempotency, minimalTrace())
			tt.modify(request)
			_, err := client.IngestTrace(context.Background(), request)
			if connect.CodeOf(err) != tt.code {
				t.Fatalf("code = %v error=%v, want %v", connect.CodeOf(err), err, tt.code)
			}
			if strings.Contains(err.Error(), "tenant_b_secret") {
				t.Fatalf("error leaked tenant header: %v", err)
			}
		})
	}
}

func TestIngestHTTPReturnsConflictForKeyReuse(t *testing.T) {
	server := httptest.NewServer(newTestHandler(t, nil, 0))
	defer server.Close()
	client := memjevv1connect.NewIngestServiceClient(server.Client(), server.URL)
	if _, err := client.IngestTrace(context.Background(), validConnectRequest(testIdempotency, minimalTrace())); err != nil {
		t.Fatal(err)
	}
	changed := minimalTrace()
	changed.Task = "different task"
	_, err := client.IngestTrace(context.Background(), validConnectRequest(testIdempotency, changed))
	if connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("code = %v error=%v", connect.CodeOf(err), err)
	}
}

func TestIngestHTTPRejectsOversizedAndMalformedBodies(t *testing.T) {
	server := httptest.NewServer(newTestHandler(t, nil, 0))
	defer server.Close()

	oversized := minimalTrace()
	oversized.Task = strings.Repeat("x", (1<<20)+1)
	body, err := protojson.Marshal(oversized)
	if err != nil {
		t.Fatal(err)
	}
	response := doRawRequest(t, server.Client(), server.URL, body)
	if response.StatusCode != http.StatusTooManyRequests {
		payload, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		t.Fatalf("oversized status=%d body=%s", response.StatusCode, payload)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}

	response = doRawRequest(t, server.Client(), server.URL, []byte("{"))
	if response.StatusCode != http.StatusBadRequest {
		payload, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		t.Fatalf("malformed status=%d body=%s", response.StatusCode, payload)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIngestHTTPDecodeErrorsNeverEchoSubmittedValues(t *testing.T) {
	server := httptest.NewServer(newTestHandler(t, nil, 0))
	defer server.Close()
	const sensitive = "SENSITIVE_SENTINEL_7284"
	tests := []struct {
		name string
		body string
	}{
		{name: "timestamp", body: `{"clientTraceId":"trace","harness":"test","events":[{"clientEventId":"event","occurredAt":"` + sensitive + `","kind":"EVENT_KIND_EXECUTE","toolName":"shell"}]}`},
		{name: "number", body: `{"clientTraceId":"trace","harness":"test","events":[{"clientEventId":"event","occurredAt":"1970-01-01T00:00:01Z","kind":"EVENT_KIND_EXECUTE","toolName":"shell","result":{"state":"TOOL_RESULT_STATE_SUCCESS","exitCode":"` + sensitive + `"}}]}`},
		{name: "enum", body: `{"clientTraceId":"trace","harness":"test","events":[{"clientEventId":"event","occurredAt":"1970-01-01T00:00:01Z","kind":"` + sensitive + `","toolName":"shell"}]}`},
		{name: "message", body: `{"clientTraceId":"trace","harness":"test","events":"` + sensitive + `"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := doRawRequest(t, server.Client(), server.URL, []byte(tt.body))
			defer func() {
				if err := response.Body.Close(); err != nil {
					t.Error(err)
				}
			}()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusBadRequest || bytes.Contains(body, []byte(sensitive)) {
				t.Fatalf("status=%d body=%s", response.StatusCode, body)
			}
			if !bytes.Contains(body, []byte(`"details"`)) || !bytes.Contains(body, []byte(`"reasonCode":"invalid_request"`)) {
				t.Fatalf("body lacks structured safe error: %s", body)
			}
		})
	}
}

func TestIngestHTTPRejectsUnknownJSONAndProtobufFields(t *testing.T) {
	server := httptest.NewServer(newTestHandler(t, nil, 0))
	defer server.Close()

	jsonBody, err := protojson.Marshal(minimalTrace())
	if err != nil {
		t.Fatal(err)
	}
	jsonBody = append(jsonBody[:len(jsonBody)-1], []byte(`,"unknownSentinel":"must-reject"}`)...)
	response := doRawRequest(t, server.Client(), server.URL, jsonBody)
	if response.StatusCode != http.StatusBadRequest {
		payload, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		t.Fatalf("unknown JSON status=%d body=%s", response.StatusCode, payload)
	}
	_ = response.Body.Close()
	charsetRequest, err := http.NewRequest(http.MethodPost, server.URL+memjevv1connect.IngestServiceIngestTraceProcedure, bytes.NewReader(jsonBody))
	if err != nil {
		t.Fatal(err)
	}
	charsetRequest.Header.Set("Content-Type", "application/json; charset=utf-8")
	charsetRequest.Header.Set("Authorization", "Bearer "+testToken)
	charsetRequest.Header.Set("Idempotency-Key", testIdempotency)
	response, err = server.Client().Do(charsetRequest)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusBadRequest {
		payload, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		t.Fatalf("unknown charset JSON status=%d body=%s", response.StatusCode, payload)
	}
	_ = response.Body.Close()

	unknown := []byte{0xfa, 0x7f, 0x01, 'x'} // Field 2047, wire type 2, one-byte payload.
	protobufCases := []struct {
		name   string
		mutate func(*memjevv1.IngestTraceRequest)
	}{
		{name: "root", mutate: func(message *memjevv1.IngestTraceRequest) { message.ProtoReflect().SetUnknown(unknown) }},
		{name: "event", mutate: func(message *memjevv1.IngestTraceRequest) { message.Events[0].ProtoReflect().SetUnknown(unknown) }},
		{name: "field", mutate: func(message *memjevv1.IngestTraceRequest) {
			message.Events[0].Fields[0].ProtoReflect().SetUnknown(unknown)
		}},
		{name: "result", mutate: func(message *memjevv1.IngestTraceRequest) {
			message.Events[0].Result.ProtoReflect().SetUnknown(unknown)
		}},
		{name: "evidence", mutate: func(message *memjevv1.IngestTraceRequest) {
			message.Events[0].Result.Evidence = []*memjevv1.Field{{Name: "assertion", StringValue: "passed"}}
			message.Events[0].Result.Evidence[0].ProtoReflect().SetUnknown(unknown)
		}},
		{name: "timestamp", mutate: func(message *memjevv1.IngestTraceRequest) {
			message.Events[0].OccurredAt.ProtoReflect().SetUnknown(unknown)
		}},
	}
	for _, tt := range protobufCases {
		t.Run("protobuf "+tt.name, func(t *testing.T) {
			message := proto.Clone(minimalTrace()).(*memjevv1.IngestTraceRequest)
			tt.mutate(message)
			protoBody, err := proto.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			request, err := http.NewRequest(http.MethodPost, server.URL+memjevv1connect.IngestServiceIngestTraceProcedure, bytes.NewReader(protoBody))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/proto")
			request.Header.Set("Authorization", "Bearer "+testToken)
			request.Header.Set("Idempotency-Key", testIdempotency)
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			payload, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil {
				t.Fatalf("read=%v close=%v", readErr, closeErr)
			}
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("unknown protobuf status=%d body=%s", response.StatusCode, payload)
			}
		})
	}
}

func TestIngestHTTPMapsDeadlineAndRedactsInternalError(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		repository := blockingRepository{}
		server := httptest.NewServer(newTestHandler(t, repository, 25*time.Millisecond))
		defer server.Close()
		client := memjevv1connect.NewIngestServiceClient(server.Client(), server.URL)
		_, err := client.IngestTrace(context.Background(), validConnectRequest(testIdempotency, minimalTrace()))
		if connect.CodeOf(err) != connect.CodeDeadlineExceeded {
			t.Fatalf("code=%v error=%v", connect.CodeOf(err), err)
		}
	})

	t.Run("internal error", func(t *testing.T) {
		const sensitive = "database-secret-detail"
		server := httptest.NewServer(newTestHandler(t, failingRepository{err: errors.New(sensitive)}, 0))
		defer server.Close()
		request := validHTTPRequest(t, server.URL, minimalTrace())
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if closeErr := response.Body.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		}()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusInternalServerError || bytes.Contains(body, []byte(sensitive)) {
			t.Fatalf("status=%d body=%s", response.StatusCode, body)
		}
	})
}

func validConnectRequest(key string, message *memjevv1.IngestTraceRequest) *connect.Request[memjevv1.IngestTraceRequest] {
	request := connect.NewRequest(message)
	request.Header().Set("Authorization", "Bearer "+testToken)
	request.Header().Set("Idempotency-Key", key)
	return request
}

func validHTTPRequest(t *testing.T, baseURL string, message *memjevv1.IngestTraceRequest) *http.Request {
	t.Helper()
	body, err := protojson.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, baseURL+memjevv1connect.IngestServiceIngestTraceProcedure, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+testToken)
	request.Header.Set("Idempotency-Key", testIdempotency)
	return request
}

func doRawRequest(t *testing.T, client *http.Client, baseURL string, body []byte) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, baseURL+memjevv1connect.IngestServiceIngestTraceProcedure, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+testToken)
	request.Header.Set("Idempotency-Key", testIdempotency)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func minimalTrace() *memjevv1.IngestTraceRequest {
	return validCommandForAPI().Request
}

func validCommandForAPI() ingest.Command {
	return ingest.Command{
		Request: &memjevv1.IngestTraceRequest{
			ClientTraceId: "trace-1",
			Harness:       "api-test",
			Task:          "safe task",
			Events: []*memjevv1.TraceEvent{{
				ClientEventId: "event-1",
				OccurredAt:    timestampAtOne(),
				Kind:          memjevv1.EventKind_EVENT_KIND_EXECUTE,
				ToolName:      "shell",
				Fields:        []*memjevv1.Field{{Name: "command", StringValue: "true"}},
				Result:        &memjevv1.ToolResult{State: memjevv1.ToolResultState_TOOL_RESULT_STATE_SUCCESS},
			}},
		},
	}
}

func newTestHandler(t *testing.T, repository store.IngestRepository, timeout time.Duration) http.Handler {
	t.Helper()
	if repository == nil {
		repository = storememory.NewIngestRepository()
	}
	archives := archive.NewMemoryStore()
	service := ingest.NewService(archives, repository, ingest.DefaultPolicy())
	var outcomeService *outcome.Service
	if outcomeRepository, ok := repository.(store.OutcomeRepository); ok {
		outcomeService = outcome.NewService(outcomeRepository, outcome.DefaultPolicy())
	}
	return NewHandler(Dependencies{
		Ingest:        service,
		Outcome:       outcomeService,
		Authenticator: security.NewBearerAuthenticator(testCredentialResolver()),
		BuildInfo:     buildinfo.Info{Version: "test", Commit: "abc123", BuiltAt: "2026-09-20T00:00:00Z"},
		Timeout:       timeout,
	})
}

type credentialResolver map[string]security.Principal

func testCredentialResolver() credentialResolver {
	return credentialResolver{
		tokenHash(testToken):       testPrincipal(policy.LearnAndRecall),
		tokenHash(recallOnlyToken): testPrincipal(policy.RecallOnly),
	}
}

func (r credentialResolver) ResolveCredentialHash(hash string) (security.Principal, error) {
	principal, ok := r[hash]
	if !ok {
		return security.Principal{}, security.ErrUnknownCredential
	}
	return principal, nil
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func testPrincipal(consent policy.ConsentMode) security.Principal {
	return security.Principal{
		TenantID: domain.TenantID("tenant_a"),
		Region:   "local",
		Scopes:   map[string]struct{}{security.ScopeIngestWrite: {}, security.ScopeOutcomeWrite: {}},
		Consent:  consent,
	}
}

type blockingRepository struct{}

func (blockingRepository) Commit(ctx context.Context, _ store.CommitIngestRequest) (store.IngestReceipt, error) {
	<-ctx.Done()
	return store.IngestReceipt{}, ctx.Err()
}

type failingRepository struct{ err error }

func (r failingRepository) Commit(context.Context, store.CommitIngestRequest) (store.IngestReceipt, error) {
	return store.IngestReceipt{}, r.err
}
