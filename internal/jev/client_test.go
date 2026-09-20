package jev

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type staticSecret string

func (s staticSecret) Token() (string, error) { return string(s), nil }

func TestClientSendsOfficialContractAndValidatesTypedResponse(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Method != http.MethodPost || request.URL.Path != "/v1/systemone" || request.Header.Get("Authorization") != "Bearer test-secret" || request.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected request: %s %s headers=%v", request.Method, request.URL.Path, request.Header)
		}
		var body struct {
			Model     string                     `json:"model"`
			State     map[string]any             `json:"state"`
			Questions map[string]json.RawMessage `json:"questions"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.Model != "jev-1.13.0" || len(body.State) != 2 || len(body.Questions) != 5 {
			t.Errorf("unexpected body: %#v", body)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(validProviderResponse()))
	}))
	defer server.Close()

	client := newTestClient(t, server, staticSecret("test-secret"), 1<<20)
	result, err := client.Evaluate(context.Background(), map[string]any{"task": "repair tests", "procedure": "run checks"})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || result.Model != "jev-1.13.0" || result.Usage.InputTokens != 321 || result.Judgment.IntentFit.ScoreMicros != 3_500_000 {
		t.Fatalf("unexpected result: %#v calls=%d", result, calls.Load())
	}
	features, err := result.Judgment.Features()
	if err != nil || len(features) != 5 {
		t.Fatalf("features = %#v, %v", features, err)
	}
}

func TestClientClassifiesFailuresWithoutLeakingSecretsOrBodies(t *testing.T) {
	secret := "super-secret-token"
	tests := []struct {
		name, responseBody, wantCode string
		status                       int
		maxBody                      int64
	}{
		{name: "auth", status: http.StatusUnauthorized, responseBody: secret, wantCode: FailureAuthentication, maxBody: 1 << 20},
		{name: "throttle", status: http.StatusTooManyRequests, responseBody: "try later", wantCode: FailureThrottled, maxBody: 1 << 20},
		{name: "overload", status: 529, responseBody: "provider says capacity unavailable", wantCode: FailureOverloaded, maxBody: 1 << 20},
		{name: "server", status: http.StatusInternalServerError, responseBody: "backend trace", wantCode: FailureProvider, maxBody: 1 << 20},
		{name: "oversized", status: http.StatusOK, responseBody: strings.Repeat("x", 100), wantCode: FailureOversized, maxBody: 32},
		{name: "malformed", status: http.StatusOK, responseBody: secret + "{", wantCode: FailureInvalidResponse, maxBody: 1 << 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.WriteHeader(tt.status)
				_, _ = response.Write([]byte(tt.responseBody))
			}))
			defer server.Close()
			client := newTestClient(t, server, staticSecret(secret), tt.maxBody)
			_, err := client.Evaluate(context.Background(), map[string]string{"task": secret})
			var providerErr *ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != tt.wantCode {
				t.Fatalf("Evaluate() error = %#v; want %s", err, tt.wantCode)
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), tt.responseBody) {
				t.Fatalf("error leaked sensitive material: %v", err)
			}
		})
	}
}

func TestClientRejectsWrongTypedAnswerAndRedirect(t *testing.T) {
	bad := strings.Replace(validProviderResponse(), `"type":"noul","noul":0.9`, `"type":"score","noul":0.9`, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/redirect" {
			http.Redirect(response, request, "/v1/systemone", http.StatusTemporaryRedirect)
			return
		}
		_, _ = response.Write([]byte(bad))
	}))
	defer server.Close()
	client := newTestClient(t, server, staticSecret("test-secret"), 1<<20)
	if _, err := client.Evaluate(context.Background(), "state"); !hasProviderCode(err, FailureInvalidResponse) {
		t.Fatalf("typed mismatch error = %v", err)
	}

	rubric, err := DefaultRubricV1("jev-1.13.0")
	if err != nil {
		t.Fatal(err)
	}
	redirectClient, err := NewClient(ClientConfig{Endpoint: server.URL + "/redirect", AllowedHost: strings.TrimPrefix(server.URL, "https://"), Model: rubric.Model, Rubric: rubric, Secret: staticSecret("test-secret"), HTTPClient: server.Client(), Timeout: time.Second, MaximumRequestBytes: 1 << 20, MaximumResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = redirectClient.Evaluate(context.Background(), "state"); !hasProviderCode(err, FailureProvider) {
		t.Fatalf("redirect error = %v", err)
	}
}

func TestClientHonorsContextCancellation(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = response.Write([]byte(validProviderResponse()))
	}))
	defer server.Close()
	client := newTestClient(t, server, staticSecret("test-secret"), 1<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := client.Evaluate(ctx, "state")
	if !hasProviderCode(err, FailureTimeout) && !hasProviderCode(err, FailureCancelled) {
		t.Fatalf("cancellation error = %v", err)
	}
}

func newTestClient(t *testing.T, server *httptest.Server, secret SecretSource, maximumResponseBytes int64) *Client {
	t.Helper()
	rubric, err := DefaultRubricV1("jev-1.13.0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(ClientConfig{Endpoint: server.URL + "/v1/systemone", AllowedHost: strings.TrimPrefix(server.URL, "https://"), Model: rubric.Model, Rubric: rubric, Secret: secret, HTTPClient: server.Client(), Timeout: time.Second, MaximumRequestBytes: 1 << 20, MaximumResponseBytes: maximumResponseBytes})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func hasProviderCode(err error, code string) bool {
	var providerErr *ProviderError
	return errors.As(err, &providerErr) && providerErr.Code == code
}

func validProviderResponse() string {
	return `{"model":"jev-1.13.0","answers":{"intent_fit":{"type":"score","score":3.5,"legend":{"0":"Unrelated","1":"Weakly related","2":"Partially aligned","3":"Strongly aligned","4":"Exact intent match"},"probabilities":{"0":0,"1":0.05,"2":0.1,"3":0.25,"4":0.6},"confidence":0.8},"preconditions_likely_satisfied":{"type":"noul","noul":0.9},"task_coverage":{"type":"score","score":2,"legend":{"0":"None","1":"Small fragment","2":"Material portion","3":"Most","4":"All"},"probabilities":{"0":0.05,"1":0.1,"2":0.55,"3":0.25,"4":0.05},"confidence":0.7},"contradicts_request":{"type":"noul","noul":0.125},"useful_as_partial_plan":{"type":"noul","noul":0.6}},"usage":{"input_tokens":321,"output_tokens":44}}`
}

func TestFileSecretUsedByClient(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jev.key")
	if err := os.WriteFile(path, []byte("test-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	secret, err := NewFileSecret(path, 128)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer test-secret" {
			t.Error("missing bearer token")
		}
		_, _ = response.Write([]byte(validProviderResponse()))
	}))
	defer server.Close()
	if _, err = newTestClient(t, server, secret, 1<<20).Evaluate(context.Background(), "state"); err != nil {
		t.Fatal(err)
	}
}
