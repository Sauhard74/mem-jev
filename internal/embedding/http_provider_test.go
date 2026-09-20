package embedding_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sauhard74/mem-jev/internal/embedding"
	"github.com/sauhard74/mem-jev/internal/retrieval"
)

func TestHTTPProviderUsesPinnedIdentityAndOpaqueBearer(t *testing.T) {
	manifest := httpProviderManifest(t)
	input, err := embedding.InputFromQuery(httpProviderQuery(t))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer provider-token" || request.Method != http.MethodPost {
			t.Errorf("request headers = %#v", request.Header)
		}
		var payload struct {
			Provider      string   `json:"provider"`
			Model         string   `json:"model"`
			ModelRevision string   `json:"model_revision"`
			Inputs        []string `json:"inputs"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil || payload.Provider != manifest.Provider || payload.Model != manifest.Model || payload.ModelRevision != manifest.ModelRevision || len(payload.Inputs) != 1 {
			t.Errorf("payload = %#v error=%v", payload, err)
		}
		_ = json.NewEncoder(response).Encode(embedding.ProviderResponse{Provider: manifest.Provider, Model: manifest.Model, ModelRevision: manifest.ModelRevision, Vectors: [][]float64{{1, 0, 0}}})
	}))
	defer server.Close()
	provider, err := embedding.NewHTTPProvider(server.URL, "provider-token", true)
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Embed(context.Background(), embedding.ProviderRequest{Manifest: manifest, Inputs: []embedding.Input{input}})
	if err != nil || len(result.Vectors) != 1 || len(result.Vectors[0]) != 3 {
		t.Fatalf("result=%#v error=%v", result, err)
	}
}

func TestHTTPProviderRejectsUnsafeEndpointRedirectAndIdentityMismatch(t *testing.T) {
	if _, err := embedding.NewHTTPProvider("http://provider.example.test/embed", "token", false); err == nil {
		t.Fatal("production provider accepted plaintext HTTP")
	}
	manifest := httpProviderManifest(t)
	input, _ := embedding.InputFromQuery(httpProviderQuery(t))
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Location", "https://other.example.test")
		response.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	provider, err := embedding.NewHTTPProvider(server.URL, "provider-token", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = provider.Embed(context.Background(), embedding.ProviderRequest{Manifest: manifest, Inputs: []embedding.Input{input}}); err == nil {
		t.Fatal("redirect was followed or accepted")
	}
}

func httpProviderManifest(t *testing.T) embedding.Manifest {
	t.Helper()
	manifest, err := embedding.NewManifest(embedding.ManifestSpec{Provider: "gateway", Model: "embed-v1", ModelRevision: "2026-09-01", Dimension: 3, Distance: embedding.DistanceCosineNormalized, Normalization: embedding.NormalizationL2, QuantizationScale: 1_000_000})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func httpProviderQuery(t *testing.T) retrieval.Query {
	t.Helper()
	query, err := retrieval.BuildQuery("tenant_a", "policy.v1", retrieval.AliasSet{}, retrieval.Input{Task: "write output", Tools: []retrieval.Tool{{Name: "writer", ContractVersionID: "tcv_writer"}}, Harness: retrieval.Harness{Name: "test", Version: "1"}, RiskClass: retrieval.RiskLow, LatencyClass: retrieval.LatencyInteractive, MaxCandidates: 10})
	if err != nil {
		t.Fatal(err)
	}
	return query
}
