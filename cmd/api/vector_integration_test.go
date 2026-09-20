//go:build integration

package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/config"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/testinfra"
)

func TestHostedRetrievalBuildsProductionVectorChannelAsOptional(t *testing.T) {
	db := testinfra.StartSurreal(t, "surrealdb/surrealdb:v3.2.4")
	provider := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer provider.Close()
	directory := t.TempDir()
	vectorPath := filepath.Join(directory, "vector.json")
	tokenPath := filepath.Join(directory, "provider-token")
	writeTestFile(t, vectorPath, `{
  "schema_version":"retrieval-vector-config.v1",
  "embedding":{"provider":"gateway","model":"embed-v1","model_revision":"2026-09-01","dimension":3,"distance":"cosine_normalized","normalization":"l2","quantization_scale":1000000},
  "tuning":{"degree":32,"build_search":64,"alpha_milli":1200,"overfetch":4,"search_effort":40}
}`)
	writeTestFile(t, tokenPath, "opaque-provider-token")
	configuration := config.RetrievalConfig{
		ChannelTimeout: 150 * time.Millisecond, Retention: time.Hour, MaximumSelections: 5,
		ExactManifestID: "idx_exact.v1", LexicalManifestID: "idx_lexical.v1", FacetManifestID: "idx_facet.v1", GraphManifestID: "idx_graph.v1",
		VectorConfigFile: vectorPath, EmbeddingProviderEndpoint: provider.URL, EmbeddingProviderTokenFile: tokenPath,
	}
	channels, required, err := buildRetrievalChannels(db, configuration, "development")
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 5 || channels[4].Name() != retrieval.ChannelVector || !channels[4].Approximate() {
		t.Fatalf("channels = %#v", channels)
	}
	if len(required) != 4 {
		t.Fatalf("required channels = %#v", required)
	}
	for _, channel := range required {
		if channel == retrieval.ChannelVector {
			t.Fatal("vector channel must remain optional")
		}
	}
	if err := os.Remove(tokenPath); err != nil {
		t.Fatal(err)
	}
	if _, _, err := buildRetrievalChannels(db, configuration, "development"); err == nil {
		t.Fatal("missing provider token did not fail startup")
	}
}

