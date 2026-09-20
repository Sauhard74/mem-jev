package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadVectorRuntimeConfigDerivesImmutableManifestAndIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vector.json")
	writeTestFile(t, path, `{
  "schema_version":"retrieval-vector-config.v1",
  "embedding":{"provider":"gateway","model":"embed-v1","model_revision":"2026-09-01","dimension":3,"distance":"cosine_normalized","normalization":"l2","quantization_scale":1000000},
  "tuning":{"degree":32,"build_search":64,"alpha_milli":1200,"overfetch":4,"search_effort":40}
}`)
	manifest, generation, err := loadVectorRuntimeConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.ID == "" || generation.EmbeddingID != manifest.ID || generation.IndexManifestID == "" || generation.TableName == "" {
		t.Fatalf("manifest=%#v generation=%#v", manifest, generation)
	}
}

func TestLoadVectorRuntimeConfigRejectsUnknownAndUnpinnedValues(t *testing.T) {
	for name, body := range map[string]string{
		"unknown": `{"schema_version":"retrieval-vector-config.v1","unknown":true}`,
		"alias":   `{"schema_version":"retrieval-vector-config.v1","embedding":{"provider":"gateway","model":"latest","model_revision":"latest","dimension":3,"distance":"cosine_normalized","normalization":"l2","quantization_scale":1000000},"tuning":{"degree":32,"build_search":64,"alpha_milli":1200,"overfetch":4,"search_effort":40}}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "vector.json")
			writeTestFile(t, path, body)
			if _, _, err := loadVectorRuntimeConfig(path); err == nil {
				t.Fatal("invalid vector configuration accepted")
			}
		})
	}
}

func TestLoadProviderTokenRejectsMultilineSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	writeTestFile(t, path, "first\nsecond")
	if _, err := loadProviderToken(path); err == nil {
		t.Fatal("multiline token accepted")
	}
}

func writeTestFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}
