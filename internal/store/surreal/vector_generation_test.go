package surreal

import (
	"testing"

	"github.com/sauhard74/mem-jev/internal/embedding"
)

func TestBuildVectorGenerationDerivesSharedPhysicalIndex(t *testing.T) {
	manifest, err := embedding.NewManifest(embedding.ManifestSpec{
		Provider: "gateway", Model: "embed-v1", ModelRevision: "2026-09-01", Dimension: 3,
		Distance: embedding.DistanceCosineNormalized, Normalization: embedding.NormalizationL2, QuantizationScale: 1_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	tuning := VectorTuning{Degree: 32, BuildSearch: 64, AlphaMilli: 1200, Overfetch: 4, SearchEffort: 40}
	first, err := BuildVectorGeneration("tenant_a", manifest, tuning)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildVectorGeneration("tenant_b", manifest, tuning)
	if err != nil {
		t.Fatal(err)
	}
	if first.IndexManifestID != second.IndexManifestID || first.TableName != second.TableName || first.IndexName != second.IndexName {
		t.Fatalf("physical generation differs by tenant: %#v %#v", first, second)
	}
	if first.ContentHash == second.ContentHash {
		t.Fatal("tenant-scoped generation records must have distinct content hashes")
	}
}
