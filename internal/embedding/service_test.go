package embedding

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/sauhard74/mem-jev/internal/retrieval"
)

func TestManifestIsCanonicalAndImmutable(t *testing.T) {
	spec := ManifestSpec{Provider: " pinned-provider ", Model: "embed-large", ModelRevision: "sha256:abc", Dimension: 3, Distance: DistanceCosineNormalized, Normalization: NormalizationL2, QuantizationScale: 1_000_000}
	first, err := NewManifest(spec)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewManifest(spec)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || !reflect.DeepEqual(first.CanonicalJSON, second.CanonicalJSON) || first.Provider != "pinned-provider" {
		t.Fatalf("manifest is not canonical: %#v %#v", first, second)
	}
	changed := spec
	changed.ModelRevision = "sha256:def"
	third, err := NewManifest(changed)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == third.ID {
		t.Fatal("model revision must change manifest identity")
	}
}

func TestManifestRejectsAliasesAndUnsafeDimensions(t *testing.T) {
	invalid := []ManifestSpec{
		{Provider: "p", Model: "latest", ModelRevision: "r", Dimension: 3, Distance: DistanceCosineNormalized, Normalization: NormalizationL2, QuantizationScale: 1_000_000},
		{Provider: "p", Model: "m", ModelRevision: "latest", Dimension: 3, Distance: DistanceCosineNormalized, Normalization: NormalizationL2, QuantizationScale: 1_000_000},
		{Provider: "p", Model: "embed:latest", ModelRevision: "r", Dimension: 3, Distance: DistanceCosineNormalized, Normalization: NormalizationL2, QuantizationScale: 1_000_000},
		{Provider: "p", Model: "m", ModelRevision: "r", Dimension: 0, Distance: DistanceCosineNormalized, Normalization: NormalizationL2, QuantizationScale: 1_000_000},
		{Provider: "p", Model: "m", ModelRevision: "r", Dimension: 3, Distance: DistanceCosine, Normalization: NormalizationNone, QuantizationScale: 1_000_000},
	}
	for _, spec := range invalid {
		if _, err := NewManifest(spec); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("expected invalid manifest for %#v, got %v", spec, err)
		}
	}
}

func TestSemanticInputAlignsQueryAndDocument(t *testing.T) {
	query, err := retrieval.BuildQuery("tenant_a", "policy.v1", retrieval.AliasSet{}, retrieval.Input{
		Task: " deploy   release ", Tools: []retrieval.Tool{{Name: "shell", ContractVersionID: "tcv_shell"}},
		Harness: retrieval.Harness{Name: "ci", Version: "1"}, Resources: []retrieval.Resource{{Type: "repo", Namespace: "main", Identity: "repository"}},
		RiskClass: retrieval.RiskMedium, LatencyClass: retrieval.LatencyInteractive, MaxCandidates: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	document := retrieval.Document{TaskText: query.Task, Tools: []retrieval.ToolRequirement{{Name: "shell", ContractVersionID: "tcv_shell"}}, Harness: query.Harness, Resources: []retrieval.ResourceRequirement{{Type: "repo", Namespace: "main", IdentityHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}
	fromQuery, err := InputFromQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	fromDocument, err := InputFromDocument(document)
	if err != nil {
		t.Fatal(err)
	}
	if fromQuery.Hash != fromDocument.Hash || string(fromQuery.CanonicalJSON) != string(fromDocument.CanonicalJSON) {
		t.Fatalf("query/document semantic input diverged:\n%s\n%s", fromQuery.CanonicalJSON, fromDocument.CanonicalJSON)
	}
}

func TestNormalizeQuantizeAndExactScore(t *testing.T) {
	manifest := mustManifest(t, 2)
	left, err := NormalizeAndQuantize(manifest, []float64{3, 4})
	if err != nil {
		t.Fatal(err)
	}
	right, err := NormalizeAndQuantize(manifest, []float64{6, 8})
	if err != nil {
		t.Fatal(err)
	}
	orthogonal, err := NormalizeAndQuantize(manifest, []float64{-4, 3})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(left.Values, []int32{600000, 800000}) || !reflect.DeepEqual(left.Values, right.Values) {
		t.Fatalf("unexpected quantization: %#v %#v", left, right)
	}
	same, err := ExactScore(manifest, left, right)
	if err != nil {
		t.Fatal(err)
	}
	other, err := ExactScore(manifest, left, orthogonal)
	if err != nil {
		t.Fatal(err)
	}
	if same <= other || other != 0 {
		t.Fatalf("unexpected exact scores: same=%d other=%d", same, other)
	}
	for _, vector := range [][]float64{{1}, {0, 0}, {math.NaN(), 1}, {math.Inf(1), 1}} {
		if _, err := NormalizeAndQuantize(manifest, vector); !errors.Is(err, ErrInvalidVector) {
			t.Fatalf("expected invalid vector for %#v, got %v", vector, err)
		}
	}
}

func TestServiceDeduplicatesAndVerifiesProviderOutput(t *testing.T) {
	manifest := mustManifest(t, 2)
	provider := &fakeProvider{revision: manifest.ModelRevision, vectors: [][]float64{{3, 4}}}
	store := newFakeStore()
	service, err := NewService(provider, store)
	if err != nil {
		t.Fatal(err)
	}
	input, err := buildInput("release", retrieval.Harness{Name: "ci", Version: "1"}, []semanticTool{{Name: "shell", ContractVersionID: "tcv_shell"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Generate(context.Background(), "tenant_a", manifest, []Input{input, input})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Generate(context.Background(), "tenant_a", manifest, []Input{input})
	if err != nil {
		t.Fatal(err)
	}
	if provider.calls != 1 || len(first) != 2 || first[0].ID != first[1].ID || first[0].ID != second[0].ID {
		t.Fatalf("generation was not idempotent: calls=%d first=%#v second=%#v", provider.calls, first, second)
	}

	badProvider := &fakeProvider{revision: "wrong", vectors: [][]float64{{3, 4}}}
	badService, _ := NewService(badProvider, newFakeStore())
	if _, err := badService.Generate(context.Background(), "tenant_a", manifest, []Input{input}); !errors.Is(err, ErrProviderMismatch) {
		t.Fatalf("expected provider mismatch, got %v", err)
	}
}

func TestServiceRejectsNonCanonicalProviderInput(t *testing.T) {
	manifest := mustManifest(t, 2)
	input, err := buildInput("release", retrieval.Harness{Name: "ci", Version: "1"}, []semanticTool{{Name: "shell", ContractVersionID: "tcv_shell"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	input.CanonicalJSON = append([]byte(" \n"), input.CanonicalJSON...)
	service, err := NewService(&fakeProvider{revision: manifest.ModelRevision, vectors: [][]float64{{1, 0}}}, newFakeStore())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Generate(context.Background(), "tenant_a", manifest, []Input{input}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
}

func mustManifest(t *testing.T, dimension uint32) Manifest {
	t.Helper()
	manifest, err := NewManifest(ManifestSpec{Provider: "provider", Model: "model-v1", ModelRevision: "revision-123", Dimension: dimension, Distance: DistanceCosineNormalized, Normalization: NormalizationL2, QuantizationScale: 1_000_000})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

type fakeProvider struct {
	revision string
	vectors  [][]float64
	calls    int
}

func (p *fakeProvider) Embed(_ context.Context, request ProviderRequest) (ProviderResponse, error) {
	p.calls++
	return ProviderResponse{Provider: request.Manifest.Provider, Model: request.Manifest.Model, ModelRevision: p.revision, Vectors: p.vectors}, nil
}

type fakeStore struct{ values map[string]Embedding }

func newFakeStore() *fakeStore { return &fakeStore{values: make(map[string]Embedding)} }
func (s *fakeStore) Get(_ context.Context, tenantID, manifestID, inputHash string) (Embedding, bool, error) {
	value, found := s.values[tenantID+"\x00"+manifestID+"\x00"+inputHash]
	return value, found, nil
}
func (s *fakeStore) Put(_ context.Context, value Embedding) (Embedding, error) {
	key := value.TenantID + "\x00" + value.ManifestID + "\x00" + value.InputHash
	if prior, found := s.values[key]; found {
		return prior, nil
	}
	s.values[key] = value
	return value, nil
}
