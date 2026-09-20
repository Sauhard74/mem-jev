package embedding

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
)

var (
	ErrInvalidVector    = errors.New("invalid embedding vector")
	ErrProviderMismatch = errors.New("embedding provider response does not match manifest")
	ErrStoreConflict    = errors.New("embedding store conflict")
)

type Vector struct {
	Values []int32 `json:"values"`
}

type Embedding struct {
	SchemaVersion string `json:"schema_version"`
	TenantID      string `json:"tenant_id"`
	ManifestID    string `json:"manifest_id"`
	InputHash     string `json:"input_hash"`
	Vector        Vector `json:"vector"`
	ID            string `json:"-"`
	ContentHash   string `json:"-"`
	CanonicalJSON []byte `json:"-"`
}

func NormalizeAndQuantize(manifest Manifest, source []float64) (Vector, error) {
	if err := ValidateManifest(manifest); err != nil || len(source) != int(manifest.Dimension) {
		return Vector{}, ErrInvalidVector
	}
	var squared float64
	for _, value := range source {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return Vector{}, ErrInvalidVector
		}
		squared += value * value
	}
	if squared == 0 || math.IsNaN(squared) || math.IsInf(squared, 0) {
		return Vector{}, ErrInvalidVector
	}
	norm := math.Sqrt(squared)
	result := Vector{Values: make([]int32, len(source))}
	for index, value := range source {
		quantized := math.Round((value / norm) * float64(manifest.QuantizationScale))
		if quantized > math.MaxInt32 || quantized < math.MinInt32 {
			return Vector{}, ErrInvalidVector
		}
		result.Values[index] = int32(quantized)
	}
	return result, nil
}

func ExactScore(manifest Manifest, query, candidate Vector) (int64, error) {
	if err := ValidateManifest(manifest); err != nil || len(query.Values) != int(manifest.Dimension) || len(candidate.Values) != int(manifest.Dimension) {
		return 0, ErrInvalidVector
	}
	var score int64
	for index := range query.Values {
		left, right := int64(query.Values[index]), int64(candidate.Values[index])
		var term int64
		if manifest.Distance == DistanceEuclidean {
			delta := left - right
			term = -(delta * delta)
		} else {
			term = left * right
		}
		if (term > 0 && score > math.MaxInt64-term) || (term < 0 && score < math.MinInt64-term) {
			return 0, ErrInvalidVector
		}
		score += term
	}
	return score, nil
}

func (v Vector) StorageValues() []float64 {
	result := make([]float64, len(v.Values))
	for index, value := range v.Values {
		result[index] = float64(value)
	}
	return result
}

type Service struct {
	provider Provider
	store    Store
}

func NewService(provider Provider, store Store) (*Service, error) {
	if provider == nil || store == nil {
		return nil, errors.New("embedding service is not configured")
	}
	return &Service{provider: provider, store: store}, nil
}

func (s *Service) Generate(ctx context.Context, tenantID string, manifest Manifest, inputs []Input) ([]Embedding, error) {
	if s == nil || s.provider == nil || s.store == nil || strings.TrimSpace(tenantID) == "" || len(inputs) == 0 || len(inputs) > 256 || ValidateManifest(manifest) != nil {
		return nil, ErrInvalidInput
	}
	unique := make(map[string]Input, len(inputs))
	for _, input := range inputs {
		if err := validateInput(input); err != nil {
			return nil, err
		}
		unique[input.Hash] = input
	}
	hashes := make([]string, 0, len(unique))
	for hash := range unique {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	values := make(map[string]Embedding, len(unique))
	missing := make([]Input, 0, len(unique))
	for _, hash := range hashes {
		stored, found, err := s.store.Get(ctx, tenantID, manifest.ID, hash)
		if err != nil {
			return nil, fmt.Errorf("read embedding: %w", err)
		}
		if found {
			if err := validateEmbedding(stored, tenantID, manifest, hash); err != nil {
				return nil, err
			}
			values[hash] = stored
		} else {
			missing = append(missing, unique[hash])
		}
	}
	if len(missing) > 0 {
		response, err := s.provider.Embed(ctx, ProviderRequest{Manifest: manifest, Inputs: missing})
		if err != nil {
			return nil, fmt.Errorf("generate embedding: %w", err)
		}
		if response.Provider != manifest.Provider || response.Model != manifest.Model || response.ModelRevision != manifest.ModelRevision || len(response.Vectors) != len(missing) {
			return nil, ErrProviderMismatch
		}
		for index, input := range missing {
			vector, vectorErr := NormalizeAndQuantize(manifest, response.Vectors[index])
			if vectorErr != nil {
				return nil, vectorErr
			}
			value, valueErr := newEmbedding(tenantID, manifest, input.Hash, vector)
			if valueErr != nil {
				return nil, valueErr
			}
			stored, storeErr := s.store.Put(ctx, value)
			if storeErr != nil {
				return nil, fmt.Errorf("store embedding: %w", storeErr)
			}
			if validateEmbedding(stored, tenantID, manifest, input.Hash) != nil || stored.ID != value.ID {
				return nil, ErrStoreConflict
			}
			values[input.Hash] = stored
		}
	}
	result := make([]Embedding, len(inputs))
	for index, input := range inputs {
		result[index] = values[input.Hash]
	}
	return result, nil
}

func newEmbedding(tenantID string, manifest Manifest, inputHash string, vector Vector) (Embedding, error) {
	value := Embedding{SchemaVersion: "embedding.v1", TenantID: tenantID, ManifestID: manifest.ID, InputHash: inputHash, Vector: vector}
	canonicalJSON, hash, err := canonical.MarshalAndHash(value)
	if err != nil {
		return Embedding{}, err
	}
	value.ID, value.ContentHash, value.CanonicalJSON = "emb_"+hash, hash, canonicalJSON
	return value, nil
}

func validateEmbedding(value Embedding, tenantID string, manifest Manifest, inputHash string) error {
	if value.TenantID != tenantID || value.ManifestID != manifest.ID || value.InputHash != inputHash || len(value.Vector.Values) != int(manifest.Dimension) {
		return ErrStoreConflict
	}
	if err := ValidateContent(value); err != nil {
		return ErrStoreConflict
	}
	return nil
}

func ValidateEmbedding(value Embedding, tenantID string, manifest Manifest, inputHash string) error {
	return validateEmbedding(value, tenantID, manifest, inputHash)
}

func ValidateContent(value Embedding) error {
	if value.SchemaVersion != "embedding.v1" || strings.TrimSpace(value.TenantID) == "" || strings.TrimSpace(value.ManifestID) == "" || !sha256Pattern.MatchString(value.InputHash) || len(value.Vector.Values) == 0 {
		return ErrStoreConflict
	}
	copyOfValue := value
	copyOfValue.ID, copyOfValue.ContentHash, copyOfValue.CanonicalJSON = "", "", nil
	canonicalJSON, hash, err := canonical.MarshalAndHash(copyOfValue)
	if err != nil || value.ID != "emb_"+hash || value.ContentHash != hash || string(value.CanonicalJSON) != string(canonicalJSON) {
		return ErrStoreConflict
	}
	return nil
}
