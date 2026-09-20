package embedding

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/retrieval"
)

const (
	manifestSchemaVersion = "embedding-manifest.v1"
	inputSchemaVersion    = "embedding-input.v1"
	maxDimension          = 65536
)

var (
	ErrInvalidManifest = errors.New("invalid embedding manifest")
	ErrInvalidInput    = errors.New("invalid embedding input")
	sha256Pattern      = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type Distance string

const (
	DistanceCosine           Distance = "cosine"
	DistanceCosineNormalized Distance = "cosine_normalized"
	DistanceEuclidean        Distance = "euclidean"
)

type Normalization string

const (
	NormalizationNone Normalization = "none"
	NormalizationL2   Normalization = "l2"
)

type ManifestSpec struct {
	Provider          string        `json:"provider"`
	Model             string        `json:"model"`
	ModelRevision     string        `json:"model_revision"`
	Dimension         uint32        `json:"dimension"`
	Distance          Distance      `json:"distance"`
	Normalization     Normalization `json:"normalization"`
	QuantizationScale int32         `json:"quantization_scale"`
}

type Manifest struct {
	SchemaVersion string `json:"schema_version"`
	ManifestSpec
	ID            string `json:"-"`
	ContentHash   string `json:"-"`
	CanonicalJSON []byte `json:"-"`
}

func NewManifest(source ManifestSpec) (Manifest, error) {
	spec := source
	spec.Provider = strings.TrimSpace(spec.Provider)
	spec.Model = strings.TrimSpace(spec.Model)
	spec.ModelRevision = strings.TrimSpace(spec.ModelRevision)
	if spec.Provider == "" || spec.Model == "" || spec.ModelRevision == "" || isAlias(spec.Model) || isAlias(spec.ModelRevision) ||
		spec.Dimension == 0 || spec.Dimension > maxDimension || spec.QuantizationScale < 1 || spec.QuantizationScale > 1_000_000 ||
		spec.Normalization != NormalizationL2 || (spec.Distance != DistanceCosineNormalized && spec.Distance != DistanceEuclidean) {
		return Manifest{}, fmt.Errorf("%w: require pinned model, bounded dimension, l2 normalization, and deterministic distance", ErrInvalidManifest)
	}
	manifest := Manifest{SchemaVersion: manifestSchemaVersion, ManifestSpec: spec}
	canonicalJSON, hash, err := canonical.MarshalAndHash(manifest)
	if err != nil {
		return Manifest{}, fmt.Errorf("canonicalize embedding manifest: %w", err)
	}
	manifest.ID, manifest.ContentHash, manifest.CanonicalJSON = "embm_"+hash, hash, canonicalJSON
	return manifest, nil
}

func ValidateManifest(manifest Manifest) error {
	rebuilt, err := NewManifest(manifest.ManifestSpec)
	if err != nil || manifest.SchemaVersion != manifestSchemaVersion || manifest.ID != rebuilt.ID || manifest.ContentHash != rebuilt.ContentHash || string(manifest.CanonicalJSON) != string(rebuilt.CanonicalJSON) {
		return ErrInvalidManifest
	}
	return nil
}

func isAlias(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "latest", "default", "production", "stable", "current":
		return true
	}
	for _, suffix := range []string{":latest", "/latest", "@latest", ":current", "/current", "@current"} {
		if strings.HasSuffix(normalized, suffix) {
			return true
		}
	}
	return false
}

type semanticTool struct {
	Name              string `json:"name"`
	ContractVersionID string `json:"contract_version_id"`
}

type semanticResource struct {
	Type      string `json:"type"`
	Namespace string `json:"namespace,omitempty"`
}

type semanticInput struct {
	SchemaVersion string             `json:"schema_version"`
	Task          string             `json:"task"`
	Tools         []semanticTool     `json:"tools"`
	Harness       retrieval.Harness  `json:"harness"`
	Resources     []semanticResource `json:"resources,omitempty"`
}

type Input struct {
	Hash          string
	CanonicalJSON []byte
}

func InputFromQuery(query retrieval.Query) (Input, error) {
	tools := make([]semanticTool, len(query.Tools))
	for index, item := range query.Tools {
		tools[index] = semanticTool{Name: item.Name, ContractVersionID: item.ContractVersionID}
	}
	resources := make([]semanticResource, len(query.Resources))
	for index, item := range query.Resources {
		resources[index] = semanticResource{Type: item.Type, Namespace: item.Namespace}
	}
	return buildInput(query.Task, query.Harness, tools, resources)
}

func InputFromDocument(document retrieval.Document) (Input, error) {
	tools := make([]semanticTool, len(document.Tools))
	for index, item := range document.Tools {
		tools[index] = semanticTool{Name: item.Name, ContractVersionID: item.ContractVersionID}
	}
	resources := make([]semanticResource, len(document.Resources))
	for index, item := range document.Resources {
		resources[index] = semanticResource{Type: item.Type, Namespace: item.Namespace}
	}
	return buildInput(document.TaskText, document.Harness, tools, resources)
}

func buildInput(task string, harness retrieval.Harness, tools []semanticTool, resources []semanticResource) (Input, error) {
	value := semanticInput{SchemaVersion: inputSchemaVersion, Task: strings.TrimSpace(task), Harness: retrieval.Harness{Name: strings.TrimSpace(harness.Name), Version: strings.TrimSpace(harness.Version)}, Tools: append([]semanticTool(nil), tools...), Resources: append([]semanticResource(nil), resources...)}
	for index := range value.Tools {
		value.Tools[index].Name = strings.TrimSpace(value.Tools[index].Name)
		value.Tools[index].ContractVersionID = strings.TrimSpace(value.Tools[index].ContractVersionID)
		if value.Tools[index].Name == "" || value.Tools[index].ContractVersionID == "" {
			return Input{}, ErrInvalidInput
		}
	}
	for index := range value.Resources {
		value.Resources[index].Type = strings.TrimSpace(value.Resources[index].Type)
		value.Resources[index].Namespace = strings.TrimSpace(value.Resources[index].Namespace)
		if value.Resources[index].Type == "" {
			return Input{}, ErrInvalidInput
		}
	}
	if value.Task == "" || value.Harness.Name == "" || len(value.Tools) == 0 {
		return Input{}, ErrInvalidInput
	}
	sort.Slice(value.Tools, func(i, j int) bool {
		if value.Tools[i].Name != value.Tools[j].Name {
			return value.Tools[i].Name < value.Tools[j].Name
		}
		return value.Tools[i].ContractVersionID < value.Tools[j].ContractVersionID
	})
	sort.Slice(value.Resources, func(i, j int) bool {
		if value.Resources[i].Type != value.Resources[j].Type {
			return value.Resources[i].Type < value.Resources[j].Type
		}
		return value.Resources[i].Namespace < value.Resources[j].Namespace
	})
	canonicalJSON, hash, err := canonical.MarshalAndHash(value)
	if err != nil {
		return Input{}, err
	}
	return Input{Hash: hash, CanonicalJSON: canonicalJSON}, nil
}

func validateInput(input Input) error {
	if !sha256Pattern.MatchString(input.Hash) || len(input.CanonicalJSON) == 0 {
		return ErrInvalidInput
	}
	canonicalJSON, hash, err := canonical.MarshalAndHashRaw(input.CanonicalJSON)
	if err != nil || hash != input.Hash || !bytes.Equal(canonicalJSON, input.CanonicalJSON) {
		return ErrInvalidInput
	}
	return nil
}
