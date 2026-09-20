package jev

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
)

const (
	ProviderTypeSafe = "typesafe"
	rubricSchemaV1   = "jev-rubric.v1"
)

var (
	ErrInvalidRubric = errors.New("invalid Jev rubric")
	pinnedModel      = regexp.MustCompile(`^jev-[0-9]+\.[0-9]+\.[0-9]+$`)
)

type QuestionType string

const (
	QuestionScore QuestionType = "score"
	QuestionNoul  QuestionType = "noul"
)

type Question struct {
	ID           string       `json:"id"`
	Type         QuestionType `json:"type"`
	Instructions string       `json:"instructions"`
	Criteria     []string     `json:"criteria"`
}

type RubricManifestSpec struct {
	Version           string     `json:"version"`
	Provider          string     `json:"provider"`
	Model             string     `json:"model"`
	AdmissionVersion  string     `json:"admission_version"`
	QuantizationScale int32      `json:"quantization_scale"`
	Questions         []Question `json:"questions"`
}

type RubricManifest struct {
	SchemaVersion string `json:"schema_version"`
	RubricManifestSpec
	ID            string `json:"-"`
	ContentHash   string `json:"-"`
	CanonicalJSON []byte `json:"-"`
}

func DefaultRubricV1(model string) (RubricManifest, error) {
	return NewRubricManifest(RubricManifestSpec{
		Version: "jev.semantic-fit.v1", Provider: ProviderTypeSafe, Model: strings.TrimSpace(model),
		AdmissionVersion: "jev.ambiguity-band.v1", QuantizationScale: ProbabilityScale,
		Questions: []Question{
			{ID: "intent_fit", Type: QuestionScore, Instructions: "How closely does the procedure's intended outcome match the requested task?", Criteria: []string{"Unrelated", "Weakly related", "Partially aligned", "Strongly aligned", "Exact intent match"}},
			{ID: "preconditions_likely_satisfied", Type: QuestionNoul, Instructions: "Given only the supplied facts, are the procedure's semantic preconditions likely satisfied?", Criteria: []string{"No", "Yes"}},
			{ID: "task_coverage", Type: QuestionScore, Instructions: "How much of the requested task would this procedure complete?", Criteria: []string{"None", "Small fragment", "Material portion", "Most", "All"}},
			{ID: "contradicts_request", Type: QuestionNoul, Instructions: "Would executing this procedure contradict the request or a stated constraint?", Criteria: []string{"No", "Yes"}},
			{ID: "useful_as_partial_plan", Type: QuestionNoul, Instructions: "Would this procedure be useful as a safe partial plan when it does not complete the whole task?", Criteria: []string{"No", "Yes"}},
		},
	})
}

func NewRubricManifest(source RubricManifestSpec) (RubricManifest, error) {
	spec := source
	spec.Version = strings.TrimSpace(spec.Version)
	spec.Provider = strings.TrimSpace(spec.Provider)
	spec.Model = strings.TrimSpace(spec.Model)
	spec.AdmissionVersion = strings.TrimSpace(spec.AdmissionVersion)
	spec.Questions = append([]Question(nil), source.Questions...)
	if spec.Version == "" || spec.Provider != ProviderTypeSafe || !pinnedModel.MatchString(spec.Model) || spec.AdmissionVersion == "" || spec.QuantizationScale != ProbabilityScale || len(spec.Questions) == 0 || len(spec.Questions) > 32 {
		return RubricManifest{}, ErrInvalidRubric
	}
	for index := range spec.Questions {
		question := &spec.Questions[index]
		question.ID = strings.TrimSpace(question.ID)
		question.Instructions = strings.TrimSpace(question.Instructions)
		question.Criteria = append([]string(nil), question.Criteria...)
		for criterionIndex := range question.Criteria {
			question.Criteria[criterionIndex] = strings.TrimSpace(question.Criteria[criterionIndex])
		}
		if question.ID == "" || question.Instructions == "" || len(question.Instructions) > 2_048 || (question.Type != QuestionScore && question.Type != QuestionNoul) || question.Type == QuestionScore && (len(question.Criteria) < 2 || len(question.Criteria) > 10) || question.Type == QuestionNoul && len(question.Criteria) != 2 {
			return RubricManifest{}, fmt.Errorf("%w: question %d", ErrInvalidRubric, index)
		}
		for _, criterion := range question.Criteria {
			if criterion == "" || len(criterion) > 1_024 {
				return RubricManifest{}, fmt.Errorf("%w: question %d criterion", ErrInvalidRubric, index)
			}
		}
	}
	sort.Slice(spec.Questions, func(i, j int) bool { return spec.Questions[i].ID < spec.Questions[j].ID })
	for index := 1; index < len(spec.Questions); index++ {
		if spec.Questions[index-1].ID == spec.Questions[index].ID {
			return RubricManifest{}, ErrInvalidRubric
		}
	}
	manifest := RubricManifest{SchemaVersion: rubricSchemaV1, RubricManifestSpec: spec}
	encoded, hash, err := canonical.MarshalAndHash(manifest)
	if err != nil {
		return RubricManifest{}, fmt.Errorf("canonicalize Jev rubric: %w", err)
	}
	manifest.ID, manifest.ContentHash, manifest.CanonicalJSON = "jevr_"+hash, hash, encoded
	return manifest, nil
}

func ValidateRubricManifest(manifest RubricManifest) error {
	rebuilt, err := NewRubricManifest(manifest.RubricManifestSpec)
	if err != nil || manifest.SchemaVersion != rubricSchemaV1 || manifest.ID != rebuilt.ID || manifest.ContentHash != rebuilt.ContentHash || !bytes.Equal(manifest.CanonicalJSON, rebuilt.CanonicalJSON) {
		return ErrInvalidRubric
	}
	return nil
}
