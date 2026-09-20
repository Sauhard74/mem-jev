package jev

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

var (
	ErrInvalidJudgment = errors.New("invalid Jev judgment")
	sha256Pattern      = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

const (
	FeatureIntentFit        = "jev_intent_fit_micros"
	FeatureNonContradiction = "jev_non_contradiction_micros"
	FeaturePartialPlan      = "jev_partial_plan_micros"
	FeaturePreconditions    = "jev_preconditions_micros"
	FeatureTaskCoverage     = "jev_task_coverage_micros"
)

type JudgmentKeyInput struct {
	TenantID           domain.TenantID `json:"tenant_id"`
	QueryHash          string          `json:"query_hash"`
	ProcedureVersionID string          `json:"procedure_version_id"`
	DocumentHash       string          `json:"document_hash"`
	EnvironmentHash    string          `json:"environment_hash"`
	PolicyManifestID   string          `json:"policy_manifest_id"`
	RubricManifestID   string          `json:"rubric_manifest_id"`
	Provider           string          `json:"provider"`
	Model              string          `json:"model"`
	PredecessorHash    string          `json:"predecessor_hash,omitempty"`
}

func NewJudgmentKey(source JudgmentKeyInput) (string, error) {
	input := source
	input.ProcedureVersionID = strings.TrimSpace(input.ProcedureVersionID)
	input.PolicyManifestID = strings.TrimSpace(input.PolicyManifestID)
	input.RubricManifestID = strings.TrimSpace(input.RubricManifestID)
	input.Provider = strings.TrimSpace(input.Provider)
	input.Model = strings.TrimSpace(input.Model)
	if input.TenantID == "" || !sha256Pattern.MatchString(input.QueryHash) || input.ProcedureVersionID == "" || !sha256Pattern.MatchString(input.DocumentHash) || !sha256Pattern.MatchString(input.EnvironmentHash) || input.PolicyManifestID == "" || input.RubricManifestID == "" || input.Provider != ProviderTypeSafe || !pinnedModel.MatchString(input.Model) || input.PredecessorHash != "" && !sha256Pattern.MatchString(input.PredecessorHash) {
		return "", ErrInvalidJudgment
	}
	_, hash, err := canonical.MarshalAndHash(input)
	if err != nil {
		return "", fmt.Errorf("canonicalize Jev judgment key: %w", err)
	}
	return "jevj_" + hash, nil
}

type ScoreAnswer struct {
	ScoreMicros         int32   `json:"score_micros"`
	ConfidenceMicros    int32   `json:"confidence_micros"`
	ProbabilitiesMicros []int32 `json:"probabilities_micros"`
}

type NoulAnswer struct {
	NoulMicros int32 `json:"noul_micros"`
}

type Judgment struct {
	IntentFit                    ScoreAnswer `json:"intent_fit"`
	PreconditionsLikelySatisfied NoulAnswer  `json:"preconditions_likely_satisfied"`
	TaskCoverage                 ScoreAnswer `json:"task_coverage"`
	ContradictsRequest           NoulAnswer  `json:"contradicts_request"`
	UsefulAsPartialPlan          NoulAnswer  `json:"useful_as_partial_plan"`
}

type Feature struct {
	Name  string `json:"name"`
	Value int32  `json:"value"`
}

func (judgment Judgment) Features() ([]Feature, error) {
	if !validScore(judgment.IntentFit, 5) || !validScore(judgment.TaskCoverage, 5) || !validNoul(judgment.PreconditionsLikelySatisfied) || !validNoul(judgment.ContradictsRequest) || !validNoul(judgment.UsefulAsPartialPlan) {
		return nil, ErrInvalidJudgment
	}
	features := []Feature{
		{Name: FeatureIntentFit, Value: judgment.IntentFit.ScoreMicros / 4},
		{Name: FeaturePreconditions, Value: judgment.PreconditionsLikelySatisfied.NoulMicros},
		{Name: FeatureTaskCoverage, Value: judgment.TaskCoverage.ScoreMicros / 4},
		{Name: FeatureNonContradiction, Value: ProbabilityScale - judgment.ContradictsRequest.NoulMicros},
		{Name: FeaturePartialPlan, Value: judgment.UsefulAsPartialPlan.NoulMicros},
	}
	sort.Slice(features, func(i, j int) bool { return features[i].Name < features[j].Name })
	return features, nil
}

func validScore(answer ScoreAnswer, levels int) bool {
	if answer.ScoreMicros < 0 || answer.ScoreMicros > int32(levels-1)*ProbabilityScale || answer.ConfidenceMicros < 0 || answer.ConfidenceMicros > ProbabilityScale || len(answer.ProbabilitiesMicros) != levels {
		return false
	}
	total := int64(0)
	for _, probability := range answer.ProbabilitiesMicros {
		if probability < 0 || probability > ProbabilityScale {
			return false
		}
		total += int64(probability)
	}
	return total >= int64(ProbabilityScale)-1 && total <= int64(ProbabilityScale)+1
}

func validNoul(answer NoulAnswer) bool {
	return answer.NoulMicros >= 0 && answer.NoulMicros <= ProbabilityScale
}
