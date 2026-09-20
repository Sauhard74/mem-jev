package selection

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/composition"
	"github.com/sauhard74/mem-jev/internal/domain"
)

var ErrInvalidSelection = errors.New("invalid selection")

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type NoveltyClass string

const (
	NoveltyExact      NoveltyClass = "exact"
	NoveltyKnownShape NoveltyClass = "known_shape"
	NoveltyBridged    NoveltyClass = "bridged"
	NoveltyPartial    NoveltyClass = "partial"
	NoveltyUnseen     NoveltyClass = "unseen"
)

type Draft struct {
	SchemaVersion             string                        `json:"schema_version"`
	TenantID                  domain.TenantID               `json:"tenant_id"`
	QueryHash                 string                        `json:"query_hash"`
	RequestContextHash        string                        `json:"request_context_hash"`
	RetrievalRunID            string                        `json:"retrieval_run_id"`
	ProjectionEpoch           uint64                        `json:"projection_epoch"`
	DocumentSetHash           string                        `json:"document_set_hash"`
	ServingConfigID           string                        `json:"serving_config_id"`
	PolicyManifestID          string                        `json:"policy_manifest_id"`
	RankerManifestID          string                        `json:"ranker_manifest_id"`
	PlannerManifestID         string                        `json:"planner_manifest_id"`
	LifecyclePolicyManifestID string                        `json:"lifecycle_policy_manifest_id"`
	ExperimentManifestID      string                        `json:"experiment_manifest_id,omitempty"`
	ExperimentAssignmentID    string                        `json:"experiment_assignment_id,omitempty"`
	NoveltyClass              NoveltyClass                  `json:"novelty_class"`
	Plan                      composition.Plan              `json:"plan"`
	ParallelSchedule          *composition.ParallelSchedule `json:"parallel_schedule,omitempty"`
	ContentHash               string                        `json:"content_hash,omitempty"`
	CanonicalJSON             []byte                        `json:"-"`
}

type Record struct {
	SchemaVersion             string                        `json:"schema_version"`
	InjectionID               string                        `json:"injection_id"`
	IdempotencyIdentityHash   string                        `json:"idempotency_identity_hash"`
	TenantID                  domain.TenantID               `json:"tenant_id"`
	QueryHash                 string                        `json:"query_hash"`
	RequestContextHash        string                        `json:"request_context_hash"`
	RetrievalRunID            string                        `json:"retrieval_run_id"`
	ProjectionEpoch           uint64                        `json:"projection_epoch"`
	DocumentSetHash           string                        `json:"document_set_hash"`
	ServingConfigID           string                        `json:"serving_config_id"`
	PolicyManifestID          string                        `json:"policy_manifest_id"`
	RankerManifestID          string                        `json:"ranker_manifest_id"`
	PlannerManifestID         string                        `json:"planner_manifest_id"`
	LifecyclePolicyManifestID string                        `json:"lifecycle_policy_manifest_id"`
	ExperimentManifestID      string                        `json:"experiment_manifest_id,omitempty"`
	ExperimentAssignmentID    string                        `json:"experiment_assignment_id,omitempty"`
	NoveltyClass              NoveltyClass                  `json:"novelty_class"`
	Plan                      composition.Plan              `json:"plan"`
	ParallelSchedule          *composition.ParallelSchedule `json:"parallel_schedule,omitempty"`
	DraftHash                 string                        `json:"draft_hash"`
	CreatedAt                 time.Time                     `json:"created_at"`
	ExpiresAt                 time.Time                     `json:"expires_at"`
	ContentHash               string                        `json:"content_hash,omitempty"`
	CanonicalJSON             []byte                        `json:"-"`
}

func NewDraft(value Draft) (Draft, error) {
	value.Plan = clonePlan(value.Plan)
	value.ParallelSchedule = cloneSchedule(value.ParallelSchedule)
	value.SchemaVersion = "selection-draft.v1"
	value.ContentHash, value.CanonicalJSON = "", nil
	if err := validateDraftFields(value); err != nil {
		return Draft{}, err
	}
	encoded, hash, err := canonical.MarshalAndHash(value)
	if err != nil {
		return Draft{}, err
	}
	value.ContentHash, value.CanonicalJSON = hash, encoded
	return value, nil
}

func ValidateDraft(value Draft) error {
	if value.SchemaVersion != "selection-draft.v1" || !sha256Pattern.MatchString(value.ContentHash) || len(value.CanonicalJSON) == 0 || len(value.CanonicalJSON) > 16<<20 || validateDraftFields(value) != nil {
		return ErrInvalidSelection
	}
	copyOfValue := value
	copyOfValue.ContentHash, copyOfValue.CanonicalJSON = "", nil
	encoded, hash, err := canonical.MarshalAndHash(copyOfValue)
	if err != nil || hash != value.ContentHash || !bytes.Equal(encoded, value.CanonicalJSON) {
		return ErrInvalidSelection
	}
	return nil
}

func validateDraftFields(value Draft) error {
	if !bounded(string(value.TenantID)) || value.ProjectionEpoch == 0 || !sha256Pattern.MatchString(value.QueryHash) || !sha256Pattern.MatchString(value.RequestContextHash) || !sha256Pattern.MatchString(value.DocumentSetHash) ||
		!bounded(value.RetrievalRunID) || !bounded(value.ServingConfigID) || !bounded(value.PolicyManifestID) || !bounded(value.RankerManifestID) || !bounded(value.PlannerManifestID) || !bounded(value.LifecyclePolicyManifestID) ||
		value.Plan.TenantID != value.TenantID || value.Plan.ProjectionEpoch != value.ProjectionEpoch || value.Plan.PolicyManifestID != value.PolicyManifestID || value.Plan.PlannerManifestID != value.PlannerManifestID || composition.ValidatePlan(value.Plan) != nil || !validNovelty(value.NoveltyClass) {
		return ErrInvalidSelection
	}
	if (value.ExperimentManifestID == "") != (value.ExperimentAssignmentID == "") || value.ExperimentManifestID != "" && (!bounded(value.ExperimentManifestID) || !bounded(value.ExperimentAssignmentID)) {
		return ErrInvalidSelection
	}
	hasBridge := false
	for _, node := range value.Plan.Nodes {
		hasBridge = hasBridge || node.Bridge
	}
	if value.Plan.Complete {
		if value.NoveltyClass == NoveltyPartial || hasBridge != (value.NoveltyClass == NoveltyBridged) || value.ParallelSchedule == nil || composition.ValidateParallelSchedule(*value.ParallelSchedule) != nil || value.ParallelSchedule.PlanID != value.Plan.ID || value.ParallelSchedule.PlanHash != value.Plan.ContentHash || value.ParallelSchedule.TenantID != value.TenantID || value.ParallelSchedule.ProjectionEpoch != value.ProjectionEpoch || value.ParallelSchedule.CompatibilityGraphID != value.Plan.CompatibilityGraphID || value.ParallelSchedule.CompatibilityGraphHash != value.Plan.CompatibilityGraphHash || value.ParallelSchedule.PlannerManifestID != value.Plan.PlannerManifestID || value.ParallelSchedule.PolicyManifestID != value.Plan.PolicyManifestID || !scheduleCoversPlan(*value.ParallelSchedule, value.Plan) {
			return ErrInvalidSelection
		}
	} else if value.ParallelSchedule != nil || value.NoveltyClass != NoveltyPartial || len(value.Plan.Gaps) == 0 {
		return ErrInvalidSelection
	}
	return nil
}

func scheduleCoversPlan(schedule composition.ParallelSchedule, plan composition.Plan) bool {
	wanted := make(map[string]struct{}, len(plan.Nodes))
	groupByVersion := make(map[string]uint32, len(plan.Nodes))
	for _, node := range plan.Nodes {
		wanted[node.VersionID] = struct{}{}
	}
	seen := 0
	for _, group := range schedule.Groups {
		for _, versionID := range group.NodeVersionIDs {
			if _, ok := wanted[versionID]; !ok {
				return false
			}
			delete(wanted, versionID)
			groupByVersion[versionID] = group.Ordinal
			seen++
		}
	}
	if seen != len(plan.Nodes) || len(wanted) != 0 {
		return false
	}
	for _, dependency := range plan.Dependencies {
		if groupByVersion[dependency.SourceVersionID] >= groupByVersion[dependency.TargetVersionID] {
			return false
		}
	}
	return true
}

func NewRecord(draft Draft, idempotencyHash string, createdAt, expiresAt time.Time) (Record, error) {
	if ValidateDraft(draft) != nil || !sha256Pattern.MatchString(idempotencyHash) || createdAt.IsZero() || !createdAt.Equal(createdAt.UTC()) || !expiresAt.Equal(expiresAt.UTC()) || !expiresAt.After(createdAt) {
		return Record{}, ErrInvalidSelection
	}
	record := Record{
		SchemaVersion: "selection-record.v1", InjectionID: InjectionID(draft.TenantID, idempotencyHash), IdempotencyIdentityHash: idempotencyHash,
		TenantID: draft.TenantID, QueryHash: draft.QueryHash, RequestContextHash: draft.RequestContextHash, RetrievalRunID: draft.RetrievalRunID,
		ProjectionEpoch: draft.ProjectionEpoch, DocumentSetHash: draft.DocumentSetHash, ServingConfigID: draft.ServingConfigID,
		PolicyManifestID: draft.PolicyManifestID, RankerManifestID: draft.RankerManifestID, PlannerManifestID: draft.PlannerManifestID,
		LifecyclePolicyManifestID: draft.LifecyclePolicyManifestID, ExperimentManifestID: draft.ExperimentManifestID, ExperimentAssignmentID: draft.ExperimentAssignmentID,
		NoveltyClass: draft.NoveltyClass, Plan: clonePlan(draft.Plan), ParallelSchedule: cloneSchedule(draft.ParallelSchedule), DraftHash: draft.ContentHash,
		CreatedAt: createdAt, ExpiresAt: expiresAt,
	}
	encoded, hash, err := canonical.MarshalAndHash(record)
	if err != nil {
		return Record{}, err
	}
	record.ContentHash, record.CanonicalJSON = hash, encoded
	return record, nil
}

func ValidateRecord(record Record) error {
	if record.SchemaVersion != "selection-record.v1" || record.InjectionID != InjectionID(record.TenantID, record.IdempotencyIdentityHash) || !sha256Pattern.MatchString(record.IdempotencyIdentityHash) || !sha256Pattern.MatchString(record.DraftHash) || !sha256Pattern.MatchString(record.ContentHash) || len(record.CanonicalJSON) == 0 || len(record.CanonicalJSON) > 16<<20 || record.CreatedAt.IsZero() || !record.CreatedAt.Equal(record.CreatedAt.UTC()) || !record.ExpiresAt.Equal(record.ExpiresAt.UTC()) || !record.ExpiresAt.After(record.CreatedAt) {
		return ErrInvalidSelection
	}
	draft, err := NewDraft(Draft{SchemaVersion: "selection-draft.v1", TenantID: record.TenantID, QueryHash: record.QueryHash, RequestContextHash: record.RequestContextHash, RetrievalRunID: record.RetrievalRunID, ProjectionEpoch: record.ProjectionEpoch, DocumentSetHash: record.DocumentSetHash, ServingConfigID: record.ServingConfigID, PolicyManifestID: record.PolicyManifestID, RankerManifestID: record.RankerManifestID, PlannerManifestID: record.PlannerManifestID, LifecyclePolicyManifestID: record.LifecyclePolicyManifestID, ExperimentManifestID: record.ExperimentManifestID, ExperimentAssignmentID: record.ExperimentAssignmentID, NoveltyClass: record.NoveltyClass, Plan: record.Plan, ParallelSchedule: cloneSchedule(record.ParallelSchedule)})
	if err != nil || draft.ContentHash != record.DraftHash {
		return ErrInvalidSelection
	}
	copyOfRecord := record
	copyOfRecord.ContentHash, copyOfRecord.CanonicalJSON = "", nil
	encoded, hash, err := canonical.MarshalAndHash(copyOfRecord)
	if err != nil || hash != record.ContentHash || !bytes.Equal(encoded, record.CanonicalJSON) {
		return ErrInvalidSelection
	}
	return nil
}

func DecodeRecord(canonicalJSON []byte, contentHash string) (Record, error) {
	if len(canonicalJSON) == 0 || len(canonicalJSON) > 16<<20 || !sha256Pattern.MatchString(contentHash) {
		return Record{}, ErrInvalidSelection
	}
	var record Record
	if err := json.Unmarshal(canonicalJSON, &record); err != nil {
		return Record{}, ErrInvalidSelection
	}
	plan, err := composition.HydratePlan(record.Plan)
	if err != nil {
		return Record{}, ErrInvalidSelection
	}
	record.Plan = plan
	if record.ParallelSchedule != nil {
		schedule, scheduleErr := composition.HydrateParallelSchedule(*record.ParallelSchedule)
		if scheduleErr != nil {
			return Record{}, ErrInvalidSelection
		}
		record.ParallelSchedule = &schedule
	}
	record.ContentHash, record.CanonicalJSON = contentHash, append([]byte(nil), canonicalJSON...)
	if ValidateRecord(record) != nil {
		return Record{}, ErrInvalidSelection
	}
	return record, nil
}

func InjectionID(tenantID domain.TenantID, idempotencyHash string) string {
	sum := sha256.Sum256([]byte(string(tenantID) + "\x00" + idempotencyHash))
	return "inj_" + hex.EncodeToString(sum[:])
}

func TaskExecutionID(injectionID string) string {
	sum := sha256.Sum256([]byte("task-execution.v1\x00" + injectionID))
	return "texec_" + hex.EncodeToString(sum[:])
}

func validNovelty(value NoveltyClass) bool {
	switch value {
	case NoveltyExact, NoveltyKnownShape, NoveltyBridged, NoveltyPartial, NoveltyUnseen:
		return true
	default:
		return false
	}
}

func bounded(value string) bool {
	return value != "" && len(value) <= 1_024 && !strings.ContainsRune(value, '\x00')
}

func cloneSchedule(source *composition.ParallelSchedule) *composition.ParallelSchedule {
	if source == nil {
		return nil
	}
	result := *source
	result.CanonicalJSON = append([]byte(nil), source.CanonicalJSON...)
	result.Groups = append([]composition.ParallelGroup(nil), source.Groups...)
	for index := range result.Groups {
		result.Groups[index].CanonicalJSON = append([]byte(nil), source.Groups[index].CanonicalJSON...)
		result.Groups[index].NodeVersionIDs = append([]string(nil), source.Groups[index].NodeVersionIDs...)
	}
	return &result
}

func clonePlan(source composition.Plan) composition.Plan {
	result := source
	result.CanonicalJSON = append([]byte(nil), source.CanonicalJSON...)
	result.Nodes = append([]composition.PlanNode(nil), source.Nodes...)
	result.Dependencies = append([]composition.PlanDependency(nil), source.Dependencies...)
	for index := range result.Dependencies {
		result.Dependencies[index].SourceProvisionIDs = append([]string(nil), source.Dependencies[index].SourceProvisionIDs...)
		result.Dependencies[index].SatisfiedRequirementIDs = append([]string(nil), source.Dependencies[index].SatisfiedRequirementIDs...)
	}
	result.CompatibilityEdgeIDs = append([]string(nil), source.CompatibilityEdgeIDs...)
	result.ExternallySatisfiedRequirementIDs = append([]string(nil), source.ExternallySatisfiedRequirementIDs...)
	result.SatisfiedRequirementIDs = append([]string(nil), source.SatisfiedRequirementIDs...)
	result.ProducedResourceIDs = append([]string(nil), source.ProducedResourceIDs...)
	result.CoveredGoalPredicateIDs = append([]string(nil), source.CoveredGoalPredicateIDs...)
	result.RequestedGoalPredicateIDs = append([]string(nil), source.RequestedGoalPredicateIDs...)
	result.SeedVersionIDs = append([]string(nil), source.SeedVersionIDs...)
	result.Gaps = append([]composition.PlanGap(nil), source.Gaps...)
	result.LimitCodes = append([]string(nil), source.LimitCodes...)
	return result
}

func CloneRecord(source Record) Record {
	result := source
	result.Plan = clonePlan(source.Plan)
	result.ParallelSchedule = cloneSchedule(source.ParallelSchedule)
	result.CanonicalJSON = append([]byte(nil), source.CanonicalJSON...)
	return result
}
