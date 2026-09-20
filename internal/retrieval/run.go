package retrieval

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

const runSchemaVersion = "retrieval-run.v2"

var (
	ErrInvalidRun  = errors.New("invalid retrieval run")
	ErrRunNotFound = errors.New("retrieval run not found")
	runIDPattern   = regexp.MustCompile(`^rrun_[0-9a-f]{64}$`)
)

type RunDisposition string

const (
	RunSelected  RunDisposition = "selected"
	RunAbstained RunDisposition = "abstained"
	RunFailed    RunDisposition = "failed"
)

type SnapshotIndex struct {
	Channel     ChannelName `json:"channel"`
	ManifestID  string      `json:"manifest_id"`
	Approximate bool        `json:"approximate"`
}

type ServingConfig struct {
	SchemaVersion    string          `json:"schema_version"`
	TenantID         domain.TenantID `json:"tenant_id"`
	PolicyManifestID string          `json:"policy_manifest_id"`
	RankerManifestID string          `json:"ranker_manifest_id"`
	Indexes          []SnapshotIndex `json:"indexes"`
	ID               string          `json:"-"`
	ContentHash      string          `json:"-"`
	CanonicalJSON    []byte          `json:"-"`
}

func BuildServingConfig(tenantID domain.TenantID, policyManifestID, rankerManifestID string, indexes []SnapshotIndex) (ServingConfig, error) {
	config := ServingConfig{SchemaVersion: "retrieval-serving-config.v1", TenantID: tenantID, PolicyManifestID: strings.TrimSpace(policyManifestID), RankerManifestID: strings.TrimSpace(rankerManifestID), Indexes: append([]SnapshotIndex(nil), indexes...)}
	snapshot := ServingSnapshot{ProjectionEpoch: 1, DocumentSetHash: strings.Repeat("0", 64), ServingConfigID: "pending", PolicyManifestID: config.PolicyManifestID, RankerManifestID: config.RankerManifestID, Indexes: config.Indexes}
	if tenantID == "" || validateSnapshot(&snapshot) != nil {
		return ServingConfig{}, ErrInvalidRun
	}
	config.Indexes = snapshot.Indexes
	canonicalJSON, hash, err := canonical.MarshalAndHash(config)
	if err != nil {
		return ServingConfig{}, err
	}
	config.ID, config.ContentHash, config.CanonicalJSON = "rsc_"+hash, hash, canonicalJSON
	return config, nil
}

func ValidateServingConfig(config ServingConfig) error {
	rebuilt, err := BuildServingConfig(config.TenantID, config.PolicyManifestID, config.RankerManifestID, config.Indexes)
	if err != nil || config.SchemaVersion != rebuilt.SchemaVersion || config.ID != rebuilt.ID || config.ContentHash != rebuilt.ContentHash || !bytes.Equal(config.CanonicalJSON, rebuilt.CanonicalJSON) {
		return ErrInvalidRun
	}
	return nil
}

type ServingSnapshot struct {
	ProjectionEpoch      uint64          `json:"projection_epoch"`
	ProjectionCreatedAt  time.Time       `json:"projection_created_at,omitempty"`
	VectorIndexCreatedAt time.Time       `json:"vector_index_created_at,omitempty"`
	DocumentSetHash      string          `json:"document_set_hash"`
	ServingConfigID      string          `json:"serving_config_id"`
	PolicyManifestID     string          `json:"policy_manifest_id"`
	RankerManifestID     string          `json:"ranker_manifest_id"`
	Indexes              []SnapshotIndex `json:"indexes"`
}

type ChannelExecution struct {
	Channel         ChannelName `json:"channel"`
	IndexManifestID string      `json:"index_manifest_id"`
	HitCount        uint32      `json:"hit_count"`
	Approximate     bool        `json:"approximate"`
	Complete        bool        `json:"complete"`
	DegradationCode string      `json:"degradation_code,omitempty"`
	LatencyMicros   int64       `json:"latency_micros"`
}

type PersistedHit struct {
	Channel           ChannelName `json:"channel"`
	VersionID         string      `json:"version_id"`
	Rank              uint32      `json:"rank"`
	RawScoreQuantized int64       `json:"raw_score_quantized"`
	IndexManifestID   string      `json:"index_manifest_id"`
	Approximate       bool        `json:"approximate"`
}

type PersistedGate struct {
	VersionID      string   `json:"version_id"`
	Eligible       bool     `json:"eligible"`
	AdvisoryOnly   bool     `json:"advisory_only"`
	RejectionCodes []string `json:"rejection_codes,omitempty"`
	CanonicalFacts string   `json:"canonical_facts"`
}

type PersistedFeature struct {
	Name  string `json:"name"`
	Value int32  `json:"value"`
}

type PersistedRank struct {
	VersionID            string             `json:"version_id"`
	RRFScore             int64              `json:"rrf_score"`
	FinalScore           int64              `json:"final_score"`
	Rank                 uint32             `json:"rank"`
	VerificationStrength int32              `json:"verification_strength"`
	ObservedEndToEnd     bool               `json:"observed_end_to_end"`
	Features             []PersistedFeature `json:"features,omitempty"`
	MissingFeatures      []string           `json:"missing_features,omitempty"`
}

type RunInput struct {
	ID                 string
	TenantID           domain.TenantID
	Query              Query
	RequestContextHash string
	QueryEnvelope      string
	Snapshot           ServingSnapshot
	ChannelExecutions  []ChannelExecution
	Hits               []PersistedHit
	Gates              []PersistedGate
	Ranked             []PersistedRank
	Disposition        RunDisposition
	DecisionCode       string
	SelectedVersionIDs []string
	CreatedAt          time.Time
	CompletedAt        time.Time
	ExpiresAt          time.Time
}

type Run struct {
	SchemaVersion       string             `json:"schema_version"`
	ID                  string             `json:"id"`
	TenantID            domain.TenantID    `json:"tenant_id"`
	QueryHash           string             `json:"query_hash"`
	RequestContextHash  string             `json:"request_context_hash"`
	QueryEnvelope       string             `json:"query_envelope"`
	Snapshot            ServingSnapshot    `json:"snapshot"`
	ChannelExecutions   []ChannelExecution `json:"channel_executions,omitempty"`
	Hits                []PersistedHit     `json:"hits,omitempty"`
	Gates               []PersistedGate    `json:"gates,omitempty"`
	Ranked              []PersistedRank    `json:"ranked,omitempty"`
	CandidateVersionIDs []string           `json:"candidate_version_ids,omitempty"`
	IndexManifestIDs    []string           `json:"index_manifest_ids"`
	Disposition         RunDisposition     `json:"disposition"`
	DecisionCode        string             `json:"decision_code,omitempty"`
	SelectedVersionIDs  []string           `json:"selected_version_ids,omitempty"`
	CreatedAt           time.Time          `json:"created_at"`
	CompletedAt         time.Time          `json:"completed_at"`
	ExpiresAt           time.Time          `json:"expires_at"`
	ContentHash         string             `json:"-"`
	CanonicalJSON       []byte             `json:"-"`
}

func BuildRun(input RunInput) (Run, error) {
	if !runIDPattern.MatchString(input.ID) || input.TenantID == "" || ValidateQuery(input.Query) != nil || input.Query.TenantID != input.TenantID || !strings.HasPrefix(input.QueryEnvelope, "enc.v1.") {
		return Run{}, ErrInvalidRun
	}
	run := Run{
		SchemaVersion: runSchemaVersion, ID: input.ID, TenantID: input.TenantID, QueryHash: input.Query.Hash, RequestContextHash: input.RequestContextHash,
		QueryEnvelope: input.QueryEnvelope, Snapshot: cloneSnapshot(input.Snapshot),
		ChannelExecutions: append([]ChannelExecution(nil), input.ChannelExecutions...), Hits: append([]PersistedHit(nil), input.Hits...),
		Gates: cloneGates(input.Gates), Ranked: cloneRanks(input.Ranked), Disposition: input.Disposition,
		DecisionCode: strings.TrimSpace(input.DecisionCode), SelectedVersionIDs: append([]string(nil), input.SelectedVersionIDs...),
		CreatedAt: input.CreatedAt.UTC(), CompletedAt: input.CompletedAt.UTC(), ExpiresAt: input.ExpiresAt.UTC(),
	}
	if err := canonicalizeRun(&run); err != nil {
		return Run{}, err
	}
	canonicalJSON, hash, err := canonical.MarshalAndHash(run)
	if err != nil {
		return Run{}, fmt.Errorf("canonicalize retrieval run: %w", err)
	}
	run.ContentHash, run.CanonicalJSON = hash, canonicalJSON
	return run, nil
}

func ValidateRun(run Run) error {
	// Query contents are encrypted by this boundary, so validation uses the
	// persisted hash after structural validation below rather than rebuilding it.
	if !sha256Pattern.MatchString(run.QueryHash) || !sha256Pattern.MatchString(run.RequestContextHash) {
		return ErrInvalidRun
	}
	copyOfRun := run
	copyOfRun.ContentHash, copyOfRun.CanonicalJSON = "", nil
	if err := validateCanonicalRun(&copyOfRun); err != nil {
		return err
	}
	canonicalJSON, hash, err := canonical.MarshalAndHash(copyOfRun)
	if err != nil || hash != run.ContentHash || !bytes.Equal(canonicalJSON, run.CanonicalJSON) {
		return ErrInvalidRun
	}
	return nil
}

func canonicalizeRun(run *Run) error {
	if err := validateSnapshot(&run.Snapshot); err != nil {
		return err
	}
	sort.Slice(run.ChannelExecutions, func(i, j int) bool { return run.ChannelExecutions[i].Channel < run.ChannelExecutions[j].Channel })
	sort.Slice(run.Hits, func(i, j int) bool {
		if run.Hits[i].Channel != run.Hits[j].Channel {
			return run.Hits[i].Channel < run.Hits[j].Channel
		}
		return run.Hits[i].Rank < run.Hits[j].Rank
	})
	for index := range run.Gates {
		sort.Strings(run.Gates[index].RejectionCodes)
	}
	sort.Slice(run.Gates, func(i, j int) bool { return run.Gates[i].VersionID < run.Gates[j].VersionID })
	for index := range run.Ranked {
		sort.Slice(run.Ranked[index].Features, func(i, j int) bool { return run.Ranked[index].Features[i].Name < run.Ranked[index].Features[j].Name })
		sort.Strings(run.Ranked[index].MissingFeatures)
	}
	sort.Slice(run.Ranked, func(i, j int) bool { return run.Ranked[i].Rank < run.Ranked[j].Rank })
	return validateCanonicalRun(run)
}

func validateCanonicalRun(run *Run) error {
	if run.SchemaVersion != runSchemaVersion || !runIDPattern.MatchString(run.ID) || run.TenantID == "" || !sha256Pattern.MatchString(run.QueryHash) || !sha256Pattern.MatchString(run.RequestContextHash) || !strings.HasPrefix(run.QueryEnvelope, "enc.v1.") || len(run.QueryEnvelope) > 1<<20 || validateSnapshot(&run.Snapshot) != nil || run.CreatedAt.IsZero() || run.CompletedAt.Before(run.CreatedAt) || !run.ExpiresAt.After(run.CompletedAt) || run.ExpiresAt.After(run.CompletedAt.Add(30*24*time.Hour)) || len(run.ChannelExecutions) > 5 || len(run.Hits) > 5000 || len(run.Gates) > 1000 || len(run.Ranked) > 1000 {
		return ErrInvalidRun
	}
	if run.Disposition != RunSelected && run.Disposition != RunAbstained && run.Disposition != RunFailed {
		return ErrInvalidRun
	}
	if run.DecisionCode != strings.TrimSpace(run.DecisionCode) || len(run.DecisionCode) > 128 || run.Disposition == RunSelected && (run.DecisionCode != "" || len(run.SelectedVersionIDs) == 0) {
		return ErrInvalidRun
	}
	if run.Disposition != RunSelected && (run.DecisionCode == "" || len(run.SelectedVersionIDs) != 0) {
		return ErrInvalidRun
	}
	indexByChannel := make(map[ChannelName]SnapshotIndex, len(run.Snapshot.Indexes))
	indexIDs := make([]string, len(run.Snapshot.Indexes))
	for index, item := range run.Snapshot.Indexes {
		indexByChannel[item.Channel] = item
		indexIDs[index] = item.ManifestID
	}
	run.IndexManifestIDs = append([]string(nil), indexIDs...)
	sort.Strings(run.IndexManifestIDs)
	executions := make(map[ChannelName]ChannelExecution, len(run.ChannelExecutions))
	for index, item := range run.ChannelExecutions {
		if !validChannel(item.Channel) || strings.TrimSpace(item.IndexManifestID) == "" || len(item.IndexManifestID) > 256 || item.LatencyMicros < 0 || item.DegradationCode != strings.TrimSpace(item.DegradationCode) || len(item.DegradationCode) > 128 || index > 0 && run.ChannelExecutions[index-1].Channel >= item.Channel {
			return ErrInvalidRun
		}
		snapshotIndex, ok := indexByChannel[item.Channel]
		if !ok || snapshotIndex.ManifestID != item.IndexManifestID || snapshotIndex.Approximate != item.Approximate || item.Complete == (item.DegradationCode != "") {
			return ErrInvalidRun
		}
		executions[item.Channel] = item
	}
	if run.Disposition != RunFailed && len(executions) != len(indexByChannel) {
		return ErrInvalidRun
	}
	hitCounts := make(map[ChannelName]uint32)
	candidates := make(map[string]struct{})
	lastRank := make(map[ChannelName]uint32)
	for _, item := range run.Hits {
		execution, ok := executions[item.Channel]
		if !ok || !execution.Complete || item.VersionID == "" || item.Rank != lastRank[item.Channel]+1 || item.IndexManifestID != execution.IndexManifestID || item.Approximate != execution.Approximate {
			return ErrInvalidRun
		}
		lastRank[item.Channel], hitCounts[item.Channel] = item.Rank, hitCounts[item.Channel]+1
		candidates[item.VersionID] = struct{}{}
	}
	for channel, execution := range executions {
		if execution.HitCount != hitCounts[channel] {
			return ErrInvalidRun
		}
	}
	run.CandidateVersionIDs = run.CandidateVersionIDs[:0]
	for versionID := range candidates {
		run.CandidateVersionIDs = append(run.CandidateVersionIDs, versionID)
	}
	sort.Strings(run.CandidateVersionIDs)
	gates := make(map[string]PersistedGate, len(run.Gates))
	for index, gate := range run.Gates {
		if gate.VersionID == "" || index > 0 && run.Gates[index-1].VersionID >= gate.VersionID || gate.AdvisoryOnly && !gate.Eligible || gate.Eligible == (len(gate.RejectionCodes) > 0) || !strictStrings(gate.RejectionCodes) || !canonicalJSONString(gate.CanonicalFacts) {
			return ErrInvalidRun
		}
		gates[gate.VersionID] = gate
		if run.Disposition != RunFailed {
			if _, ok := candidates[gate.VersionID]; !ok {
				return ErrInvalidRun
			}
		}
	}
	if run.Disposition != RunFailed {
		for versionID := range candidates {
			if _, ok := gates[versionID]; !ok {
				return ErrInvalidRun
			}
		}
	}
	ranked := make(map[string]PersistedRank, len(run.Ranked))
	for index, item := range run.Ranked {
		if item.VersionID == "" || item.Rank != uint32(index+1) || item.VerificationStrength < 0 || !strictFeatures(item.Features) || !strictStrings(item.MissingFeatures) {
			return ErrInvalidRun
		}
		gate, ok := gates[item.VersionID]
		if !ok || !gate.Eligible {
			return ErrInvalidRun
		}
		if _, ok := candidates[item.VersionID]; !ok {
			return ErrInvalidRun
		}
		if _, duplicate := ranked[item.VersionID]; duplicate {
			return ErrInvalidRun
		}
		ranked[item.VersionID] = item
	}
	var previousSelectedRank uint32
	for _, versionID := range run.SelectedVersionIDs {
		item, ok := ranked[versionID]
		if !ok || item.Rank <= previousSelectedRank {
			return ErrInvalidRun
		}
		previousSelectedRank = item.Rank
	}
	return nil
}

func validateSnapshot(snapshot *ServingSnapshot) error {
	if snapshot.ProjectionEpoch == 0 || !sha256Pattern.MatchString(snapshot.DocumentSetHash) || strings.TrimSpace(snapshot.ServingConfigID) == "" || strings.TrimSpace(snapshot.PolicyManifestID) == "" || strings.TrimSpace(snapshot.RankerManifestID) == "" || len(snapshot.Indexes) == 0 || len(snapshot.Indexes) > 5 {
		return ErrInvalidRun
	}
	sort.Slice(snapshot.Indexes, func(i, j int) bool { return snapshot.Indexes[i].Channel < snapshot.Indexes[j].Channel })
	for index, item := range snapshot.Indexes {
		if !validChannel(item.Channel) || strings.TrimSpace(item.ManifestID) == "" || item.Approximate != (item.Channel == ChannelVector) || index > 0 && snapshot.Indexes[index-1].Channel == item.Channel {
			return ErrInvalidRun
		}
	}
	return nil
}

func canonicalJSONString(value string) bool {
	canonicalJSON, _, err := canonical.MarshalAndHashRaw([]byte(value))
	return err == nil && bytes.Equal(canonicalJSON, []byte(value))
}

func strictStrings(values []string) bool {
	for index, value := range values {
		if strings.TrimSpace(value) == "" || len(value) > 256 || index > 0 && values[index-1] >= value {
			return false
		}
	}
	return true
}

func strictFeatures(values []PersistedFeature) bool {
	for index, value := range values {
		if strings.TrimSpace(value.Name) == "" || len(value.Name) > 128 || index > 0 && values[index-1].Name >= value.Name {
			return false
		}
	}
	return true
}

func cloneSnapshot(value ServingSnapshot) ServingSnapshot {
	value.Indexes = append([]SnapshotIndex(nil), value.Indexes...)
	return value
}
func cloneGates(values []PersistedGate) []PersistedGate {
	result := append([]PersistedGate(nil), values...)
	for index := range result {
		result[index].RejectionCodes = append([]string(nil), result[index].RejectionCodes...)
	}
	return result
}
func cloneRanks(values []PersistedRank) []PersistedRank {
	result := append([]PersistedRank(nil), values...)
	for index := range result {
		result[index].Features = append([]PersistedFeature(nil), result[index].Features...)
		result[index].MissingFeatures = append([]string(nil), result[index].MissingFeatures...)
	}
	return result
}

func DecodeRun(canonicalJSON []byte, contentHash string) (Run, error) {
	var run Run
	if err := json.Unmarshal(canonicalJSON, &run); err != nil {
		return Run{}, ErrInvalidRun
	}
	run.CanonicalJSON, run.ContentHash = append([]byte(nil), canonicalJSON...), contentHash
	if err := ValidateRun(run); err != nil {
		return Run{}, err
	}
	return run, nil
}
