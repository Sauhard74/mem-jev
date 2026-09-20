package retrieval

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/eligibility"
)

type Explanation struct {
	RunID        string
	Disposition  RunDisposition
	DecisionCode string
	QueryHash    string
	Snapshot     ServingSnapshot
	Candidates   []CandidateExplanation
	Degraded     []DegradedChannel
	Approximate  bool
	Plan         *PlanArtifact
}

type CandidateExplanation struct {
	VersionID      string
	Eligible       bool
	AdvisoryOnly   bool
	SourceChannels []ChannelName
	Facts          []eligibility.Rejection
	RRFScore       int64
	FinalScore     int64
	FinalRank      uint32
}

func (s *Service) Explain(ctx context.Context, tenantID domain.TenantID, runID string) (Explanation, error) {
	if err := ctx.Err(); err != nil {
		return Explanation{}, err
	}
	if s == nil || tenantID == "" || runID == "" {
		return Explanation{}, ErrServiceUnavailable
	}
	run, err := s.repository.RetrievalRun(ctx, tenantID, runID)
	if err != nil {
		return Explanation{}, err
	}
	channels := make(map[string][]ChannelName, len(run.CandidateVersionIDs))
	for _, hit := range run.Hits {
		channels[hit.VersionID] = append(channels[hit.VersionID], hit.Channel)
	}
	ranks := make(map[string]PersistedRank, len(run.Ranked))
	for _, item := range run.Ranked {
		ranks[item.VersionID] = item
	}
	gates := make(map[string]PersistedGate, len(run.Gates))
	for _, item := range run.Gates {
		gates[item.VersionID] = item
	}
	explanation := Explanation{RunID: run.ID, Disposition: run.Disposition, DecisionCode: run.DecisionCode, QueryHash: run.QueryHash, Snapshot: run.Snapshot}
	for _, execution := range run.ChannelExecutions {
		explanation.Approximate = explanation.Approximate || execution.Complete && execution.Approximate
		if !execution.Complete {
			explanation.Degraded = append(explanation.Degraded, DegradedChannel{Channel: execution.Channel, Code: execution.DegradationCode, IndexManifestID: execution.IndexManifestID, Approximate: execution.Approximate, LatencyMicros: execution.LatencyMicros})
		}
	}
	for _, versionID := range run.CandidateVersionIDs {
		gate, ok := gates[versionID]
		if !ok {
			continue
		}
		var facts []eligibility.Rejection
		if err := json.Unmarshal([]byte(gate.CanonicalFacts), &facts); err != nil {
			return Explanation{}, ErrInvalidRun
		}
		item := ranks[versionID]
		source := append([]ChannelName(nil), channels[versionID]...)
		sort.Slice(source, func(i, j int) bool { return source[i] < source[j] })
		explanation.Candidates = append(explanation.Candidates, CandidateExplanation{VersionID: versionID, Eligible: gate.Eligible, AdvisoryOnly: gate.AdvisoryOnly, SourceChannels: source, Facts: facts, RRFScore: item.RRFScore, FinalScore: item.FinalScore, FinalRank: item.Rank})
	}
	if run.Disposition == RunSelected {
		plan, findErr := s.plans.FindByRetrievalRunID(ctx, tenantID, run.ID)
		if findErr != nil {
			return Explanation{}, &ServiceError{Code: "plan_persistence_failed", RunID: run.ID, Err: findErr}
		}
		explanation.Plan = &plan
	}
	return explanation, nil
}
