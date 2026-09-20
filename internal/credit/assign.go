package credit

import (
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/evidence"
	"github.com/sauhard74/mem-jev/internal/selection"
)

type AssignmentRequest struct {
	Manifest        RuleManifest
	Selection       selection.Record
	Outcome         domain.CanonicalOutcome
	Evaluation      evidence.Result
	InjectionID     string
	TaskExecutionID string
	Now             time.Time
}

func Assign(request AssignmentRequest) (Record, error) {
	if selection.ValidateRecord(request.Selection) != nil || ValidateRuleManifest(request.Manifest) != nil || request.Now.IsZero() || request.Outcome.TenantID != request.Selection.TenantID || request.InjectionID != request.Selection.InjectionID || request.TaskExecutionID != selection.TaskExecutionID(request.Selection.InjectionID) || !request.Now.Before(request.Selection.ExpiresAt) {
		return Record{}, ErrInvalidCredit
	}
	reasons := []string{}
	temporal := temporalEvidence(request)
	goalSatisfied, goalFailed := goalEvidence(request.Selection, request.Outcome)
	class := Unattributable
	switch request.Evaluation.State {
	case domain.OutcomeStateVerifiedSuccess:
		if temporal && request.Selection.Plan.Complete && goalSatisfied {
			class = CausalSuccess
		} else {
			class = AssociatedSuccess
		}
	case domain.OutcomeStateProvisionalSuccess:
		class = AssociatedSuccess
		reasons = append(reasons, "provisional_evidence")
	case domain.OutcomeStateVerifiedFailure:
		if temporal && request.Selection.Plan.Complete && goalFailed {
			class = CausalFailure
		} else {
			class = AssociatedFailure
		}
	default:
		reasons = append(reasons, "inconclusive_outcome")
	}
	if !temporal {
		reasons = append(reasons, "outside_attribution_window")
	}
	if !request.Selection.Plan.Complete {
		reasons = append(reasons, "partial_plan")
	}
	if request.Evaluation.State == domain.OutcomeStateVerifiedSuccess && !goalSatisfied || request.Evaluation.State == domain.OutcomeStateVerifiedFailure && !goalFailed {
		reasons = append(reasons, "goal_evidence_missing")
	}
	procedureVersionID := ""
	if (class == CausalSuccess || class == CausalFailure) && len(request.Selection.Plan.Nodes) == 1 {
		procedureVersionID = request.Selection.Plan.Nodes[0].VersionID
	}
	return newRecord(Record{TenantID: request.Outcome.TenantID, OutcomeID: request.Outcome.ID, InjectionID: request.InjectionID, TaskExecutionID: request.TaskExecutionID, ProcedureVersionID: procedureVersionID, Class: class, RuleManifestID: request.Manifest.ID, SelectionHash: request.Selection.ContentHash, ReasonCodes: reasons, CreatedAt: request.Now.UTC(), ExpiresAt: request.Selection.ExpiresAt})
}

func temporalEvidence(request AssignmentRequest) bool {
	skew := time.Duration(request.Manifest.MaximumClockSkewSeconds) * time.Second
	for _, fact := range request.Outcome.Evidence {
		observed, err := time.Parse(time.RFC3339Nano, fact.ObservedAt)
		if err != nil || observed.Before(request.Selection.CreatedAt.Add(-skew)) || observed.After(request.Now.Add(skew)) || !observed.Before(request.Selection.ExpiresAt) {
			return false
		}
	}
	return true
}

func goalEvidence(record selection.Record, outcome domain.CanonicalOutcome) (bool, bool) {
	verdicts := make(map[string]domain.EvidenceVerdict, len(outcome.Evidence))
	for _, fact := range outcome.Evidence {
		if fact.Class == domain.EvidenceClassGoalPredicate || fact.Class == domain.EvidenceClassIndependentVerifier {
			verdicts[fact.PredicateID] = fact.Verdict
		}
	}
	allSatisfied, anyFailed := len(record.Plan.RequestedGoalPredicateIDs) > 0, false
	for _, goal := range record.Plan.RequestedGoalPredicateIDs {
		allSatisfied = allSatisfied && verdicts[goal] == domain.EvidenceVerdictSatisfied
		anyFailed = anyFailed || verdicts[goal] == domain.EvidenceVerdictFailed
	}
	return allSatisfied, anyFailed
}
