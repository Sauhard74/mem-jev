package evidence

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/domain"
)

var ErrInvalidPolicy = errors.New("invalid evidence policy")

type Policy struct {
	Version               string
	RequiredPredicates    []string
	SuccessClassCeiling   domain.EvidenceClass
	FailureClassCeiling   domain.EvidenceClass
	StrongConflictCeiling domain.EvidenceClass
}

type Result struct {
	State              domain.OutcomeState
	PromotionEligible  bool
	PolicyVersion      string
	StrongestSatisfied domain.EvidenceClass
	StrongestFailed    domain.EvidenceClass
	ConflictPredicates []string
	MissingPredicates  []string
	Reasons            []string
}

func Evaluate(policy Policy, facts []domain.OutcomeEvidence, superseded map[domain.EvidenceID]struct{}) (Result, error) {
	if err := validatePolicy(policy); err != nil {
		return Result{}, err
	}
	result := Result{PolicyVersion: policy.Version}
	active := make([]domain.OutcomeEvidence, 0, len(facts))
	seen := make(map[domain.EvidenceID]struct{}, len(facts))
	for _, fact := range facts {
		if fact.ID == "" || fact.PredicateID == "" || classRank(fact.Class) == 0 || !validVerdict(fact.Verdict) {
			return Result{}, fmt.Errorf("invalid evidence %q", fact.ID)
		}
		if _, duplicate := seen[fact.ID]; duplicate {
			return Result{}, fmt.Errorf("duplicate evidence %q", fact.ID)
		}
		seen[fact.ID] = struct{}{}
		if _, inactive := superseded[fact.ID]; !inactive {
			active = append(active, fact)
		}
	}

	result.StrongestSatisfied = strongest(active, domain.EvidenceVerdictSatisfied)
	result.StrongestFailed = strongest(active, domain.EvidenceVerdictFailed)
	byPredicate := make(map[string][]domain.OutcomeEvidence)
	for _, fact := range active {
		byPredicate[fact.PredicateID] = append(byPredicate[fact.PredicateID], fact)
	}
	for predicate, grouped := range byPredicate {
		satisfied, failed := strongestRanks(grouped)
		if satisfied != 0 && satisfied == failed && satisfied <= classRank(policy.StrongConflictCeiling) {
			result.ConflictPredicates = append(result.ConflictPredicates, predicate)
		}
	}
	sort.Strings(result.ConflictPredicates)
	if len(result.ConflictPredicates) > 0 {
		result.State = domain.OutcomeStateInconclusive
		result.Reasons = append(result.Reasons, "conflicting strong evidence")
		return result, nil
	}

	allVerified := true
	anySatisfied := false
	verifiedFailure := false
	for _, predicate := range canonicalPredicates(policy.RequiredPredicates) {
		satisfied, failed := strongestRanks(byPredicate[predicate])
		if satisfied != 0 {
			anySatisfied = true
		}
		if failed != 0 && (satisfied == 0 || failed < satisfied) && failed <= classRank(policy.FailureClassCeiling) {
			verifiedFailure = true
		}
		if satisfied == 0 || (failed != 0 && failed <= satisfied) || satisfied > classRank(policy.SuccessClassCeiling) {
			allVerified = false
			if satisfied == 0 && failed == 0 {
				result.MissingPredicates = append(result.MissingPredicates, predicate)
			}
		}
	}
	if verifiedFailure {
		result.State = domain.OutcomeStateVerifiedFailure
		result.Reasons = append(result.Reasons, "required predicate failed with qualifying evidence")
		return result, nil
	}
	if allVerified {
		result.State = domain.OutcomeStateVerifiedSuccess
		result.PromotionEligible = true
		return result, nil
	}
	if anySatisfied || result.StrongestSatisfied != "" {
		result.State = domain.OutcomeStateProvisionalSuccess
		result.Reasons = append(result.Reasons, "goal proof is incomplete")
		return result, nil
	}
	result.State = domain.OutcomeStateInconclusive
	result.Reasons = append(result.Reasons, "evidence is missing or below verification threshold")
	return result, nil
}

func validatePolicy(policy Policy) error {
	if strings.TrimSpace(policy.Version) == "" || len(canonicalPredicates(policy.RequiredPredicates)) == 0 ||
		classRank(policy.SuccessClassCeiling) == 0 || classRank(policy.FailureClassCeiling) == 0 ||
		classRank(policy.StrongConflictCeiling) == 0 {
		return ErrInvalidPolicy
	}
	return nil
}

func canonicalPredicates(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; !exists {
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func strongest(facts []domain.OutcomeEvidence, verdict domain.EvidenceVerdict) domain.EvidenceClass {
	best := 0
	var class domain.EvidenceClass
	for _, fact := range facts {
		if fact.Verdict == verdict && (best == 0 || classRank(fact.Class) < best) {
			best = classRank(fact.Class)
			class = fact.Class
		}
	}
	return class
}

func strongestRanks(facts []domain.OutcomeEvidence) (satisfied, failed int) {
	for _, fact := range facts {
		rank := classRank(fact.Class)
		switch fact.Verdict {
		case domain.EvidenceVerdictSatisfied:
			if satisfied == 0 || rank < satisfied {
				satisfied = rank
			}
		case domain.EvidenceVerdictFailed:
			if failed == 0 || rank < failed {
				failed = rank
			}
		}
	}
	return satisfied, failed
}

func validVerdict(value domain.EvidenceVerdict) bool {
	return value == domain.EvidenceVerdictSatisfied || value == domain.EvidenceVerdictFailed || value == domain.EvidenceVerdictUnknown
}

func classRank(value domain.EvidenceClass) int {
	switch value {
	case domain.EvidenceClassIndependentVerifier:
		return 1
	case domain.EvidenceClassGoalPredicate:
		return 2
	case domain.EvidenceClassHarnessAssertion:
		return 3
	case domain.EvidenceClassToolPostcondition:
		return 4
	case domain.EvidenceClassExitStatusOrSelfReport:
		return 5
	default:
		return 0
	}
}
