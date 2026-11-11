package retrieval

import (
	"context"
	"errors"
)

// PlanStep is one tool call of a stored procedure, in execution order. It
// names the tool and its contract version; argument values are never
// returned.
type PlanStep struct {
	Ordinal           uint32
	ToolName          string
	ToolVersion       string
	ContractVersionID string
	// SideEffect and Risk come from the step's tool contract.
	SideEffect string
	Risk       string
}

// StepReader loads the ordered steps of procedure versions in a tenant scope.
type StepReader interface {
	ProcedureSteps(ctx context.Context, tenantID string, versionIDs []string) (map[string][]PlanStep, error)
}

var ErrStepsUnavailable = errors.New("procedure steps unavailable")

// unpromotedPlanAllowed reports whether an unpromoted (candidate) procedure
// may be handed out as a plan: low or medium risk, and no step that is
// irreversible or whose tool contract is high or critical risk.
func unpromotedPlanAllowed(document Document, steps []PlanStep) bool {
	if document.RiskClass != "low" && document.RiskClass != "medium" || len(steps) == 0 {
		return false
	}
	for _, step := range steps {
		if step.SideEffect == "" || step.SideEffect == "irreversible" || step.Risk != "low" && step.Risk != "medium" {
			return false
		}
	}
	return true
}
