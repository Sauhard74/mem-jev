package surreal

import (
	"context"
	"sort"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	memworkflow "github.com/sauhard74/mem-jev/internal/workflow"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

// StepRepository reads the ordered steps of stored procedures for plans.
type StepRepository struct {
	db *surrealdb.DB
}

func NewStepRepository(db *surrealdb.DB) *StepRepository {
	return &StepRepository{db: db}
}

// ProcedureSteps returns each version's steps in order, with the side effect
// and risk declared by the step's tool contract. A version with no stored
// steps is absent from the result.
func (r *StepRepository) ProcedureSteps(ctx context.Context, tenantID string, versionIDs []string) (map[string][]retrieval.PlanStep, error) {
	if r == nil || r.db == nil || tenantID == "" {
		return nil, retrieval.ErrStepsUnavailable
	}
	result := make(map[string][]retrieval.PlanStep, len(versionIDs))
	if len(versionIDs) == 0 {
		return result, nil
	}
	type stepRow struct {
		VersionID  string         `json:"procedure_version_id"`
		Ordinal    uint32         `json:"ordinal"`
		ContractID *string        `json:"tool_contract_version_id"`
		Payload    map[string]any `json:"payload"`
	}
	rows, err := surrealdb.Query[[]stepRow](ctx, r.db, `SELECT procedure_version_id, ordinal, tool_contract_version_id, payload FROM step
		WHERE tenant_id = $tenant_id AND procedure_version_id IN $version_ids`, map[string]any{"tenant_id": tenantID, "version_ids": versionIDs})
	if err != nil {
		return nil, databaseFailure("read procedure steps", err)
	}
	contractIDs := make([]string, 0)
	seen := make(map[string]bool)
	if rows != nil && len(*rows) > 0 {
		for _, row := range (*rows)[0].Result {
			step := retrieval.PlanStep{Ordinal: row.Ordinal}
			if row.ContractID != nil {
				step.ContractVersionID = *row.ContractID
				if !seen[step.ContractVersionID] {
					seen[step.ContractVersionID] = true
					contractIDs = append(contractIDs, step.ContractVersionID)
				}
			}
			step.ToolName, _ = row.Payload["tool_name"].(string)
			step.ToolVersion, _ = row.Payload["tool_version"].(string)
			result[row.VersionID] = append(result[row.VersionID], step)
		}
	}
	if len(contractIDs) > 0 {
		type contractRow struct {
			ContractID string `json:"contract_version_id"`
			SideEffect string `json:"side_effect"`
			Risk       string `json:"risk"`
		}
		contracts, err := surrealdb.Query[[]contractRow](ctx, r.db, `SELECT contract_version_id, manifest.side_effect AS side_effect, manifest.risk AS risk FROM tool_contract_version
			WHERE tenant_id IN $tenant_ids AND contract_version_id IN $contract_ids`, map[string]any{"tenant_ids": contractTenants(domain.TenantID(tenantID)), "contract_ids": contractIDs})
		if err != nil {
			return nil, databaseFailure("read step contracts", err)
		}
		byID := make(map[string]contractRow)
		if contracts != nil && len(*contracts) > 0 {
			for _, row := range (*contracts)[0].Result {
				byID[row.ContractID] = row
			}
		}
		for versionID, steps := range result {
			for index := range steps {
				contract := byID[steps[index].ContractVersionID]
				steps[index].SideEffect, steps[index].Risk = contract.SideEffect, contract.Risk
			}
			result[versionID] = steps
		}
	}
	for versionID, steps := range result {
		sort.Slice(steps, func(i, j int) bool { return steps[i].Ordinal < steps[j].Ordinal })
		result[versionID] = steps
	}
	return result, nil
}

var _ retrieval.StepReader = (*StepRepository)(nil)

// RecalledPlan returns the procedure versions of the plan an injection handed
// out, in plan order, with each version's ordered steps.
func (r *StepRepository) RecalledPlan(ctx context.Context, tenantID domain.TenantID, injectionID string) (memworkflow.RecalledPlan, bool, error) {
	if r == nil || r.db == nil || tenantID == "" || injectionID == "" {
		return memworkflow.RecalledPlan{}, false, retrieval.ErrStepsUnavailable
	}
	rows, err := surrealdb.Query[[]struct {
		VersionID string `json:"procedure_version_id"`
		Ordinal   uint32 `json:"ordinal"`
	}](ctx, r.db, `SELECT procedure_version_id, ordinal FROM selection_plan_node WHERE tenant_id = $tenant_id AND injection_id = $injection_id ORDER BY ordinal`,
		map[string]any{"tenant_id": string(tenantID), "injection_id": injectionID})
	if err != nil {
		return memworkflow.RecalledPlan{}, false, databaseFailure("read recalled plan", err)
	}
	plan := memworkflow.RecalledPlan{Steps: map[string][]memworkflow.RecalledStep{}}
	if rows != nil && len(*rows) > 0 {
		for _, row := range (*rows)[0].Result {
			plan.VersionIDs = append(plan.VersionIDs, row.VersionID)
		}
	}
	if len(plan.VersionIDs) == 0 {
		return plan, false, nil
	}
	steps, err := r.ProcedureSteps(ctx, string(tenantID), plan.VersionIDs)
	if err != nil {
		return memworkflow.RecalledPlan{}, false, err
	}
	for versionID, versionSteps := range steps {
		for _, step := range versionSteps {
			plan.Steps[versionID] = append(plan.Steps[versionID], memworkflow.RecalledStep{ContractVersionID: step.ContractVersionID, SideEffect: step.SideEffect})
		}
	}
	return plan, true, nil
}
