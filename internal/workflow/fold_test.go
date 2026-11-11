package workflow

import (
	"testing"

	"github.com/sauhard74/mem-jev/internal/domain"
)

func TestFoldTargetMatchesRecalledProcedure(t *testing.T) {
	plan := RecalledPlan{VersionIDs: []string{"pv_a"}, Steps: map[string][]RecalledStep{"pv_a": {
		{ContractVersionID: "tcv_build", SideEffect: "write"}, {ContractVersionID: "tcv_publish", SideEffect: "external"},
	}}}
	step := func(contract, sideEffect string) domain.ProcedureStep {
		return domain.ProcedureStep{ToolContractVersionID: contract, SideEffect: sideEffect}
	}
	for name, test := range map[string]struct {
		plan  RecalledPlan
		steps []domain.ProcedureStep
		want  bool
	}{
		"same steps":             {plan, []domain.ProcedureStep{step("tcv_build", "write"), step("tcv_publish", "external")}, true},
		"plus a person check":    {plan, []domain.ProcedureStep{step("tcv_build", "write"), step("tcv_publish", "external"), step("tcv_person_check", "none")}, true},
		"plus a read in between": {plan, []domain.ProcedureStep{step("tcv_build", "write"), step("tcv_status", "read"), step("tcv_publish", "external")}, true},
		"extra write":            {plan, []domain.ProcedureStep{step("tcv_build", "write"), step("tcv_publish", "external"), step("tcv_delete", "write")}, false},
		"missing a step":         {plan, []domain.ProcedureStep{step("tcv_build", "write")}, false},
		"different order":        {plan, []domain.ProcedureStep{step("tcv_publish", "external"), step("tcv_build", "write")}, false},
		"plan of two procedures": {RecalledPlan{VersionIDs: []string{"pv_a", "pv_b"}, Steps: plan.Steps}, []domain.ProcedureStep{step("tcv_build", "write"), step("tcv_publish", "external")}, false},
		"recalled steps unknown": {RecalledPlan{VersionIDs: []string{"pv_a"}}, []domain.ProcedureStep{step("tcv_build", "write")}, false},
	} {
		t.Run(name, func(t *testing.T) {
			versionID, fold := foldTarget(test.plan, test.steps)
			if fold != test.want || fold && versionID != "pv_a" {
				t.Fatalf("foldTarget = %q, %v; want fold %v", versionID, fold, test.want)
			}
		})
	}
}
