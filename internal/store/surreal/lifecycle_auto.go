package surreal

import (
	"context"
	"errors"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/lifecycle"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// LifecycleScopes lists every tenant scope (including per-person scopes)
// that has published procedures.
func (r *LifecycleRepository) LifecycleScopes(ctx context.Context) ([]domain.TenantID, error) {
	if r == nil || r.db == nil {
		return nil, lifecycle.ErrLifecycleNotFound
	}
	rows, err := surrealdb.Query[[]struct {
		TenantID string `json:"tenant_id"`
	}](ctx, r.db, `SELECT tenant_id FROM retrieval_document GROUP BY tenant_id`, nil)
	if err != nil {
		return nil, databaseFailure("list lifecycle scopes", err)
	}
	result := []domain.TenantID{}
	if rows != nil && len(*rows) > 0 {
		for _, row := range (*rows)[0].Result {
			result = append(result, domain.TenantID(row.TenantID))
		}
	}
	return result, nil
}

// EvaluateScope evaluates the lifecycle of every candidate, trial, and active
// procedure in one tenant scope under manifest, using only that scope's
// causal outcome credit, and commits a decision when the state changes or
// the evidence changed since the last decision. Procedures that were never
// tried (no causal outcomes, no earlier decision) are skipped. It returns the
// committed decisions.
func (r *LifecycleRepository) EvaluateScope(ctx context.Context, tenantID domain.TenantID, manifest lifecycle.PolicyManifest, now time.Time) ([]lifecycle.Decision, error) {
	if r == nil || r.db == nil || tenantID == "" || lifecycle.ValidatePolicyManifest(manifest) != nil {
		return nil, lifecycle.ErrInvalidLifecycle
	}
	cutoff := now.UTC().Truncate(time.Second)
	rows, err := surrealdb.Query[[]struct {
		VersionID string `json:"procedure_version_id"`
	}](ctx, r.db, `SELECT procedure_version_id FROM retrieval_document WHERE tenant_id = $tenant_id GROUP BY procedure_version_id`, map[string]any{"tenant_id": string(tenantID)})
	if err != nil {
		return nil, databaseFailure("list lifecycle versions", err)
	}
	decisions := []lifecycle.Decision{}
	if rows == nil || len(*rows) == 0 {
		return decisions, nil
	}
	for _, row := range (*rows)[0].Result {
		decision, committed, err := r.evaluateVersion(ctx, tenantID, row.VersionID, manifest, cutoff)
		if err != nil {
			return decisions, err
		}
		if committed {
			decisions = append(decisions, decision)
		}
	}
	return decisions, nil
}

func (r *LifecycleRepository) evaluateVersion(ctx context.Context, tenantID domain.TenantID, versionID string, manifest lifecycle.PolicyManifest, cutoff time.Time) (lifecycle.Decision, bool, error) {
	document, epoch, err := currentLifecycleDocument(ctx, r.db, tenantID, versionID)
	if err != nil {
		return lifecycle.Decision{}, false, databaseFailure("read lifecycle document", err)
	}
	prior := lifecycle.State(document.Lifecycle)
	if prior != lifecycle.Candidate && prior != lifecycle.Trial && prior != lifecycle.Active {
		return lifecycle.Decision{}, false, nil
	}
	evidence, err := r.causalEvidence(ctx, tenantID, versionID, cutoff)
	if err != nil {
		return lifecycle.Decision{}, false, err
	}
	// Unsafe outcomes recorded on the procedure always count: the policy
	// quarantines at the first one and never promotes past the ceiling.
	evidence.UnsafeOutcomeCount = document.UnsafeOutcomeCount
	head, headFound, err := findVersionHead(ctx, r.db, tenantID, versionID, manifest.ID)
	if err != nil {
		return lifecycle.Decision{}, false, databaseFailure("read lifecycle version head", err)
	}
	samples := evidence.CausalSuccessCount + evidence.CausalFailureCount
	if !headFound && samples == 0 {
		return lifecycle.Decision{}, false, nil
	}
	if prior == lifecycle.Trial {
		if evidence.TrialStartedAt, err = r.trialStartedAt(ctx, tenantID, versionID, manifest.ID, cutoff); err != nil {
			return lifecycle.Decision{}, false, err
		}
	}
	decision, err := lifecycle.Evaluate(lifecycle.EvaluationRequest{
		TenantID: tenantID, ProcedureID: document.ProcedureID, ProcedureVersionID: versionID, PriorState: prior,
		Manifest: manifest, Evidence: evidence, EvidenceCutoffAt: cutoff, EvaluatedAt: cutoff,
	})
	if err != nil {
		return lifecycle.Decision{}, false, err
	}
	if headFound && decision.NextState == prior {
		last, err := r.decisionCounts(ctx, tenantID, head.DecisionID)
		if err != nil {
			return lifecycle.Decision{}, false, err
		}
		if last == [2]uint64{evidence.CausalSuccessCount, evidence.CausalFailureCount} {
			return lifecycle.Decision{}, false, nil
		}
	}
	_, err = r.Commit(ctx, lifecycle.Publication{Manifest: manifest, Decision: decision, SourceDocument: document, SourceEpoch: epoch})
	if errors.Is(err, lifecycle.ErrLifecycleConflict) || errors.Is(err, lifecycle.ErrChampionConflict) {
		// Another evaluation or publish moved the procedure; the next pass
		// re-reads it.
		return lifecycle.Decision{}, false, nil
	}
	if err != nil {
		return lifecycle.Decision{}, false, err
	}
	return decision, true, nil
}

// causalEvidence counts the scope's causal outcome credit for one version up
// to cutoff. Only verified outcomes of runs that followed the plan are
// causal; provisional or unattributed outcomes are never counted.
func (r *LifecycleRepository) causalEvidence(ctx context.Context, tenantID domain.TenantID, versionID string, cutoff time.Time) (lifecycle.Evidence, error) {
	rows, err := surrealdb.Query[[]struct {
		Class     string                `json:"credit_class"`
		CreatedAt models.CustomDateTime `json:"created_at"`
	}](ctx, r.db, `SELECT credit_class, created_at FROM outcome_credit WHERE tenant_id = $tenant_id AND procedure_version_id = $version_id AND credit_class IN ["causal_success", "causal_failure"] AND created_at <= $cutoff`,
		map[string]any{"tenant_id": string(tenantID), "version_id": versionID, "cutoff": models.CustomDateTime{Time: cutoff}})
	if err != nil {
		return lifecycle.Evidence{}, databaseFailure("read causal outcome credit", err)
	}
	evidence := lifecycle.Evidence{}
	if rows == nil || len(*rows) == 0 {
		return evidence, nil
	}
	for _, row := range (*rows)[0].Result {
		if row.Class == "causal_success" {
			evidence.CausalSuccessCount++
		} else {
			evidence.CausalFailureCount++
		}
		if created := row.CreatedAt.UTC(); created.After(evidence.LatestCausalAt) {
			evidence.LatestCausalAt = created
		}
	}
	return evidence, nil
}

func (r *LifecycleRepository) trialStartedAt(ctx context.Context, tenantID domain.TenantID, versionID, manifestID string, cutoff time.Time) (time.Time, error) {
	rows, err := surrealdb.Query[[]struct {
		CreatedAt models.CustomDateTime `json:"created_at"`
	}](ctx, r.db, `SELECT created_at FROM lifecycle_decision WHERE tenant_id = $tenant_id AND procedure_version_id = $version_id AND lifecycle_policy_manifest_id = $manifest_id AND next_state = "trial" AND prior_state != "trial" ORDER BY created_at DESC LIMIT 1`,
		map[string]any{"tenant_id": string(tenantID), "version_id": versionID, "manifest_id": manifestID})
	if err != nil {
		return time.Time{}, databaseFailure("read trial start", err)
	}
	if rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return time.Time{}, nil
	}
	started := (*rows)[0].Result[0].CreatedAt.UTC()
	if started.After(cutoff) {
		started = cutoff
	}
	return started, nil
}

func (r *LifecycleRepository) decisionCounts(ctx context.Context, tenantID domain.TenantID, decisionID string) ([2]uint64, error) {
	rows, err := surrealdb.Query[[]struct {
		Successes uint64 `json:"causal_success_count"`
		Failures  uint64 `json:"causal_failure_count"`
	}](ctx, r.db, `SELECT causal_success_count, causal_failure_count FROM lifecycle_decision WHERE tenant_id = $tenant_id AND lifecycle_decision_id = $decision_id LIMIT 1`,
		map[string]any{"tenant_id": string(tenantID), "decision_id": decisionID})
	if err != nil {
		return [2]uint64{}, databaseFailure("read lifecycle decision", err)
	}
	if rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return [2]uint64{^uint64(0), ^uint64(0)}, nil
	}
	return [2]uint64{(*rows)[0].Result[0].Successes, (*rows)[0].Result[0].Failures}, nil
}
