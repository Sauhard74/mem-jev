package surreal

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/composition"
	"github.com/sauhard74/mem-jev/internal/domain"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type CompositionRepository struct {
	db  *surrealdb.DB
	now func() time.Time
}

func NewCompositionRepository(db *surrealdb.DB) *CompositionRepository {
	return &CompositionRepository{db: db, now: time.Now}
}

func (r *CompositionRepository) Publish(ctx context.Context, graph composition.CompatibilityGraph) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.db == nil {
		return errors.New("SurrealDB composition repository is not configured")
	}
	if composition.ValidateCompatibilityGraph(graph) != nil {
		return composition.ErrInvalidCompatibilityInput
	}
	const attempts = 8
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		err := r.publishOnce(ctx, graph)
		if err == nil {
			return nil
		}
		lastErr = err
		if hash, found, lookupErr := findCompatibilityGraphHash(ctx, r.db, graph.TenantID, graph.ProjectionEpoch, graph.PlannerManifestID, graph.PolicyManifestID); lookupErr != nil {
			return databaseFailure("resolve compatibility graph publication", lookupErr)
		} else if found {
			if hash != graph.ContentHash {
				return composition.ErrCompositionConflict
			}
			return nil
		}
		if !surrealdb.IsTransactionConflict(err) {
			return databaseFailure("publish compatibility graph", err)
		}
		if err := waitForRetry(ctx, attempt); err != nil {
			return err
		}
	}
	return databaseFailure("publish compatibility graph after conflict retries", lastErr)
}

func (r *CompositionRepository) publishOnce(ctx context.Context, graph composition.CompatibilityGraph) (_ error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	if hash, found, err := findCompatibilityGraphHash(ctx, tx, graph.TenantID, graph.ProjectionEpoch, graph.PlannerManifestID, graph.PolicyManifestID); err != nil {
		return err
	} else if found {
		_ = tx.Cancel(ctx)
		if hash != graph.ContentHash {
			return composition.ErrCompositionConflict
		}
		return nil
	}
	createdAt := r.now().UTC()
	header := map[string]any{"tenant_id": string(graph.TenantID), "compatibility_graph_id": graph.ID, "projection_epoch": graph.ProjectionEpoch, "planner_manifest_id": graph.PlannerManifestID, "policy_manifest_id": graph.PolicyManifestID, "matrix_hash": graph.MatrixHash, "candidate_set_hash": graph.CandidateSetHash, "edge_count": len(graph.Edges), "canonical_graph": string(graph.CanonicalJSON), "created_at": createdAt, "schema_version": graph.SchemaVersion, "content_hash": graph.ContentHash}
	if err := createRecord(ctx, tx, models.NewRecordID("compatibility_graph_projection", graph.ID), header); err != nil {
		return err
	}
	for _, edge := range graph.Edges {
		canonicalEdge, err := canonicalEdgeJSON(edge)
		if err != nil {
			return err
		}
		record := map[string]any{"tenant_id": string(edge.TenantID), "compatibility_edge_id": edge.ID, "projection_epoch": edge.ProjectionEpoch, "source_procedure_version_id": edge.SourceVersionID, "target_procedure_version_id": edge.TargetVersionID, "source_interface_hash": edge.SourceInterfaceHash, "target_interface_hash": edge.TargetInterfaceHash, "source_provision_ids": edge.SourceProvisionIDs, "satisfied_requirement_ids": edge.SatisfiedRequirementIDs, "planner_manifest_id": edge.PlannerManifestID, "policy_manifest_id": edge.PolicyManifestID, "canonical_edge": string(canonicalEdge), "created_at": createdAt, "schema_version": edge.SchemaVersion, "content_hash": edge.ContentHash}
		if err := createRecord(ctx, tx, models.NewRecordID("procedure_compatibility_edge", edge.ID), record); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *CompositionRepository) Edges(ctx context.Context, tenantID domain.TenantID, epoch uint64, plannerManifestID, policyManifestID string) ([]composition.CompatibilityEdge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil || tenantID == "" || epoch == 0 || plannerManifestID == "" || policyManifestID == "" {
		return nil, composition.ErrInvalidCompatibilityInput
	}
	type row struct {
		ID        string `json:"compatibility_graph_id"`
		Hash      string `json:"content_hash"`
		Canonical string `json:"canonical_graph"`
	}
	rows, err := surrealdb.Query[[]row](ctx, r.db, `SELECT compatibility_graph_id, content_hash, canonical_graph FROM compatibility_graph_projection WHERE tenant_id = $tenant_id AND projection_epoch = $epoch AND planner_manifest_id = $planner_id AND policy_manifest_id = $policy_id LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "epoch": epoch, "planner_id": plannerManifestID, "policy_id": policyManifestID})
	if err != nil {
		return nil, databaseFailure("read compatibility graph", err)
	}
	if rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return nil, nil
	}
	stored := (*rows)[0].Result[0]
	var graph composition.CompatibilityGraph
	if err := json.Unmarshal([]byte(stored.Canonical), &graph); err != nil {
		return nil, databaseFailure("decode compatibility graph", err)
	}
	graph.ID, graph.ContentHash, graph.CanonicalJSON = stored.ID, stored.Hash, []byte(stored.Canonical)
	if composition.ValidateCompatibilityGraph(graph) != nil {
		return nil, composition.ErrCompositionConflict
	}
	edges := append([]composition.CompatibilityEdge(nil), graph.Edges...)
	for index := range edges {
		edges[index].SourceProvisionIDs = append([]string(nil), graph.Edges[index].SourceProvisionIDs...)
		edges[index].SatisfiedRequirementIDs = append([]string(nil), graph.Edges[index].SatisfiedRequirementIDs...)
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].ID < edges[j].ID })
	return edges, nil
}

func findCompatibilityGraphHash[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, tenantID domain.TenantID, epoch uint64, plannerID, policyID string) (string, bool, error) {
	type row struct {
		Hash string `json:"content_hash"`
	}
	rows, err := surrealdb.Query[[]row](ctx, sender, `SELECT content_hash FROM compatibility_graph_projection WHERE tenant_id = $tenant_id AND projection_epoch = $epoch AND planner_manifest_id = $planner_id AND policy_manifest_id = $policy_id LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "epoch": epoch, "planner_id": plannerID, "policy_id": policyID})
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return "", false, err
	}
	return (*rows)[0].Result[0].Hash, true, nil
}

func canonicalEdgeJSON(edge composition.CompatibilityEdge) ([]byte, error) {
	encoded, _, err := canonical.MarshalAndHash(edge)
	return encoded, err
}
