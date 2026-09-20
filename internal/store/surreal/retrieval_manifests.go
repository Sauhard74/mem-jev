package surreal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/eligibility"
	"github.com/sauhard74/mem-jev/internal/ranking"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

type RetrievalManifestRepository struct{ db *surrealdb.DB }

func NewRetrievalManifestRepository(db *surrealdb.DB) (*RetrievalManifestRepository, error) {
	if db == nil {
		return nil, errors.New("retrieval manifest repository is not configured")
	}
	return &RetrievalManifestRepository{db: db}, nil
}

func (r *RetrievalManifestRepository) EligibilityPolicy(ctx context.Context, tenantID domain.TenantID, id string) (eligibility.Policy, error) {
	manifest, hash, err := r.read(ctx, "eligibility_policy_manifest", "policy_manifest_id", tenantID, id)
	if err != nil {
		return eligibility.Policy{}, err
	}
	var spec eligibility.PolicySpec
	if err = json.Unmarshal([]byte(manifest), &spec); err != nil {
		return eligibility.Policy{}, storeManifestConflict()
	}
	policy, err := eligibility.NewPolicy(spec)
	if err != nil || policy.ID != id || !strings.HasSuffix(policy.ID, hash) || string(policy.CanonicalJSON) != manifest {
		return eligibility.Policy{}, storeManifestConflict()
	}
	return policy, nil
}

func (r *RetrievalManifestRepository) Ranker(ctx context.Context, tenantID domain.TenantID, id string) (ranking.Manifest, error) {
	manifest, hash, err := r.read(ctx, "ranker_manifest", "ranker_manifest_id", tenantID, id)
	if err != nil {
		return ranking.Manifest{}, err
	}
	var spec ranking.ManifestSpec
	if err = json.Unmarshal([]byte(manifest), &spec); err != nil {
		return ranking.Manifest{}, storeManifestConflict()
	}
	ranker, err := ranking.NewManifest(spec)
	if err != nil || ranker.ID != id || !strings.HasSuffix(ranker.ID, hash) || string(ranker.CanonicalJSON) != manifest {
		return ranking.Manifest{}, storeManifestConflict()
	}
	return ranker, nil
}

func (r *RetrievalManifestRepository) read(ctx context.Context, table, field string, tenantID domain.TenantID, id string) (string, string, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	if r == nil || r.db == nil || tenantID == "" || strings.TrimSpace(id) == "" || !identifierPattern.MatchString(table) || !identifierPattern.MatchString(field) {
		return "", "", storeManifestConflict()
	}
	statement := "SELECT manifest, content_hash FROM " + table + " WHERE tenant_id = $tenant_id AND " + field + " = $id LIMIT 1"
	rows, err := surrealdb.Query[[]struct {
		Manifest string `json:"manifest"`
		Hash     string `json:"content_hash"`
	}](ctx, r.db, statement, map[string]any{"tenant_id": string(tenantID), "id": id})
	if err != nil {
		return "", "", databaseFailure("read retrieval manifest", err)
	}
	if rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return "", "", storeManifestConflict()
	}
	row := (*rows)[0].Result[0]
	return row.Manifest, row.Hash, nil
}

func storeManifestConflict() error { return errors.New("retrieval manifest is missing or invalid") }

var _ retrieval.ManifestResolver = (*RetrievalManifestRepository)(nil)
