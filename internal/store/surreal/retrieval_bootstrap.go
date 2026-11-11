package surreal

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

// EnsureDefaultServing activates memJev's default serving configuration for a
// tenant. It does nothing when the tenant already serves an operator-published
// configuration. It replaces the active configuration only when that one is an
// earlier memJev default (for example, the no-Jev ranker after Jev is enabled).
//
// indexes lists the channels the API serves. A vector index is included only
// when its manifest was already provisioned for the tenant; other index
// manifests are created when missing. It reports whether it activated a config.
func (r *RetrievalRunRepository) EnsureDefaultServing(ctx context.Context, tenantID domain.TenantID, defaults retrieval.DefaultServing, indexes []retrieval.SnapshotIndex) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if r == nil || r.db == nil || tenantID == "" || len(indexes) == 0 {
		return false, store.ErrServingConfigUnavailable
	}
	// A recall must not create state for a scope that has learned nothing
	// (for example an unknown person): activate only once there is something
	// to serve.
	learned, err := hasLearned(ctx, r.db, tenantID)
	if err != nil {
		return false, databaseFailure("check learned procedures", err)
	}
	if !learned {
		return false, retrieval.ErrNothingLearned
	}
	now := time.Now().UTC()
	served := make([]retrieval.SnapshotIndex, 0, len(indexes))
	for _, index := range indexes {
		exists, err := r.manifestExists(ctx, "retrieval_index_manifest", "index_manifest_id", tenantID, index.ManifestID)
		if err != nil {
			return false, err
		}
		if index.Channel == retrieval.ChannelVector && !exists {
			continue
		}
		served = append(served, index)
	}
	config, err := retrieval.BuildServingConfig(tenantID, defaults.Policy.ID, defaults.Ranker.ID, served)
	if err != nil {
		return false, err
	}
	expectedHead, replaceable, err := r.defaultServingReplaceable(ctx, tenantID, config)
	if err != nil || !replaceable {
		return false, err
	}
	if err = r.ensureManifest(ctx, "eligibility_policy_manifest", "policy_manifest_id", tenantID, defaults.Policy.ID, map[string]any{
		"tenant_id": string(tenantID), "policy_manifest_id": defaults.Policy.ID, "version": defaults.Policy.Version, "manifest": string(defaults.Policy.CanonicalJSON),
		"created_at": now, "schema_version": "eligibility-policy.v1", "content_hash": strings.TrimPrefix(defaults.Policy.ID, "egp_"),
	}); err != nil {
		return false, err
	}
	if err = r.ensureManifest(ctx, "ranker_manifest", "ranker_manifest_id", tenantID, defaults.Ranker.ID, map[string]any{
		"tenant_id": string(tenantID), "ranker_manifest_id": defaults.Ranker.ID, "version": defaults.Ranker.Version, "manifest": string(defaults.Ranker.CanonicalJSON),
		"created_at": now, "schema_version": "ranker.v1", "content_hash": strings.TrimPrefix(defaults.Ranker.ID, "rnk_"),
	}); err != nil {
		return false, err
	}
	for _, index := range served {
		if index.Channel == retrieval.ChannelVector {
			continue
		}
		if err = r.ensureIndexManifest(ctx, tenantID, index, now); err != nil {
			return false, err
		}
	}
	// Activate only if the head is still what was judged replaceable above, so
	// an operator activation that lands in between is never overwritten.
	if err = r.activateServingConfig(ctx, config, &expectedHead); err != nil {
		if errors.Is(err, errServingHeadChanged) {
			return false, nil
		}
		// A concurrent request may have activated the same config first.
		if active, found, readErr := r.activeServingConfig(ctx, tenantID); readErr == nil && found && active.ID == config.ID {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// defaultServingReplaceable reports whether desired should become the active
// config: true when no config is active or the active one is an older default.
// It also returns the active config ID ("" for none) for the conditional switch.
func (r *RetrievalRunRepository) defaultServingReplaceable(ctx context.Context, tenantID domain.TenantID, desired retrieval.ServingConfig) (string, bool, error) {
	active, found, err := r.activeServingConfig(ctx, tenantID)
	if err != nil {
		return "", false, err
	}
	if !found {
		return "", true, nil
	}
	if active.ID == desired.ID {
		return active.ID, false, nil
	}
	policyVersion, err := r.manifestVersion(ctx, "eligibility_policy_manifest", "policy_manifest_id", tenantID, active.PolicyManifestID)
	if err != nil {
		return "", false, err
	}
	rankerVersion, err := r.manifestVersion(ctx, "ranker_manifest", "ranker_manifest_id", tenantID, active.RankerManifestID)
	if err != nil {
		return "", false, err
	}
	return active.ID, retrieval.IsDefaultManifestVersion(policyVersion) && retrieval.IsDefaultManifestVersion(rankerVersion), nil
}

func (r *RetrievalRunRepository) activeServingConfig(ctx context.Context, tenantID domain.TenantID) (retrieval.ServingConfig, bool, error) {
	heads, err := surrealdb.Query[[]struct {
		ConfigID string `json:"serving_config_id"`
	}](ctx, r.db, `SELECT serving_config_id FROM retrieval_serving_head WHERE tenant_id = $tenant_id LIMIT 1`, map[string]any{"tenant_id": string(tenantID)})
	if err != nil {
		return retrieval.ServingConfig{}, false, databaseFailure("read serving head", err)
	}
	if heads == nil || len(*heads) == 0 || len((*heads)[0].Result) == 0 {
		return retrieval.ServingConfig{}, false, nil
	}
	return readServingConfig(ctx, r.db, tenantID, (*heads)[0].Result[0].ConfigID)
}

func (r *RetrievalRunRepository) manifestExists(ctx context.Context, table, field string, tenantID domain.TenantID, id string) (bool, error) {
	_, found, err := r.manifestHash(ctx, table, field, tenantID, id)
	return found, err
}

func (r *RetrievalRunRepository) manifestVersion(ctx context.Context, table, field string, tenantID domain.TenantID, id string) (string, error) {
	rows, err := surrealdb.Query[[]struct {
		Version string `json:"version"`
	}](ctx, r.db, "SELECT version FROM "+table+" WHERE tenant_id = $tenant_id AND "+field+" = $id LIMIT 1", map[string]any{"tenant_id": string(tenantID), "id": id})
	if err != nil {
		return "", databaseFailure("read manifest version", err)
	}
	if rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return "", nil
	}
	return (*rows)[0].Result[0].Version, nil
}

func (r *RetrievalRunRepository) manifestHash(ctx context.Context, table, field string, tenantID domain.TenantID, id string) (string, bool, error) {
	if !identifierPattern.MatchString(table) || !identifierPattern.MatchString(field) {
		return "", false, errors.New("invalid manifest table")
	}
	rows, err := surrealdb.Query[[]struct {
		Hash string `json:"content_hash"`
	}](ctx, r.db, "SELECT content_hash FROM "+table+" WHERE tenant_id = $tenant_id AND "+field+" = $id LIMIT 1", map[string]any{"tenant_id": string(tenantID), "id": id})
	if err != nil {
		return "", false, databaseFailure("read manifest", err)
	}
	if rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return "", false, nil
	}
	return (*rows)[0].Result[0].Hash, true, nil
}

// ensureManifest creates an immutable manifest record unless an identical one exists.
func (r *RetrievalRunRepository) ensureManifest(ctx context.Context, table, field string, tenantID domain.TenantID, id string, record map[string]any) error {
	want, _ := record["content_hash"].(string)
	hash, found, err := r.manifestHash(ctx, table, field, tenantID, id)
	if err != nil {
		return err
	}
	if !found {
		_, createErr := surrealdb.Query[any](ctx, r.db, "CREATE ONLY "+table+" CONTENT $record", map[string]any{"record": record})
		if hash, found, err = r.manifestHash(ctx, table, field, tenantID, id); err != nil {
			return err
		}
		if !found {
			if createErr != nil {
				return databaseFailure("create manifest", createErr)
			}
			return store.ErrServingConfigUnavailable
		}
	}
	if hash != want {
		return store.ErrRetrievalRunConflict
	}
	return nil
}

func (r *RetrievalRunRepository) ensureIndexManifest(ctx context.Context, tenantID domain.TenantID, index retrieval.SnapshotIndex, now time.Time) error {
	exists, err := r.manifestExists(ctx, "retrieval_index_manifest", "index_manifest_id", tenantID, index.ManifestID)
	if err != nil || exists {
		return err
	}
	rows, err := surrealdb.Query[[]struct {
		Generation int64 `json:"generation"`
	}](ctx, r.db, `SELECT generation FROM retrieval_index_manifest WHERE tenant_id = $tenant_id AND channel = $channel ORDER BY generation DESC LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "channel": string(index.Channel)})
	if err != nil {
		return databaseFailure("read index generation", err)
	}
	generation := int64(1)
	if rows != nil && len(*rows) > 0 && len((*rows)[0].Result) > 0 {
		generation = (*rows)[0].Result[0].Generation + 1
	}
	indexName := "retrieval_document_" + string(index.Channel)
	_, hash, err := canonical.MarshalAndHash(map[string]any{"tenant_id": string(tenantID), "index_manifest_id": index.ManifestID, "channel": string(index.Channel), "index_name": indexName, "approximate": index.Approximate})
	if err != nil {
		return err
	}
	return r.ensureManifest(ctx, "retrieval_index_manifest", "index_manifest_id", tenantID, index.ManifestID, map[string]any{
		"tenant_id": string(tenantID), "index_manifest_id": index.ManifestID, "channel": string(index.Channel), "generation": generation,
		"index_name": indexName, "approximate": index.Approximate, "configuration": map[string]any{}, "created_at": now,
		"schema_version": "retrieval-index.v1", "content_hash": hash,
	})
}
