package surreal

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type RetrievalRunRepository struct {
	db      *surrealdb.DB
	failure failurePoint
}

func NewRetrievalRunRepository(db *surrealdb.DB) *RetrievalRunRepository {
	return &RetrievalRunRepository{db: db}
}

func newRetrievalRunRepository(db *surrealdb.DB, failure failurePoint) *RetrievalRunRepository {
	return &RetrievalRunRepository{db: db, failure: failure}
}

func (r *RetrievalRunRepository) ActivateServingConfig(ctx context.Context, config retrieval.ServingConfig) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.db == nil || retrieval.ValidateServingConfig(config) != nil {
		return store.ErrServingConfigUnavailable
	}
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return databaseFailure("begin serving config activation", err)
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	if err = verifyServingReferences(ctx, tx, config); err != nil {
		return err
	}
	existing, found, err := readServingConfig(ctx, tx, config.TenantID, config.ID)
	if err != nil {
		return err
	}
	if found {
		if existing.ContentHash != config.ContentHash {
			return store.ErrRetrievalRunConflict
		}
	} else {
		indexIDs := make([]string, len(config.Indexes))
		for index, item := range config.Indexes {
			indexIDs[index] = item.ManifestID
		}
		record := map[string]any{"tenant_id": string(config.TenantID), "serving_config_id": config.ID, "policy_manifest_id": config.PolicyManifestID, "ranker_manifest_id": config.RankerManifestID, "index_manifest_ids": indexIDs, "canonical_config": string(config.CanonicalJSON), "created_at": time.Now().UTC(), "schema_version": config.SchemaVersion, "content_hash": config.ContentHash}
		if err = createRecord(ctx, tx, models.NewRecordID("retrieval_serving_config", config.ID), record); err != nil {
			return err
		}
	}
	headID := models.NewRecordID("retrieval_serving_head", "serving_"+string(config.TenantID))
	type headRow struct {
		ID *models.RecordID `json:"id"`
	}
	heads, err := surrealdb.Query[[]headRow](ctx, tx, `SELECT id FROM $id`, map[string]any{"id": headID})
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if heads == nil || len(*heads) == 0 || len((*heads)[0].Result) == 0 {
		if err = createRecord(ctx, tx, headID, map[string]any{"tenant_id": string(config.TenantID), "serving_config_id": config.ID, "updated_at": now, "schema_version": "retrieval-serving-head.v1", "content_hash": config.ContentHash}); err != nil {
			return err
		}
	} else {
		if _, err = surrealdb.Query[any](ctx, tx, `UPDATE $id SET serving_config_id = $config_id, updated_at = $at, content_hash = $hash`, map[string]any{"id": headID, "config_id": config.ID, "at": now, "hash": config.ContentHash}); err != nil {
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return databaseFailure("commit serving config activation", err)
	}
	return nil
}

func verifyServingReferences(ctx context.Context, tx *surrealdb.Transaction, config retrieval.ServingConfig) error {
	checks := []struct{ table, field, id string }{{"eligibility_policy_manifest", "policy_manifest_id", config.PolicyManifestID}, {"ranker_manifest", "ranker_manifest_id", config.RankerManifestID}}
	for _, item := range config.Indexes {
		checks = append(checks, struct{ table, field, id string }{"retrieval_index_manifest", "index_manifest_id", item.ManifestID})
	}
	for _, check := range checks {
		statement := fmt.Sprintf("SELECT id FROM %s WHERE tenant_id = $tenant_id AND %s = $logical_id LIMIT 1", check.table, check.field)
		rows, err := surrealdb.Query[[]struct {
			ID any `json:"id"`
		}](ctx, tx, statement, map[string]any{"tenant_id": string(config.TenantID), "logical_id": check.id})
		if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
			return store.ErrServingConfigUnavailable
		}
	}
	return nil
}

func readServingConfig[T interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender T, tenantID domain.TenantID, id string) (retrieval.ServingConfig, bool, error) {
	rows, err := surrealdb.Query[[]struct {
		Hash      string `json:"content_hash"`
		Canonical string `json:"canonical_config"`
	}](ctx, sender, `SELECT content_hash, canonical_config FROM retrieval_serving_config WHERE tenant_id = $tenant_id AND serving_config_id = $id LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "id": id})
	if err != nil {
		return retrieval.ServingConfig{}, false, databaseFailure("read serving config", err)
	}
	if rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return retrieval.ServingConfig{}, false, nil
	}
	stored := (*rows)[0].Result[0]
	var config retrieval.ServingConfig
	if err = json.Unmarshal([]byte(stored.Canonical), &config); err != nil {
		return retrieval.ServingConfig{}, false, store.ErrRetrievalRunConflict
	}
	config.ID, config.ContentHash, config.CanonicalJSON = id, stored.Hash, []byte(stored.Canonical)
	if retrieval.ValidateServingConfig(config) != nil {
		return retrieval.ServingConfig{}, false, store.ErrRetrievalRunConflict
	}
	return config, true, nil
}

func (r *RetrievalRunRepository) AcquireServingSnapshot(ctx context.Context, tenantID domain.TenantID) (_ retrieval.ServingSnapshot, err error) {
	if err := ctx.Err(); err != nil {
		return retrieval.ServingSnapshot{}, err
	}
	if r == nil || r.db == nil || tenantID == "" {
		return retrieval.ServingSnapshot{}, store.ErrServingConfigUnavailable
	}
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return retrieval.ServingSnapshot{}, databaseFailure("begin serving snapshot", err)
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	type headRow struct {
		ConfigID string `json:"serving_config_id"`
	}
	heads, err := surrealdb.Query[[]headRow](ctx, tx, `SELECT serving_config_id FROM retrieval_serving_head WHERE tenant_id = $tenant_id LIMIT 1`, map[string]any{"tenant_id": string(tenantID)})
	if err != nil || heads == nil || len(*heads) == 0 || len((*heads)[0].Result) == 0 {
		return retrieval.ServingSnapshot{}, store.ErrServingConfigUnavailable
	}
	config, found, err := readServingConfig(ctx, tx, tenantID, (*heads)[0].Result[0].ConfigID)
	if err != nil || !found {
		return retrieval.ServingSnapshot{}, store.ErrServingConfigUnavailable
	}
	type epochRow struct {
		Epoch uint64 `json:"current_epoch"`
		Hash  string `json:"document_set_hash"`
	}
	epochs, err := surrealdb.Query[[]epochRow](ctx, tx, `SELECT current_epoch, document_set_hash FROM projection_epoch_head WHERE tenant_id = $tenant_id LIMIT 1`, map[string]any{"tenant_id": string(tenantID)})
	if err != nil || epochs == nil || len(*epochs) == 0 || len((*epochs)[0].Result) == 0 {
		return retrieval.ServingSnapshot{}, store.ErrServingConfigUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return retrieval.ServingSnapshot{}, databaseFailure("commit serving snapshot", err)
	}
	epoch := (*epochs)[0].Result[0]
	return retrieval.ServingSnapshot{ProjectionEpoch: epoch.Epoch, DocumentSetHash: epoch.Hash, ServingConfigID: config.ID, PolicyManifestID: config.PolicyManifestID, RankerManifestID: config.RankerManifestID, Indexes: append([]retrieval.SnapshotIndex(nil), config.Indexes...)}, nil
}

func (r *RetrievalRunRepository) SaveRetrievalRun(ctx context.Context, run retrieval.Run) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.db == nil || retrieval.ValidateRun(run) != nil {
		return store.ErrRetrievalRunConflict
	}
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return databaseFailure("begin retrieval run", err)
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	if existing, found, findErr := readRunHash(ctx, tx, run.TenantID, run.ID); findErr != nil {
		return findErr
	} else if found {
		if existing != run.ContentHash {
			return store.ErrRetrievalRunConflict
		}
		_ = tx.Cancel(ctx)
		return nil
	}
	config, found, err := readServingConfig(ctx, tx, run.TenantID, run.Snapshot.ServingConfigID)
	if err != nil || !found || config.PolicyManifestID != run.Snapshot.PolicyManifestID || config.RankerManifestID != run.Snapshot.RankerManifestID || !reflect.DeepEqual(config.Indexes, run.Snapshot.Indexes) {
		return store.ErrRetrievalSnapshotMismatch
	}
	type epochRow struct {
		Hash string `json:"document_set_hash"`
	}
	epochs, err := surrealdb.Query[[]epochRow](ctx, tx, `SELECT document_set_hash FROM projection_epoch WHERE tenant_id = $tenant_id AND epoch = $epoch LIMIT 1`, map[string]any{"tenant_id": string(run.TenantID), "epoch": run.Snapshot.ProjectionEpoch})
	if err != nil || epochs == nil || len(*epochs) == 0 || len((*epochs)[0].Result) == 0 || (*epochs)[0].Result[0].Hash != run.Snapshot.DocumentSetHash {
		return store.ErrRetrievalSnapshotMismatch
	}
	for _, item := range run.ChannelExecutions {
		if err = saveChannelExecution(ctx, tx, run, item); err != nil {
			return err
		}
	}
	for _, item := range run.Hits {
		if err = saveHit(ctx, tx, run, item); err != nil {
			return err
		}
	}
	for _, item := range run.Gates {
		if err = saveGate(ctx, tx, run, item); err != nil {
			return err
		}
	}
	for _, item := range run.Ranked {
		if err = saveRank(ctx, tx, run, item); err != nil {
			return err
		}
	}
	if r.failure == failureAfterRetrievalChildren {
		return errInjectedFailure
	}
	approximate, degraded := []string{}, []string{}
	for _, item := range run.ChannelExecutions {
		if item.Approximate {
			approximate = append(approximate, string(item.Channel))
		}
		if !item.Complete {
			degraded = append(degraded, string(item.Channel)+":"+item.DegradationCode)
		}
	}
	record := map[string]any{
		"tenant_id": string(run.TenantID), "retrieval_run_id": run.ID, "query_hash": run.QueryHash, "canonical_query_envelope": run.QueryEnvelope,
		"projection_epoch": run.Snapshot.ProjectionEpoch, "document_set_hash": run.Snapshot.DocumentSetHash, "serving_config_id": run.Snapshot.ServingConfigID,
		"policy_manifest_id": run.Snapshot.PolicyManifestID, "ranker_manifest_id": run.Snapshot.RankerManifestID, "index_manifest_ids": run.IndexManifestIDs,
		"candidate_version_ids": run.CandidateVersionIDs, "approximate_channels": approximate, "degraded_channels": degraded,
		"disposition": string(run.Disposition), "canonical_run": string(run.CanonicalJSON), "created_at": run.CreatedAt, "completed_at": run.CompletedAt,
		"expires_at": run.ExpiresAt, "schema_version": run.SchemaVersion, "content_hash": run.ContentHash,
	}
	if run.DecisionCode != "" {
		record["decision_code"] = run.DecisionCode
		if run.Disposition == retrieval.RunAbstained {
			record["abstention_code"] = run.DecisionCode
		}
	}
	if len(run.SelectedVersionIDs) > 0 {
		record["selected_version_ids"] = run.SelectedVersionIDs
	}
	if err = createRecord(ctx, tx, models.NewRecordID("retrieval_run", run.ID), record); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return databaseFailure("commit retrieval run", err)
	}
	return nil
}

func childHash(value any) string { _, hash, _ := canonical.MarshalAndHash(value); return hash }

func saveChannelExecution(ctx context.Context, tx *surrealdb.Transaction, run retrieval.Run, item retrieval.ChannelExecution) error {
	hash := childHash(struct {
		Tenant, Run string
		Item        retrieval.ChannelExecution
	}{string(run.TenantID), run.ID, item})
	record := map[string]any{"tenant_id": string(run.TenantID), "retrieval_run_id": run.ID, "channel": string(item.Channel), "index_manifest_id": item.IndexManifestID, "hit_count": item.HitCount, "approximate": item.Approximate, "complete": item.Complete, "latency_micros": item.LatencyMicros, "created_at": run.CompletedAt, "expires_at": run.ExpiresAt, "schema_version": "retrieval-channel-result.v1", "content_hash": hash}
	if item.DegradationCode != "" {
		record["degradation_code"] = item.DegradationCode
	}
	return createRecord(ctx, tx, models.NewRecordID("retrieval_channel_result", "rcr_"+hash), record)
}

func saveHit(ctx context.Context, tx *surrealdb.Transaction, run retrieval.Run, item retrieval.PersistedHit) error {
	hash := childHash(struct {
		Tenant, Run string
		Item        retrieval.PersistedHit
	}{string(run.TenantID), run.ID, item})
	record := map[string]any{"tenant_id": string(run.TenantID), "retrieval_run_id": run.ID, "channel": string(item.Channel), "procedure_version_id": item.VersionID, "channel_rank": item.Rank, "raw_score_quantized": item.RawScoreQuantized, "index_manifest_id": item.IndexManifestID, "approximate": item.Approximate, "created_at": run.CompletedAt, "expires_at": run.ExpiresAt, "schema_version": "retrieval-channel-hit.v1", "content_hash": hash}
	return createRecord(ctx, tx, models.NewRecordID("retrieval_channel_hit", "rch_"+hash), record)
}

func saveGate(ctx context.Context, tx *surrealdb.Transaction, run retrieval.Run, item retrieval.PersistedGate) error {
	hash := childHash(struct {
		Tenant, Run string
		Item        retrieval.PersistedGate
	}{string(run.TenantID), run.ID, item})
	record := map[string]any{"tenant_id": string(run.TenantID), "retrieval_run_id": run.ID, "procedure_version_id": item.VersionID, "eligible": item.Eligible, "advisory_only": item.AdvisoryOnly, "rejection_codes": nonNilStrings(item.RejectionCodes), "canonical_facts": item.CanonicalFacts, "created_at": run.CompletedAt, "expires_at": run.ExpiresAt, "schema_version": "retrieval-gate-decision.v1", "content_hash": hash}
	return createRecord(ctx, tx, models.NewRecordID("retrieval_gate_decision", "rgd_"+hash), record)
}

func saveRank(ctx context.Context, tx *surrealdb.Transaction, run retrieval.Run, item retrieval.PersistedRank) error {
	hash := childHash(struct {
		Tenant, Run string
		Item        retrieval.PersistedRank
	}{string(run.TenantID), run.ID, item})
	features := make(map[string]int32, len(item.Features))
	for _, feature := range item.Features {
		features[feature.Name] = feature.Value
	}
	record := map[string]any{"tenant_id": string(run.TenantID), "retrieval_run_id": run.ID, "procedure_version_id": item.VersionID, "fused_rank_score": item.RRFScore, "final_score": item.FinalScore, "final_rank": item.Rank, "verification_strength": item.VerificationStrength, "observed_end_to_end": item.ObservedEndToEnd, "feature_values": features, "missing_features": nonNilStrings(item.MissingFeatures), "created_at": run.CompletedAt, "expires_at": run.ExpiresAt, "schema_version": "retrieval-ranked-candidate.v1", "content_hash": hash}
	return createRecord(ctx, tx, models.NewRecordID("retrieval_ranked_candidate", "rrc_"+hash), record)
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func readRunHash[T interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender T, tenantID domain.TenantID, id string) (string, bool, error) {
	rows, err := surrealdb.Query[[]struct {
		Hash string `json:"content_hash"`
	}](ctx, sender, `SELECT content_hash FROM retrieval_run WHERE tenant_id = $tenant_id AND retrieval_run_id = $id LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "id": id})
	if err != nil {
		return "", false, databaseFailure("read retrieval run", err)
	}
	if rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return "", false, nil
	}
	return (*rows)[0].Result[0].Hash, true, nil
}

func (r *RetrievalRunRepository) RetrievalRun(ctx context.Context, tenantID domain.TenantID, id string) (retrieval.Run, error) {
	if err := ctx.Err(); err != nil {
		return retrieval.Run{}, err
	}
	if r == nil || r.db == nil || tenantID == "" || strings.TrimSpace(id) == "" {
		return retrieval.Run{}, store.ErrRetrievalRunNotFound
	}
	rows, err := surrealdb.Query[[]struct {
		Hash      string `json:"content_hash"`
		Canonical string `json:"canonical_run"`
	}](ctx, r.db, `SELECT content_hash, canonical_run FROM retrieval_run WHERE tenant_id = $tenant_id AND retrieval_run_id = $id LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "id": id})
	if err != nil {
		return retrieval.Run{}, databaseFailure("read retrieval run", err)
	}
	if rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return retrieval.Run{}, store.ErrRetrievalRunNotFound
	}
	row := (*rows)[0].Result[0]
	return retrieval.DecodeRun([]byte(row.Canonical), row.Hash)
}

func (r *RetrievalRunRepository) ChildCounts(ctx context.Context, runID string) (map[string]int, error) {
	result := make(map[string]int)
	for _, table := range []string{"retrieval_channel_result", "retrieval_channel_hit", "retrieval_gate_decision", "retrieval_ranked_candidate"} {
		rows, err := surrealdb.Query[[]struct {
			Count int `json:"count"`
		}](ctx, r.db, fmt.Sprintf("SELECT count() AS count FROM %s WHERE retrieval_run_id = $id GROUP ALL", table), map[string]any{"id": runID})
		if err != nil {
			return nil, err
		}
		if rows != nil && len(*rows) > 0 && len((*rows)[0].Result) > 0 {
			result[table] = (*rows)[0].Result[0].Count
		}
	}
	return result, nil
}

var _ store.RetrievalRunRepository = (*RetrievalRunRepository)(nil)
