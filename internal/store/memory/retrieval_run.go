package memory

import (
	"context"
	"reflect"
	"sync"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store"
)

type RetrievalRunRepository struct {
	mu         sync.RWMutex
	projection *ProjectionRepository
	configs    map[string]retrieval.ServingConfig
	active     map[domain.TenantID]string
	runs       map[string]retrieval.Run
}

func NewRetrievalRunRepository(projection *ProjectionRepository) *RetrievalRunRepository {
	return &RetrievalRunRepository{projection: projection, configs: make(map[string]retrieval.ServingConfig), active: make(map[domain.TenantID]string), runs: make(map[string]retrieval.Run)}
}

func (r *RetrievalRunRepository) ActivateServingConfig(ctx context.Context, config retrieval.ServingConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.projection == nil || retrieval.ValidateServingConfig(config) != nil {
		return store.ErrServingConfigUnavailable
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := configKey(config.TenantID, config.ID)
	if existing, found := r.configs[key]; found && existing.ContentHash != config.ContentHash {
		return store.ErrRetrievalRunConflict
	}
	config.CanonicalJSON = append([]byte(nil), config.CanonicalJSON...)
	r.configs[key] = config
	r.active[config.TenantID] = config.ID
	return nil
}

func (r *RetrievalRunRepository) AcquireServingSnapshot(ctx context.Context, tenantID domain.TenantID) (retrieval.ServingSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return retrieval.ServingSnapshot{}, err
	}
	if r == nil || r.projection == nil || tenantID == "" {
		return retrieval.ServingSnapshot{}, store.ErrServingConfigUnavailable
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	configID := r.active[tenantID]
	config, found := r.configs[configKey(tenantID, configID)]
	if !found {
		return retrieval.ServingSnapshot{}, store.ErrServingConfigUnavailable
	}
	r.projection.mu.RLock()
	defer r.projection.mu.RUnlock()
	epoch := r.projection.epochs[tenantID]
	hash := r.projection.epochHistory[epochKey(tenantID, epoch)]
	if epoch == 0 || hash == "" {
		return retrieval.ServingSnapshot{}, store.ErrServingConfigUnavailable
	}
	return retrieval.ServingSnapshot{ProjectionEpoch: epoch, DocumentSetHash: hash, ServingConfigID: config.ID, PolicyManifestID: config.PolicyManifestID, RankerManifestID: config.RankerManifestID, Indexes: append([]retrieval.SnapshotIndex(nil), config.Indexes...)}, nil
}

func (r *RetrievalRunRepository) SaveRetrievalRun(ctx context.Context, run retrieval.Run) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.projection == nil || retrieval.ValidateRun(run) != nil {
		return store.ErrRetrievalRunConflict
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	config, found := r.configs[configKey(run.TenantID, run.Snapshot.ServingConfigID)]
	if !found || config.PolicyManifestID != run.Snapshot.PolicyManifestID || config.RankerManifestID != run.Snapshot.RankerManifestID || !reflect.DeepEqual(config.Indexes, run.Snapshot.Indexes) {
		return store.ErrRetrievalSnapshotMismatch
	}
	r.projection.mu.RLock()
	hash := r.projection.epochHistory[epochKey(run.TenantID, run.Snapshot.ProjectionEpoch)]
	r.projection.mu.RUnlock()
	if hash == "" || hash != run.Snapshot.DocumentSetHash {
		return store.ErrRetrievalSnapshotMismatch
	}
	key := runKey(run.TenantID, run.ID)
	if existing, exists := r.runs[key]; exists {
		if existing.ContentHash != run.ContentHash {
			return store.ErrRetrievalRunConflict
		}
		return nil
	}
	run.CanonicalJSON = append([]byte(nil), run.CanonicalJSON...)
	r.runs[key] = run
	return nil
}

func (r *RetrievalRunRepository) RetrievalRun(ctx context.Context, tenantID domain.TenantID, id string) (retrieval.Run, error) {
	if err := ctx.Err(); err != nil {
		return retrieval.Run{}, err
	}
	if r == nil || tenantID == "" {
		return retrieval.Run{}, store.ErrRetrievalRunNotFound
	}
	r.mu.RLock()
	run, found := r.runs[runKey(tenantID, id)]
	r.mu.RUnlock()
	if !found {
		return retrieval.Run{}, store.ErrRetrievalRunNotFound
	}
	return retrieval.DecodeRun(run.CanonicalJSON, run.ContentHash)
}

func configKey(tenantID domain.TenantID, id string) string { return string(tenantID) + "\x00" + id }
func runKey(tenantID domain.TenantID, id string) string    { return string(tenantID) + "\x00" + id }

var _ store.RetrievalRunRepository = (*RetrievalRunRepository)(nil)
