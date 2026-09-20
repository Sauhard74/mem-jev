package memory

import (
	"context"
	"fmt"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/lifecycle"
	"github.com/sauhard74/mem-jev/internal/retrieval"
)

type LifecycleRepository struct {
	projection *ProjectionRepository
	manifests  map[string]lifecycle.PolicyManifest
	decisions  map[string]lifecycle.Decision
	receipts   map[string]lifecycle.Receipt
	versions   map[string]lifecycle.VersionHead
	champions  map[string]lifecycle.ChampionHead
}

func NewLifecycleRepository(projection *ProjectionRepository) *LifecycleRepository {
	return &LifecycleRepository{projection: projection, manifests: make(map[string]lifecycle.PolicyManifest), decisions: make(map[string]lifecycle.Decision), receipts: make(map[string]lifecycle.Receipt), versions: make(map[string]lifecycle.VersionHead), champions: make(map[string]lifecycle.ChampionHead)}
}

func (r *LifecycleRepository) Commit(ctx context.Context, value lifecycle.Publication) (lifecycle.Receipt, error) {
	if err := ctx.Err(); err != nil {
		return lifecycle.Receipt{}, err
	}
	if r == nil || r.projection == nil || lifecycle.ValidatePublication(value) != nil {
		return lifecycle.Receipt{}, lifecycle.ErrInvalidLifecycle
	}
	r.projection.mu.Lock()
	defer r.projection.mu.Unlock()
	if existing, ok := r.receipts[value.Decision.ID]; ok {
		existing.Disposition = lifecycle.DispositionDuplicate
		return existing, nil
	}
	if existing, ok := r.manifests[value.Manifest.ID]; ok && existing.ContentHash != value.Manifest.ContentHash {
		return lifecycle.Receipt{}, lifecycle.ErrLifecycleConflict
	}
	latestEpoch, latestDocument, found := r.latestDocument(value.Decision.TenantID, value.Decision.ProcedureVersionID)
	if !found || latestEpoch != value.SourceEpoch || latestDocument.ContentHash != value.SourceDocument.ContentHash {
		return lifecycle.Receipt{}, lifecycle.ErrLifecycleConflict
	}
	versionKey := lifecycleVersionKey(value.Decision.TenantID, value.Decision.ProcedureVersionID, value.Manifest.ID)
	if head, ok := r.versions[versionKey]; ok {
		if head.State != value.Decision.PriorState || head.ProjectionEpoch != value.SourceEpoch {
			return lifecycle.Receipt{}, lifecycle.ErrLifecycleConflict
		}
	}
	championKey := lifecycleChampionKey(value.Decision.TenantID, value.Decision.ProcedureID, value.Manifest.ID)
	if value.Decision.NextState == lifecycle.Active {
		if champion, ok := r.champions[championKey]; ok && champion.ProcedureVersionID != value.Decision.ProcedureVersionID {
			return lifecycle.Receipt{}, lifecycle.ErrChampionConflict
		}
	}
	receipt := lifecycle.Receipt{DecisionID: value.Decision.ID, RetrievalDocumentID: value.SourceDocument.ID, ProjectionEpoch: value.SourceEpoch, Disposition: lifecycle.DispositionPublished}
	if value.Decision.NextState != value.Decision.PriorState {
		document, err := retrieval.ReviseLifecycle(value.SourceDocument, string(value.Decision.NextState), value.Decision.CausalSuccessCount, value.Decision.UnsafeOutcomeCount, value.Decision.EvaluatedAt, value.Manifest.ID)
		if err != nil {
			return lifecycle.Receipt{}, err
		}
		epoch := r.projection.epochs[value.Decision.TenantID] + 1
		previousHash := r.projection.epochHashes[value.Decision.TenantID]
		if previousHash == "" {
			previousHash = fmt.Sprintf("%064d", 0)
		}
		_, setHash, err := canonical.MarshalAndHash(struct {
			TenantID           domain.TenantID `json:"tenant_id"`
			Epoch              uint64          `json:"epoch"`
			PreviousHash       string          `json:"previous_hash"`
			ProcedureVersionID string          `json:"procedure_version_id"`
			DocumentHash       string          `json:"document_hash"`
		}{value.Decision.TenantID, epoch, previousHash, value.Decision.ProcedureVersionID, document.ContentHash})
		if err != nil {
			return lifecycle.Receipt{}, err
		}
		r.projection.epochs[value.Decision.TenantID], r.projection.epochHashes[value.Decision.TenantID] = epoch, setHash
		r.projection.epochHistory[epochKey(value.Decision.TenantID, epoch)] = setHash
		r.projection.epochCreated[epochKey(value.Decision.TenantID, epoch)] = value.Decision.EvaluatedAt
		key := documentKey(value.Decision.TenantID, value.Decision.ProcedureVersionID, epoch)
		r.projection.documents[key], r.projection.documentEpoch[key] = document, epoch
		receipt.RetrievalDocumentID, receipt.ProjectionEpoch = document.ID, epoch
	}
	r.manifests[value.Manifest.ID], r.decisions[value.Decision.ID], r.receipts[value.Decision.ID] = value.Manifest, value.Decision, receipt
	r.versions[versionKey] = lifecycle.VersionHead{TenantID: value.Decision.TenantID, ProcedureID: value.Decision.ProcedureID, ProcedureVersionID: value.Decision.ProcedureVersionID, PolicyManifestID: value.Manifest.ID, State: value.Decision.NextState, DecisionID: value.Decision.ID, ProjectionEpoch: receipt.ProjectionEpoch, UpdatedAt: value.Decision.EvaluatedAt}
	if value.Decision.NextState == lifecycle.Active {
		r.champions[championKey] = lifecycle.ChampionHead{TenantID: value.Decision.TenantID, ProcedureID: value.Decision.ProcedureID, PolicyManifestID: value.Manifest.ID, ProcedureVersionID: value.Decision.ProcedureVersionID, DecisionID: value.Decision.ID, ProjectionEpoch: receipt.ProjectionEpoch, UpdatedAt: value.Decision.EvaluatedAt}
	} else if value.Decision.PriorState == lifecycle.Active {
		delete(r.champions, championKey)
	}
	return receipt, nil
}

func (r *LifecycleRepository) VersionHead(ctx context.Context, tenantID domain.TenantID, versionID, policyID string) (lifecycle.VersionHead, error) {
	if err := ctx.Err(); err != nil {
		return lifecycle.VersionHead{}, err
	}
	if r == nil || r.projection == nil {
		return lifecycle.VersionHead{}, lifecycle.ErrLifecycleNotFound
	}
	r.projection.mu.RLock()
	defer r.projection.mu.RUnlock()
	value, ok := r.versions[lifecycleVersionKey(tenantID, versionID, policyID)]
	if !ok {
		return lifecycle.VersionHead{}, lifecycle.ErrLifecycleNotFound
	}
	return value, nil
}

func (r *LifecycleRepository) Champion(ctx context.Context, tenantID domain.TenantID, procedureID, policyID string) (lifecycle.ChampionHead, error) {
	if err := ctx.Err(); err != nil {
		return lifecycle.ChampionHead{}, err
	}
	if r == nil || r.projection == nil {
		return lifecycle.ChampionHead{}, lifecycle.ErrLifecycleNotFound
	}
	r.projection.mu.RLock()
	defer r.projection.mu.RUnlock()
	value, ok := r.champions[lifecycleChampionKey(tenantID, procedureID, policyID)]
	if !ok {
		return lifecycle.ChampionHead{}, lifecycle.ErrLifecycleNotFound
	}
	return value, nil
}

func (r *LifecycleRepository) latestDocument(tenantID domain.TenantID, versionID string) (uint64, retrieval.Document, bool) {
	var epoch uint64
	var result retrieval.Document
	for key, document := range r.projection.documents {
		current := r.projection.documentEpoch[key]
		if document.TenantID == tenantID && document.ProcedureVersionID == versionID && current > epoch {
			epoch, result = current, document
		}
	}
	return epoch, result, epoch > 0
}

func lifecycleVersionKey(tenantID domain.TenantID, versionID, policyID string) string {
	return string(tenantID) + "\x00" + versionID + "\x00" + policyID
}
func lifecycleChampionKey(tenantID domain.TenantID, procedureID, policyID string) string {
	return string(tenantID) + "\x00" + procedureID + "\x00" + policyID
}
