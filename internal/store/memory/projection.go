package memory

import (
	"bytes"
	"context"
	"sync"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/projection"
)

type ProjectionRepository struct {
	mu        sync.RWMutex
	families  map[string]projection.Family
	versions  map[string]projection.Version
	steps     map[string]projection.Step
	edges     map[string]projection.Edge
	negative  map[string]struct{}
	manifests map[string]projection.Manifest
	evidence  map[string]struct{}
	canonical map[string][]byte
}

func NewProjectionRepository() *ProjectionRepository {
	return &ProjectionRepository{
		families: make(map[string]projection.Family), versions: make(map[string]projection.Version),
		steps: make(map[string]projection.Step), edges: make(map[string]projection.Edge), negative: make(map[string]struct{}),
		manifests: make(map[string]projection.Manifest), evidence: make(map[string]struct{}), canonical: make(map[string][]byte),
	}
}

func (r *ProjectionRepository) Publish(ctx context.Context, value projection.Projection) (projection.PublishReceipt, error) {
	if err := ctx.Err(); err != nil {
		return projection.PublishReceipt{}, err
	}
	if err := projection.Validate(value); err != nil {
		return projection.PublishReceipt{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, found := r.manifests[value.Manifest.ID]; found {
		if existing.ContentHash != value.Manifest.ContentHash {
			return projection.PublishReceipt{}, projection.ErrProjectionConflict
		}
		return projection.PublishReceipt{ManifestID: value.Manifest.ID, ProcedureID: value.Family.ID, ProcedureVersionID: value.Version.ID, Disposition: projection.DispositionDuplicate}, nil
	}
	receipt := projection.PublishReceipt{ManifestID: value.Manifest.ID, ProcedureID: value.Family.ID, ProcedureVersionID: value.Version.ID, Disposition: projection.DispositionPublished}
	if value.Manifest.Status == "abstained" {
		r.manifests[value.Manifest.ID] = value.Manifest
		return receipt, nil
	}
	if existing, found := r.families[value.Family.ID]; found {
		if existing.ContentHash != value.Family.ContentHash {
			return projection.PublishReceipt{}, projection.ErrProjectionConflict
		}
	} else {
		r.families[value.Family.ID] = value.Family
		receipt.NewFamily = true
	}
	if existing, found := r.versions[value.Version.ID]; found {
		if existing.ContentHash != value.Version.ContentHash || !bytes.Equal(r.canonical[value.Version.ID], value.CanonicalProjectionJSON) {
			return projection.PublishReceipt{}, projection.ErrProjectionConflict
		}
	} else {
		r.versions[value.Version.ID] = value.Version
		r.canonical[value.Version.ID] = append([]byte(nil), value.CanonicalProjectionJSON...)
		for _, step := range value.Steps {
			r.steps[step.ID] = step
		}
		for _, edge := range value.Edges {
			r.edges[edge.ID] = edge
		}
		receipt.NewVersion = true
	}
	for _, path := range value.NegativePaths {
		r.negative[string(value.TenantID)+"\x00"+value.Version.ID+"\x00"+path.ID] = struct{}{}
	}
	evidenceKey := value.Version.ID + "\x00" + string(value.Manifest.OutcomeID)
	if _, found := r.evidence[evidenceKey]; !found {
		r.evidence[evidenceKey] = struct{}{}
		receipt.EvidenceAdded = true
	}
	r.manifests[value.Manifest.ID] = value.Manifest
	return receipt, nil
}

func (r *ProjectionRepository) Counts(ctx context.Context) (projection.Counts, error) {
	if err := ctx.Err(); err != nil {
		return projection.Counts{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return projection.Counts{Families: len(r.families), Versions: len(r.versions), Steps: len(r.steps), Edges: len(r.edges), NegativePaths: len(r.negative), Manifests: len(r.manifests), EvidenceLinks: len(r.evidence)}, nil
}

func (r *ProjectionRepository) Canonical(ctx context.Context, tenantID domain.TenantID, versionID string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	version, found := r.versions[versionID]
	if !found || r.families[version.ProcedureID].ID == "" {
		return nil, projection.ErrProjectionNotFound
	}
	// Version IDs are tenant-derived, but retain an explicit tenant ownership
	// check so this adapter has the same fail-closed contract as SurrealDB.
	family := r.families[version.ProcedureID]
	expectedPrefix := valueFamilyID(tenantID, family)
	if expectedPrefix != family.ID {
		return nil, projection.ErrProjectionNotFound
	}
	return append([]byte(nil), r.canonical[versionID]...), nil
}

func valueFamilyID(tenantID domain.TenantID, family projection.Family) string {
	identity := struct {
		TenantID            domain.TenantID `json:"tenant_id"`
		IntentHash          string          `json:"intent_hash"`
		EffectSignatureHash string          `json:"effect_signature_hash"`
	}{tenantID, family.IntentHash, family.EffectSignatureHash}
	_, hash, err := canonical.MarshalAndHash(identity)
	if err != nil {
		return ""
	}
	return "proc_" + hash
}
