package memory

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/projection"
	"github.com/sauhard74/mem-jev/internal/retrieval"
)

type ProjectionRepository struct {
	mu            sync.RWMutex
	families      map[string]projection.Family
	versions      map[string]projection.Version
	steps         map[string]projection.Step
	edges         map[string]projection.Edge
	negative      map[string]struct{}
	manifests     map[string]projection.Manifest
	evidence      map[string]struct{}
	canonical     map[string][]byte
	documents     map[string]retrieval.Document
	documentEpoch map[string]uint64
	epochs        map[domain.TenantID]uint64
	epochHashes   map[domain.TenantID]string
	epochHistory  map[string]string
	epochCreated  map[string]time.Time
}

func NewProjectionRepository() *ProjectionRepository {
	return &ProjectionRepository{
		families: make(map[string]projection.Family), versions: make(map[string]projection.Version),
		steps: make(map[string]projection.Step), edges: make(map[string]projection.Edge), negative: make(map[string]struct{}),
		manifests: make(map[string]projection.Manifest), evidence: make(map[string]struct{}), canonical: make(map[string][]byte),
		documents: make(map[string]retrieval.Document), documentEpoch: make(map[string]uint64), epochs: make(map[domain.TenantID]uint64), epochHashes: make(map[domain.TenantID]string), epochHistory: make(map[string]string), epochCreated: make(map[string]time.Time),
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
	count := uint64(0)
	for key := range r.evidence {
		if strings.HasPrefix(key, value.Version.ID+"\x00") {
			count++
		}
	}
	document, err := retrieval.ReviseEvidence(value.RetrievalDocument, count, 0, value.CreatedAt)
	if err != nil {
		return projection.PublishReceipt{}, err
	}
	r.epochs[value.TenantID]++
	receipt.ProjectionEpoch = r.epochs[value.TenantID]
	previousHash := r.epochHashes[value.TenantID]
	if previousHash == "" {
		previousHash = strings.Repeat("0", 64)
	}
	_, documentSetHash, err := canonical.MarshalAndHash(struct {
		TenantID           domain.TenantID `json:"tenant_id"`
		Epoch              uint64          `json:"epoch"`
		PreviousHash       string          `json:"previous_hash"`
		ProcedureVersionID string          `json:"procedure_version_id"`
		DocumentHash       string          `json:"document_hash"`
	}{value.TenantID, receipt.ProjectionEpoch, previousHash, value.Version.ID, document.ContentHash})
	if err != nil {
		return projection.PublishReceipt{}, err
	}
	r.epochHashes[value.TenantID] = documentSetHash
	r.epochHistory[epochKey(value.TenantID, receipt.ProjectionEpoch)] = documentSetHash
	r.epochCreated[epochKey(value.TenantID, receipt.ProjectionEpoch)] = value.CreatedAt.UTC()
	receipt.RetrievalDocumentID = document.ID
	key := documentKey(value.TenantID, value.Version.ID, receipt.ProjectionEpoch)
	r.documents[key] = document
	r.documentEpoch[key] = receipt.ProjectionEpoch
	return receipt, nil
}

func (r *ProjectionRepository) Counts(ctx context.Context) (projection.Counts, error) {
	if err := ctx.Err(); err != nil {
		return projection.Counts{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return projection.Counts{Families: len(r.families), Versions: len(r.versions), Steps: len(r.steps), Edges: len(r.edges), NegativePaths: len(r.negative), Manifests: len(r.manifests), EvidenceLinks: len(r.evidence), RetrievalDocuments: len(r.documents), ProjectionEpochs: epochCount(r.epochs)}, nil
}

func (r *ProjectionRepository) RetrievalDocument(ctx context.Context, tenantID domain.TenantID, versionID string, epoch uint64) (retrieval.Document, error) {
	if err := ctx.Err(); err != nil {
		return retrieval.Document{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	document, ok := r.documents[documentKey(tenantID, versionID, epoch)]
	if !ok {
		return retrieval.Document{}, projection.ErrProjectionNotFound
	}
	return document, nil
}

func (r *ProjectionRepository) RetrievalDocuments(ctx context.Context, tenantID domain.TenantID, versionIDs []string, epoch uint64) ([]retrieval.Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tenantID == "" || epoch == 0 || len(versionIDs) == 0 || len(versionIDs) > 1000 {
		return nil, projection.ErrProjectionNotFound
	}
	wanted := make(map[string]struct{}, len(versionIDs))
	for _, id := range versionIDs {
		if id == "" {
			return nil, projection.ErrProjectionNotFound
		}
		wanted[id] = struct{}{}
	}
	r.mu.RLock()
	latestEpoch := make(map[string]uint64, len(wanted))
	latest := make(map[string]retrieval.Document, len(wanted))
	for key, document := range r.documents {
		documentEpoch := r.documentEpoch[key]
		if document.TenantID != tenantID || documentEpoch > epoch {
			continue
		}
		if _, ok := wanted[document.ProcedureVersionID]; !ok || documentEpoch <= latestEpoch[document.ProcedureVersionID] {
			continue
		}
		latestEpoch[document.ProcedureVersionID], latest[document.ProcedureVersionID] = documentEpoch, document
	}
	r.mu.RUnlock()
	ids := make([]string, 0, len(latest))
	for id := range latest {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]retrieval.Document, len(ids))
	for index, id := range ids {
		result[index] = latest[id]
	}
	return result, nil
}

func documentKey(tenantID domain.TenantID, versionID string, epoch uint64) string {
	return fmt.Sprintf("%s\x00%s\x00%020d", tenantID, versionID, epoch)
}

func epochKey(tenantID domain.TenantID, epoch uint64) string {
	return fmt.Sprintf("%s\x00%020d", tenantID, epoch)
}

func epochCount(epochs map[domain.TenantID]uint64) int {
	total := 0
	for _, epoch := range epochs {
		total += int(epoch)
	}
	return total
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
