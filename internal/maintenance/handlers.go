package maintenance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/composition"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/lifecycle"
	"github.com/sauhard74/mem-jev/internal/rebuild"
	"github.com/sauhard74/mem-jev/internal/retrieval"
)

var ErrInvalidPayload = errors.New("invalid authenticated maintenance payload")

// CompatibilityPublisher is intentionally narrower than a query repository:
// a projection worker may only publish a fully authenticated graph.
type CompatibilityPublisher interface {
	Publish(context.Context, composition.CompatibilityGraph) error
}

type CompatibilityPayload struct {
	SchemaVersion  string          `json:"schema_version"`
	TenantID       domain.TenantID `json:"tenant_id"`
	GraphID        string          `json:"compatibility_graph_id"`
	GraphHash      string          `json:"compatibility_graph_hash"`
	CanonicalGraph []byte          `json:"canonical_graph"`
}

func NewCompatibilityPayload(graph composition.CompatibilityGraph) ([]byte, error) {
	if composition.ValidateCompatibilityGraph(graph) != nil {
		return nil, ErrInvalidPayload
	}
	value := CompatibilityPayload{SchemaVersion: "compatibility-projection-job.v1", TenantID: graph.TenantID, GraphID: graph.ID, GraphHash: graph.ContentHash, CanonicalGraph: append([]byte(nil), graph.CanonicalJSON...)}
	encoded, _, err := canonical.MarshalAndHash(value)
	return encoded, err
}

type CompatibilityHandler struct{ Publisher CompatibilityPublisher }

func (h CompatibilityHandler) Handle(ctx context.Context, lease Lease) error {
	if h.Publisher == nil || lease.Kind != CompatibilityProjection {
		return ErrInvalidPayload
	}
	var payload CompatibilityPayload
	if strictDecode(lease.CanonicalPayload, &payload) != nil || payload.SchemaVersion != "compatibility-projection-job.v1" || payload.TenantID != lease.TenantID {
		return ErrInvalidPayload
	}
	var graph composition.CompatibilityGraph
	if strictDecode(payload.CanonicalGraph, &graph) != nil {
		return ErrInvalidPayload
	}
	graph.ID, graph.ContentHash, graph.CanonicalJSON = payload.GraphID, payload.GraphHash, append([]byte(nil), payload.CanonicalGraph...)
	if graph.TenantID != payload.TenantID || composition.ValidateCompatibilityGraph(graph) != nil {
		return ErrInvalidPayload
	}
	return h.Publisher.Publish(ctx, graph)
}

type LifecyclePayload struct {
	SchemaVersion      string             `json:"schema_version"`
	TenantID           domain.TenantID    `json:"tenant_id"`
	ManifestID         string             `json:"manifest_id"`
	ManifestHash       string             `json:"manifest_hash"`
	CanonicalManifest  []byte             `json:"canonical_manifest"`
	SourceDocumentID   string             `json:"source_document_id"`
	SourceDocumentHash string             `json:"source_document_hash"`
	CanonicalDocument  []byte             `json:"canonical_document"`
	SourceEpoch        uint64             `json:"source_epoch"`
	Evidence           lifecycle.Evidence `json:"evidence"`
	EvidenceCutoffAt   string             `json:"evidence_cutoff_at"`
	EvaluatedAt        string             `json:"evaluated_at"`
}

type LifecyclePublisher interface {
	Commit(context.Context, lifecycle.Publication) (lifecycle.Receipt, error)
}

// NewLifecyclePayload seals all inputs needed to reproduce a transition. The
// times are encoded by canonical JSON from their RFC3339 representation in the
// lifecycle request, avoiding dependence on a worker's local clock.
func NewLifecyclePayload(request lifecycle.EvaluationRequest, source retrieval.Document, sourceEpoch uint64) ([]byte, error) {
	if _, err := lifecycle.Evaluate(request); err != nil || retrieval.ValidateDocument(source) != nil || sourceEpoch == 0 || request.TenantID != source.TenantID || request.ProcedureID != source.ProcedureID || request.ProcedureVersionID != source.ProcedureVersionID || string(request.PriorState) != source.Lifecycle {
		return nil, ErrInvalidPayload
	}
	value := LifecyclePayload{
		SchemaVersion: "lifecycle-evaluation-job.v1", TenantID: request.TenantID,
		ManifestID: request.Manifest.ID, ManifestHash: request.Manifest.ContentHash, CanonicalManifest: append([]byte(nil), request.Manifest.CanonicalJSON...),
		SourceDocumentID: source.ID, SourceDocumentHash: source.ContentHash, CanonicalDocument: append([]byte(nil), source.CanonicalJSON...), SourceEpoch: sourceEpoch,
		Evidence: request.Evidence, EvidenceCutoffAt: request.EvidenceCutoffAt.Format("2006-01-02T15:04:05.999999999Z07:00"), EvaluatedAt: request.EvaluatedAt.Format("2006-01-02T15:04:05.999999999Z07:00"),
	}
	encoded, _, err := canonical.MarshalAndHash(value)
	return encoded, err
}

type LifecycleHandler struct {
	Publisher LifecyclePublisher
	Rollback  bool
}

type ExpiryPayload struct {
	SchemaVersion string          `json:"schema_version"`
	TenantID      domain.TenantID `json:"tenant_id"`
	CutoffAt      string          `json:"cutoff_at"`
	MaximumRows   uint32          `json:"maximum_rows_per_table"`
}

type TransientExpirer interface {
	ExpireTransient(context.Context, domain.TenantID, time.Time, uint32) error
}

func NewExpiryPayload(tenantID domain.TenantID, cutoffAt time.Time, maximumRows uint32) ([]byte, error) {
	if tenantID == "" || cutoffAt.IsZero() || !cutoffAt.Equal(cutoffAt.UTC()) || maximumRows == 0 || maximumRows > 100_000 {
		return nil, ErrInvalidPayload
	}
	value := ExpiryPayload{SchemaVersion: "transient-expiry-job.v1", TenantID: tenantID, CutoffAt: cutoffAt.Format(time.RFC3339Nano), MaximumRows: maximumRows}
	encoded, _, err := canonical.MarshalAndHash(value)
	return encoded, err
}

type TransientExpiryHandler struct{ Expirer TransientExpirer }

func (h TransientExpiryHandler) Handle(ctx context.Context, lease Lease) error {
	if h.Expirer == nil || lease.Kind != TransientExpiry {
		return ErrInvalidPayload
	}
	var payload ExpiryPayload
	if strictDecode(lease.CanonicalPayload, &payload) != nil || payload.SchemaVersion != "transient-expiry-job.v1" || payload.TenantID != lease.TenantID || payload.MaximumRows == 0 || payload.MaximumRows > 100_000 {
		return ErrInvalidPayload
	}
	cutoff, err := time.Parse(time.RFC3339Nano, payload.CutoffAt)
	if err != nil || !cutoff.Equal(cutoff.UTC()) {
		return ErrInvalidPayload
	}
	return h.Expirer.ExpireTransient(ctx, payload.TenantID, cutoff, payload.MaximumRows)
}

type RebuildPayload struct {
	SchemaVersion       string          `json:"schema_version"`
	TenantID            domain.TenantID `json:"tenant_id"`
	StoredSnapshotID    string          `json:"stored_snapshot_id"`
	StoredSnapshotHash  string          `json:"stored_snapshot_hash"`
	CanonicalStored     []byte          `json:"canonical_stored_snapshot"`
	RebuiltSnapshotID   string          `json:"rebuilt_snapshot_id"`
	RebuiltSnapshotHash string          `json:"rebuilt_snapshot_hash"`
	CanonicalRebuilt    []byte          `json:"canonical_rebuilt_snapshot"`
	AuthorizedAt        string          `json:"authorized_at"`
}

type RebuildPublisher interface {
	PublishActivation(context.Context, rebuild.DerivedSnapshot, rebuild.DerivedSnapshot, rebuild.ActivationPermit) error
}

func NewRebuildPayload(stored, rebuilt rebuild.DerivedSnapshot, authorizedAt time.Time) ([]byte, error) {
	if rebuild.ValidateDerivedSnapshot(stored) != nil || rebuild.ValidateDerivedSnapshot(rebuilt) != nil || authorizedAt.IsZero() || !authorizedAt.Equal(authorizedAt.UTC()) || stored.TenantID != rebuilt.TenantID || stored.ProjectionEpoch != rebuilt.ProjectionEpoch {
		return nil, ErrInvalidPayload
	}
	value := RebuildPayload{SchemaVersion: "projection-rebuild-job.v1", TenantID: stored.TenantID, StoredSnapshotID: stored.ID, StoredSnapshotHash: stored.ContentHash, CanonicalStored: append([]byte(nil), stored.CanonicalJSON...), RebuiltSnapshotID: rebuilt.ID, RebuiltSnapshotHash: rebuilt.ContentHash, CanonicalRebuilt: append([]byte(nil), rebuilt.CanonicalJSON...), AuthorizedAt: authorizedAt.Format(time.RFC3339Nano)}
	encoded, _, err := canonical.MarshalAndHash(value)
	return encoded, err
}

type ProjectionRebuildHandler struct{ Publisher RebuildPublisher }

func (h ProjectionRebuildHandler) Handle(ctx context.Context, lease Lease) error {
	if h.Publisher == nil || lease.Kind != ProjectionRebuild {
		return ErrInvalidPayload
	}
	var payload RebuildPayload
	if strictDecode(lease.CanonicalPayload, &payload) != nil || payload.SchemaVersion != "projection-rebuild-job.v1" || payload.TenantID != lease.TenantID {
		return ErrInvalidPayload
	}
	stored, err := rebuild.HydrateDerivedSnapshot(payload.CanonicalStored, payload.StoredSnapshotID, payload.StoredSnapshotHash)
	if err != nil {
		return ErrInvalidPayload
	}
	rebuilt, err := rebuild.HydrateDerivedSnapshot(payload.CanonicalRebuilt, payload.RebuiltSnapshotID, payload.RebuiltSnapshotHash)
	if err != nil || stored.TenantID != payload.TenantID || rebuilt.TenantID != payload.TenantID {
		return ErrInvalidPayload
	}
	authorizedAt, err := time.Parse(time.RFC3339Nano, payload.AuthorizedAt)
	if err != nil || !authorizedAt.Equal(authorizedAt.UTC()) {
		return ErrInvalidPayload
	}
	permit, err := rebuild.CompareDerivedSnapshots(ctx, stored, rebuilt, authorizedAt)
	if err != nil {
		return err
	}
	return h.Publisher.PublishActivation(ctx, stored, rebuilt, permit)
}

type ExposureAuditPayload struct {
	SchemaVersion string          `json:"schema_version"`
	TenantID      domain.TenantID `json:"tenant_id"`
	WindowStart   string          `json:"window_start"`
}

type ExposureAuditor interface {
	VerifyExposureBudgets(context.Context, domain.TenantID, time.Time) error
}

func NewExposureAuditPayload(tenantID domain.TenantID, windowStart time.Time) ([]byte, error) {
	if tenantID == "" || windowStart.IsZero() || !windowStart.Equal(windowStart.UTC()) {
		return nil, ErrInvalidPayload
	}
	value := ExposureAuditPayload{SchemaVersion: "exposure-aggregation-job.v1", TenantID: tenantID, WindowStart: windowStart.Format(time.RFC3339Nano)}
	encoded, _, err := canonical.MarshalAndHash(value)
	return encoded, err
}

type ExposureAggregationHandler struct{ Auditor ExposureAuditor }

func (h ExposureAggregationHandler) Handle(ctx context.Context, lease Lease) error {
	if h.Auditor == nil || lease.Kind != ExposureAggregation {
		return ErrInvalidPayload
	}
	var payload ExposureAuditPayload
	if strictDecode(lease.CanonicalPayload, &payload) != nil || payload.SchemaVersion != "exposure-aggregation-job.v1" || payload.TenantID != lease.TenantID {
		return ErrInvalidPayload
	}
	windowStart, err := time.Parse(time.RFC3339Nano, payload.WindowStart)
	if err != nil || !windowStart.Equal(windowStart.UTC()) {
		return ErrInvalidPayload
	}
	return h.Auditor.VerifyExposureBudgets(ctx, payload.TenantID, windowStart)
}

func (h LifecycleHandler) Handle(ctx context.Context, lease Lease) error {
	wantKind := LifecycleEvaluation
	if h.Rollback {
		wantKind = LifecycleRollback
	}
	if h.Publisher == nil || lease.Kind != wantKind {
		return ErrInvalidPayload
	}
	publication, err := lifecyclePublication(lease)
	if err != nil {
		return err
	}
	if h.Rollback && (publication.Decision.PriorState != lifecycle.Active || publication.Decision.NextState == lifecycle.Active) {
		return ErrInvalidPayload
	}
	_, err = h.Publisher.Commit(ctx, publication)
	return err
}

func lifecyclePublication(lease Lease) (lifecycle.Publication, error) {
	var payload LifecyclePayload
	if strictDecode(lease.CanonicalPayload, &payload) != nil || payload.SchemaVersion != "lifecycle-evaluation-job.v1" || payload.TenantID != lease.TenantID {
		return lifecycle.Publication{}, ErrInvalidPayload
	}
	var manifest lifecycle.PolicyManifest
	if strictDecode(payload.CanonicalManifest, &manifest) != nil {
		return lifecycle.Publication{}, ErrInvalidPayload
	}
	manifest.ID, manifest.ContentHash, manifest.CanonicalJSON = payload.ManifestID, payload.ManifestHash, append([]byte(nil), payload.CanonicalManifest...)
	var document retrieval.Document
	if strictDecode(payload.CanonicalDocument, &document) != nil {
		return lifecycle.Publication{}, ErrInvalidPayload
	}
	document.ID, document.ContentHash, document.CanonicalJSON = payload.SourceDocumentID, payload.SourceDocumentHash, append([]byte(nil), payload.CanonicalDocument...)
	cutoff, err := time.Parse(time.RFC3339Nano, payload.EvidenceCutoffAt)
	if err != nil {
		return lifecycle.Publication{}, ErrInvalidPayload
	}
	evaluated, err := time.Parse(time.RFC3339Nano, payload.EvaluatedAt)
	if err != nil {
		return lifecycle.Publication{}, ErrInvalidPayload
	}
	request := lifecycle.EvaluationRequest{TenantID: payload.TenantID, ProcedureID: document.ProcedureID, ProcedureVersionID: document.ProcedureVersionID, PriorState: lifecycle.State(document.Lifecycle), Manifest: manifest, Evidence: payload.Evidence, EvidenceCutoffAt: cutoff, EvaluatedAt: evaluated}
	decision, err := lifecycle.Evaluate(request)
	if err != nil {
		return lifecycle.Publication{}, ErrInvalidPayload
	}
	publication := lifecycle.Publication{Manifest: manifest, Decision: decision, SourceDocument: document, SourceEpoch: payload.SourceEpoch}
	if lifecycle.ValidatePublication(publication) != nil {
		return lifecycle.Publication{}, ErrInvalidPayload
	}
	return publication, nil
}

func strictDecode(source []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(source))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrInvalidPayload
	}
	return nil
}
