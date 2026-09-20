package lifecycle

import (
	"context"
	"errors"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
)

var (
	ErrLifecycleConflict = errors.New("lifecycle publication conflict")
	ErrChampionConflict  = errors.New("procedure family already has an active champion")
	ErrLifecycleNotFound = errors.New("lifecycle record not found")
)

type Publication struct {
	Manifest       PolicyManifest
	Decision       Decision
	SourceDocument retrieval.Document
	SourceEpoch    uint64
}

type Disposition string

const (
	DispositionPublished Disposition = "published"
	DispositionDuplicate Disposition = "duplicate"
)

type Receipt struct {
	DecisionID          string
	RetrievalDocumentID string
	ProjectionEpoch     uint64
	Disposition         Disposition
}

type VersionHead struct {
	TenantID           domain.TenantID
	ProcedureID        string
	ProcedureVersionID string
	PolicyManifestID   string
	State              State
	DecisionID         string
	ProjectionEpoch    uint64
	UpdatedAt          time.Time
}

type ChampionHead struct {
	TenantID           domain.TenantID
	ProcedureID        string
	PolicyManifestID   string
	ProcedureVersionID string
	DecisionID         string
	ProjectionEpoch    uint64
	UpdatedAt          time.Time
}

type Repository interface {
	Commit(context.Context, Publication) (Receipt, error)
	VersionHead(context.Context, domain.TenantID, string, string) (VersionHead, error)
	Champion(context.Context, domain.TenantID, string, string) (ChampionHead, error)
}

func ValidatePublication(value Publication) error {
	if ValidatePolicyManifest(value.Manifest) != nil || ValidateDecision(value.Decision) != nil || retrieval.ValidateDocument(value.SourceDocument) != nil || value.SourceEpoch == 0 ||
		value.Decision.TenantID != value.SourceDocument.TenantID || value.Decision.TenantID == "" || value.Decision.ProcedureID != value.SourceDocument.ProcedureID || value.Decision.ProcedureVersionID != value.SourceDocument.ProcedureVersionID ||
		value.Decision.PolicyManifestID != value.Manifest.ID || string(value.Decision.PriorState) != value.SourceDocument.Lifecycle {
		return ErrInvalidLifecycle
	}
	return nil
}
