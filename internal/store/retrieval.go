package store

import (
	"context"
	"errors"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
)

var (
	ErrServingConfigUnavailable  = errors.New("retrieval serving configuration unavailable")
	ErrRetrievalRunNotFound      = retrieval.ErrRunNotFound
	ErrRetrievalRunConflict      = errors.New("retrieval run conflict")
	ErrRetrievalSnapshotMismatch = errors.New("retrieval snapshot mismatch")
)

type RetrievalRunRepository interface {
	ActivateServingConfig(context.Context, retrieval.ServingConfig) error
	AcquireServingSnapshot(context.Context, domain.TenantID) (retrieval.ServingSnapshot, error)
	SaveRetrievalRun(context.Context, retrieval.Run) error
	RetrievalRun(context.Context, domain.TenantID, string) (retrieval.Run, error)
}
