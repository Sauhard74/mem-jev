package ingest

import (
	"context"
	"regexp"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/contracts"
	"github.com/sauhard74/mem-jev/internal/policy"
	"github.com/sauhard74/mem-jev/internal/security"
	"github.com/sauhard74/mem-jev/internal/store"
)

type Command struct {
	Principal          security.Principal
	IdempotencyKeyHash string
	Request            *memjevv1.IngestTraceRequest
}

type Service struct {
	archives   archive.Store
	repository store.IngestRepository
	policy     SanitizerPolicy
}

var idempotencyHashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func NewService(archives archive.Store, repository store.IngestRepository, sanitizerPolicy SanitizerPolicy) *Service {
	return &Service{
		archives:   archives,
		repository: repository,
		policy:     clonePolicy(sanitizerPolicy),
	}
}

func (s *Service) Ingest(ctx context.Context, command Command) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if s == nil || s.archives == nil || s.repository == nil {
		return Result{}, ErrMisconfigured
	}
	if command.Principal.TenantID == "" || command.Request == nil ||
		!idempotencyHashPattern.MatchString(command.IdempotencyKeyHash) {
		return Result{}, ErrInvalidCommand
	}
	if !command.Principal.HasScope(security.ScopeIngestWrite) {
		return Result{}, ErrPermissionDenied
	}
	if err := policy.AuthorizeIngest(command.Principal.Consent); err != nil {
		return Result{}, operationError("authorize ingest", err)
	}
	if err := contracts.ValidateIngest(command.Request); err != nil {
		return Result{}, operationError("validate trace", ErrInvalidTrace)
	}
	clean, report, err := Sanitize(command.Request, s.policy)
	if err != nil {
		return Result{}, operationError("sanitize trace", err)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	batch, err := canonical.Build(command.Principal.TenantID, clean)
	if err != nil {
		return Result{}, operationError("canonicalize trace", ErrInvalidTrace)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	object, err := s.archives.PutCanonical(ctx, archive.PutRequest{
		TenantID:      command.Principal.TenantID,
		SchemaVersion: batch.SchemaVersion,
		Hash:          batch.Hash,
		Body:          batch.CanonicalJSON,
	})
	if err != nil {
		return Result{}, operationError("archive canonical trace", err)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	receipt, err := s.repository.Commit(ctx, store.CommitIngestRequest{
		TenantID:           command.Principal.TenantID,
		IdempotencyKeyHash: command.IdempotencyKeyHash,
		Batch:              batch,
		Archive:            object,
	})
	if err != nil {
		return Result{}, operationError("commit ingest ledger", err)
	}
	return Result{
		ReceiptID:      receipt.ID,
		TraceID:        receipt.TraceID,
		CanonicalHash:  receipt.ContentHash,
		ArchiveKey:     object.Key,
		Disposition:    receipt.Disposition,
		AcceptedEvents: len(batch.Events),
		Redactions:     report.Redactions,
	}, nil
}

func clonePolicy(source SanitizerPolicy) SanitizerPolicy {
	cloned := source
	cloned.AllowedFields = make(map[string]struct{}, len(source.AllowedFields))
	for field := range source.AllowedFields {
		cloned.AllowedFields[field] = struct{}{}
	}
	return cloned
}
