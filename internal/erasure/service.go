package erasure

import (
	"context"
	"errors"
	"time"
)

type ArchiveEraser interface {
	EraseTenant(context.Context, string) (uint64, error)
}

type Repository interface {
	CommitErasure(context.Context, string, Receipt) (Receipt, error)
}

type Service struct {
	archives   ArchiveEraser
	repository Repository
	clock      func() time.Time
}

func NewService(archives ArchiveEraser, repository Repository, clock func() time.Time) (*Service, error) {
	if archives == nil || repository == nil || clock == nil {
		return nil, ErrInvalidRequest
	}
	return &Service{archives: archives, repository: repository, clock: clock}, nil
}

func (service *Service) Erase(ctx context.Context, request Request) (Receipt, error) {
	if service == nil || ctx == nil || !validRequest(request) {
		return Receipt{}, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	deleted, err := service.archives.EraseTenant(ctx, request.TenantID)
	if err != nil {
		return Receipt{}, errors.Join(errors.New("erase tenant archives"), err)
	}
	receipt, err := NewReceipt(request.RequestID, request.TenantID, deleted, service.clock())
	if err != nil {
		return Receipt{}, err
	}
	return service.repository.CommitErasure(ctx, request.TenantID, receipt)
}
