package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/sauhard74/mem-jev/internal/maintenance"
)

type maintenanceRow struct {
	spec                    maintenance.Spec
	state                   maintenance.State
	attempt                 uint32
	fencing                 uint64
	owner                   string
	leaseExpires, available time.Time
	errorCode               string
}

type MaintenanceRepository struct {
	mu          sync.Mutex
	jobs        map[string]maintenanceRow
	idempotency map[string]string
	now         func() time.Time
}

func NewMaintenanceRepository() *MaintenanceRepository { return newMaintenanceRepository(time.Now) }
func newMaintenanceRepository(now func() time.Time) *MaintenanceRepository {
	return &MaintenanceRepository{jobs: make(map[string]maintenanceRow), idempotency: make(map[string]string), now: now}
}

func (r *MaintenanceRepository) Enqueue(ctx context.Context, spec maintenance.Spec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || maintenance.ValidateSpec(spec) != nil {
		return maintenance.ErrInvalidJob
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := string(spec.TenantID) + "\x00" + string(spec.Kind) + "\x00" + spec.IdempotencyKeyHash
	if id, ok := r.idempotency[key]; ok {
		if r.jobs[id].spec.ContentHash != spec.ContentHash {
			return maintenance.ErrJobConflict
		}
		return nil
	}
	r.jobs[spec.ID] = maintenanceRow{spec: spec, state: maintenance.Pending, available: spec.CreatedAt}
	r.idempotency[key] = spec.ID
	return nil
}

func (r *MaintenanceRepository) Claim(ctx context.Context, request maintenance.ClaimRequest) ([]maintenance.Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || request.WorkerID == "" || request.Limit < 1 || request.Limit > 100 || request.LeaseDuration < time.Second || maintenance.ValidateKinds(request.Kinds) != nil {
		return nil, maintenance.ErrInvalidJob
	}
	allowed := make(map[maintenance.Kind]bool, len(request.Kinds))
	for _, kind := range request.Kinds {
		allowed[kind] = true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now().UTC()
	ids := make([]string, 0, len(r.jobs))
	for id, row := range r.jobs {
		if allowed[row.spec.Kind] && (row.state == maintenance.Pending && !row.available.After(now) || row.state == maintenance.Leased && !row.leaseExpires.After(now)) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		left, right := r.jobs[ids[i]], r.jobs[ids[j]]
		if !left.available.Equal(right.available) {
			return left.available.Before(right.available)
		}
		return ids[i] < ids[j]
	})
	if len(ids) > request.Limit {
		ids = ids[:request.Limit]
	}
	result := make([]maintenance.Lease, len(ids))
	for index, id := range ids {
		row := r.jobs[id]
		row.state, row.attempt, row.fencing, row.owner, row.leaseExpires = maintenance.Leased, row.attempt+1, row.fencing+1, request.WorkerID, now.Add(request.LeaseDuration)
		r.jobs[id] = row
		result[index] = maintenance.Lease{Spec: row.spec, AttemptCount: row.attempt, FencingToken: row.fencing, LeaseOwner: row.owner, LeaseExpiresAt: row.leaseExpires}
	}
	return result, nil
}

func (r *MaintenanceRepository) Complete(ctx context.Context, request maintenance.FinishRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil {
		return maintenance.ErrInvalidJob
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	row, ok := r.jobs[request.JobID]
	if !ok || row.spec.TenantID != request.TenantID {
		return maintenance.ErrJobNotFound
	}
	if !validMaintenanceLease(row, request, r.now().UTC()) {
		return maintenance.ErrStaleLease
	}
	row.state, row.owner, row.leaseExpires = maintenance.Completed, "", time.Time{}
	r.jobs[request.JobID] = row
	return nil
}

func (r *MaintenanceRepository) Fail(ctx context.Context, request maintenance.FailRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || request.ErrorCode == "" || request.RetryAt.IsZero() || !request.RetryAt.Equal(request.RetryAt.UTC()) {
		return maintenance.ErrInvalidJob
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	row, ok := r.jobs[request.JobID]
	if !ok || row.spec.TenantID != request.TenantID {
		return maintenance.ErrJobNotFound
	}
	if !validMaintenanceLease(row, request.FinishRequest, r.now().UTC()) {
		return maintenance.ErrStaleLease
	}
	if request.DeadLetter {
		row.state = maintenance.DeadLetter
	} else {
		row.state, row.available = maintenance.Pending, request.RetryAt
	}
	row.owner, row.leaseExpires, row.errorCode = "", time.Time{}, request.ErrorCode
	r.jobs[request.JobID] = row
	return nil
}

func (r *MaintenanceRepository) Stats(ctx context.Context) ([]maintenance.QueueStats, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil {
		return nil, maintenance.ErrInvalidJob
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now().UTC()
	byKind := make(map[maintenance.Kind]maintenance.QueueStats)
	for _, row := range r.jobs {
		if row.state != maintenance.Pending && (row.state != maintenance.Leased || row.leaseExpires.After(now)) {
			continue
		}
		value := byKind[row.spec.Kind]
		value.Kind, value.Pending = row.spec.Kind, value.Pending+1
		age := now.Sub(row.available)
		if age < 0 {
			age = 0
		}
		if age > value.OldestPendingAge {
			value.OldestPendingAge = age
		}
		byKind[row.spec.Kind] = value
	}
	result := make([]maintenance.QueueStats, 0, len(byKind))
	for _, value := range byKind {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Kind < result[j].Kind })
	return result, nil
}

func validMaintenanceLease(row maintenanceRow, request maintenance.FinishRequest, now time.Time) bool {
	return request.WorkerID != "" && request.FencingToken > 0 && row.state == maintenance.Leased && row.owner == request.WorkerID && row.fencing == request.FencingToken && row.leaseExpires.After(now)
}
