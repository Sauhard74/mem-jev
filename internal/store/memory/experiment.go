package memory

import (
	"context"
	"strconv"
	"sync"

	"github.com/sauhard74/mem-jev/internal/experiment"
)

type experimentBudget struct {
	Exposure uint64
	Unsafe   uint64
}

type ExperimentRepository struct {
	mu          sync.Mutex
	manifests   map[string]experiment.Manifest
	epochs      map[string]string
	assignments map[string]experiment.Assignment
	tenant      map[string]experimentBudget
	global      map[string]experimentBudget
}

func NewExperimentRepository() *ExperimentRepository {
	return &ExperimentRepository{manifests: make(map[string]experiment.Manifest), epochs: make(map[string]string), assignments: make(map[string]experiment.Assignment), tenant: make(map[string]experimentBudget), global: make(map[string]experimentBudget)}
}

func (r *ExperimentRepository) Reserve(ctx context.Context, request experiment.AssignmentRequest) (experiment.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return experiment.Reservation{}, err
	}
	candidate, err := experiment.Assign(request)
	if err != nil {
		return experiment.Reservation{}, err
	}
	request.Exposure.WindowStart = candidate.WindowStart
	r.mu.Lock()
	defer r.mu.Unlock()
	key := experimentAssignmentKey(string(candidate.TenantID), candidate.ManifestID, candidate.AssignmentKeyHash)
	if existing, ok := r.assignments[key]; ok {
		if existing.QueryBucketHash != candidate.QueryBucketHash || existing.Risk != candidate.Risk {
			return experiment.Reservation{}, experiment.ErrExperimentConflict
		}
		return experiment.Reservation{Assignment: existing, Disposition: experiment.DispositionDuplicate}, nil
	}
	epochKey := experimentEpochKey(string(request.Manifest.TenantID), request.Manifest.ProcedureID, request.Manifest.ExperimentEpoch)
	if manifestID, ok := r.epochs[epochKey]; ok && manifestID != request.Manifest.ID {
		return experiment.Reservation{}, experiment.ErrExperimentConflict
	}
	tenantKey := experimentBudgetKey(string(request.Manifest.TenantID), request.Exposure.WindowStart.String())
	globalKey := experimentBudgetKey("_global", request.Exposure.WindowStart.String())
	tenantBudget, globalBudget := r.tenant[tenantKey], r.global[globalKey]
	request.Exposure.TenantExposureCount = max(request.Exposure.TenantExposureCount, tenantBudget.Exposure)
	request.Exposure.GlobalExposureCount = max(request.Exposure.GlobalExposureCount, globalBudget.Exposure)
	request.Exposure.TenantUnsafeCount = max(request.Exposure.TenantUnsafeCount, tenantBudget.Unsafe)
	request.Exposure.GlobalUnsafeCount = max(request.Exposure.GlobalUnsafeCount, globalBudget.Unsafe)
	assignment, err := experiment.Assign(request)
	if err != nil {
		return experiment.Reservation{}, err
	}
	if assignment.Challenger {
		tenantBudget.Exposure++
		globalBudget.Exposure++
	}
	tenantBudget.Unsafe, globalBudget.Unsafe = request.Exposure.TenantUnsafeCount, request.Exposure.GlobalUnsafeCount
	r.manifests[request.Manifest.ID], r.epochs[epochKey], r.assignments[key] = request.Manifest, request.Manifest.ID, assignment
	r.tenant[tenantKey], r.global[globalKey] = tenantBudget, globalBudget
	return experiment.Reservation{Assignment: assignment, Disposition: experiment.DispositionReserved}, nil
}

func (r *ExperimentRepository) Find(ctx context.Context, tenantID, manifestID, assignmentKeyHash string) (experiment.Assignment, error) {
	if err := ctx.Err(); err != nil {
		return experiment.Assignment{}, err
	}
	if r == nil {
		return experiment.Assignment{}, experiment.ErrExperimentNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.assignments[experimentAssignmentKey(tenantID, manifestID, assignmentKeyHash)]
	if !ok {
		return experiment.Assignment{}, experiment.ErrExperimentNotFound
	}
	return value, nil
}

func experimentAssignmentKey(tenantID, manifestID, keyHash string) string {
	return tenantID + "\x00" + manifestID + "\x00" + keyHash
}
func experimentEpochKey(tenantID, procedureID string, epoch uint64) string {
	return tenantID + "\x00" + procedureID + "\x00" + strconv.FormatUint(epoch, 10)
}
func experimentBudgetKey(scope, window string) string { return scope + "\x00" + window }
