package maintenance

import (
	"bytes"
	"errors"
	"regexp"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

var (
	ErrInvalidJob  = errors.New("invalid maintenance job")
	ErrJobConflict = errors.New("maintenance job conflict")
	ErrJobNotFound = errors.New("maintenance job not found")
	ErrStaleLease  = errors.New("stale maintenance job lease")
	hashPattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type Kind string

const (
	CompatibilityProjection Kind = "compatibility_projection"
	LifecycleEvaluation     Kind = "lifecycle_evaluation"
	ExposureAggregation     Kind = "exposure_aggregation"
	TransientExpiry         Kind = "transient_expiry"
	LifecycleRollback       Kind = "lifecycle_rollback"
	ProjectionRebuild       Kind = "projection_rebuild"
)

type State string

const (
	Pending    State = "pending"
	Leased     State = "leased"
	Completed  State = "completed"
	DeadLetter State = "dead_letter"
)

type Spec struct {
	SchemaVersion      string          `json:"schema_version"`
	ID                 string          `json:"maintenance_job_id"`
	TenantID           domain.TenantID `json:"tenant_id"`
	Kind               Kind            `json:"job_kind"`
	IdempotencyKeyHash string          `json:"idempotency_key_hash"`
	CanonicalPayload   []byte          `json:"canonical_payload"`
	CreatedAt          time.Time       `json:"created_at"`
	ContentHash        string          `json:"content_hash,omitempty"`
	CanonicalJSON      []byte          `json:"-"`
}

func NewSpec(tenantID domain.TenantID, kind Kind, idempotencyKeyHash string, canonicalPayload []byte, createdAt time.Time) (Spec, error) {
	normalizedPayload, _, payloadErr := canonical.MarshalAndHashRaw(canonicalPayload)
	value := Spec{SchemaVersion: "maintenance-job.v1", TenantID: tenantID, Kind: kind, IdempotencyKeyHash: idempotencyKeyHash, CanonicalPayload: append([]byte(nil), canonicalPayload...), CreatedAt: createdAt}
	if payloadErr != nil || !bytes.Equal(normalizedPayload, canonicalPayload) || tenantID == "" || !validKind(kind) || !hashPattern.MatchString(idempotencyKeyHash) || len(canonicalPayload) == 0 || len(canonicalPayload) > 16<<20 || createdAt.IsZero() || !createdAt.Equal(createdAt.UTC()) {
		return Spec{}, ErrInvalidJob
	}
	value.ID, value.ContentHash, value.CanonicalJSON = "", "", nil
	encoded, hash, err := canonical.MarshalAndHash(value)
	if err != nil {
		return Spec{}, err
	}
	value.ID, value.ContentHash, value.CanonicalJSON = "mjob_"+hash, hash, encoded
	return value, nil
}

func ValidateSpec(value Spec) error {
	rebuilt, err := NewSpec(value.TenantID, value.Kind, value.IdempotencyKeyHash, value.CanonicalPayload, value.CreatedAt)
	if err != nil || rebuilt.ID != value.ID || rebuilt.ContentHash != value.ContentHash || !bytes.Equal(rebuilt.CanonicalJSON, value.CanonicalJSON) {
		return ErrInvalidJob
	}
	return nil
}

type Lease struct {
	Spec
	AttemptCount   uint32
	FencingToken   uint64
	LeaseOwner     string
	LeaseExpiresAt time.Time
}

type ClaimRequest struct {
	WorkerID      string
	Kinds         []Kind
	Limit         int
	LeaseDuration time.Duration
}
type FinishRequest struct {
	TenantID        domain.TenantID
	JobID, WorkerID string
	FencingToken    uint64
}
type FailRequest struct {
	FinishRequest
	ErrorCode  string
	RetryAt    time.Time
	DeadLetter bool
}

func validKind(value Kind) bool {
	return value == CompatibilityProjection || value == LifecycleEvaluation || value == ExposureAggregation || value == TransientExpiry || value == LifecycleRollback || value == ProjectionRebuild
}

// ValidKind reports whether value is understood by this binary.
func ValidKind(value Kind) bool { return validKind(value) }

// ValidateKinds rejects empty, unknown, and duplicate claim filters.
func ValidateKinds(values []Kind) error {
	if len(values) == 0 {
		return ErrInvalidJob
	}
	seen := make(map[Kind]struct{}, len(values))
	for _, value := range values {
		if !validKind(value) {
			return ErrInvalidJob
		}
		if _, exists := seen[value]; exists {
			return ErrInvalidJob
		}
		seen[value] = struct{}{}
	}
	return nil
}
