package rebuild

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/observability"
)

var (
	ErrInvalidDerivedSnapshot = errors.New("invalid derived projection snapshot")
	digestPattern             = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type RecordDigest struct {
	ID          string `json:"id"`
	ContentHash string `json:"content_hash"`
}

type DerivedInput struct {
	TenantID           domain.TenantID
	ProjectionEpoch    uint64
	Compatibility      []RecordDigest
	SelectionCredits   []RecordDigest
	LifecycleDecisions []RecordDigest
	LifecycleHeads     []RecordDigest
}

type DerivedSnapshot struct {
	SchemaVersion      string          `json:"schema_version"`
	TenantID           domain.TenantID `json:"tenant_id"`
	ProjectionEpoch    uint64          `json:"projection_epoch"`
	Compatibility      []RecordDigest  `json:"compatibility"`
	SelectionCredits   []RecordDigest  `json:"selection_credits"`
	LifecycleDecisions []RecordDigest  `json:"lifecycle_decisions"`
	LifecycleHeads     []RecordDigest  `json:"lifecycle_heads"`
	ID                 string          `json:"-"`
	ContentHash        string          `json:"-"`
	CanonicalJSON      []byte          `json:"-"`
}

type ActivationPermit struct {
	SchemaVersion     string          `json:"schema_version"`
	TenantID          domain.TenantID `json:"tenant_id"`
	ProjectionEpoch   uint64          `json:"projection_epoch"`
	StoredSnapshotID  string          `json:"stored_snapshot_id"`
	RebuiltSnapshotID string          `json:"rebuilt_snapshot_id"`
	AuthorizedAt      time.Time       `json:"authorized_at"`
	ID                string          `json:"-"`
	ContentHash       string          `json:"-"`
	CanonicalJSON     []byte          `json:"-"`
}

func BuildDerivedSnapshot(input DerivedInput) (DerivedSnapshot, error) {
	if input.TenantID == "" || input.ProjectionEpoch == 0 {
		return DerivedSnapshot{}, ErrInvalidDerivedSnapshot
	}
	compatibility, err := normalizeDigests(input.Compatibility)
	if err != nil {
		return DerivedSnapshot{}, err
	}
	credits, err := normalizeDigests(input.SelectionCredits)
	if err != nil {
		return DerivedSnapshot{}, err
	}
	decisions, err := normalizeDigests(input.LifecycleDecisions)
	if err != nil {
		return DerivedSnapshot{}, err
	}
	heads, err := normalizeDigests(input.LifecycleHeads)
	if err != nil {
		return DerivedSnapshot{}, err
	}
	value := DerivedSnapshot{SchemaVersion: "derived-projection-snapshot.v1", TenantID: input.TenantID, ProjectionEpoch: input.ProjectionEpoch, Compatibility: compatibility, SelectionCredits: credits, LifecycleDecisions: decisions, LifecycleHeads: heads}
	encoded, hash, err := canonical.MarshalAndHash(value)
	if err != nil {
		return DerivedSnapshot{}, err
	}
	value.ID, value.ContentHash, value.CanonicalJSON = "dps_"+hash, hash, encoded
	return value, nil
}

func CompareDerivedSnapshots(ctx context.Context, stored, rebuilt DerivedSnapshot, authorizedAt time.Time) (ActivationPermit, error) {
	if ValidateDerivedSnapshot(stored) != nil || ValidateDerivedSnapshot(rebuilt) != nil || authorizedAt.IsZero() || !authorizedAt.Equal(authorizedAt.UTC()) || stored.TenantID != rebuilt.TenantID || stored.ProjectionEpoch != rebuilt.ProjectionEpoch {
		return ActivationPermit{}, ErrInvalidDerivedSnapshot
	}
	for _, component := range []struct {
		name        string
		left, right []RecordDigest
	}{{"compatibility", stored.Compatibility, rebuilt.Compatibility}, {"selection_credit", stored.SelectionCredits, rebuilt.SelectionCredits}, {"lifecycle_decision", stored.LifecycleDecisions, rebuilt.LifecycleDecisions}, {"lifecycle_head", stored.LifecycleHeads, rebuilt.LifecycleHeads}} {
		if !equalDigests(component.left, component.right) {
			observability.RecordRebuildMismatch(ctx, component.name)
			return ActivationPermit{}, ErrProjectionMismatch
		}
	}
	permit := ActivationPermit{SchemaVersion: "derived-activation-permit.v1", TenantID: stored.TenantID, ProjectionEpoch: stored.ProjectionEpoch, StoredSnapshotID: stored.ID, RebuiltSnapshotID: rebuilt.ID, AuthorizedAt: authorizedAt}
	encoded, hash, err := canonical.MarshalAndHash(permit)
	if err != nil {
		return ActivationPermit{}, err
	}
	permit.ID, permit.ContentHash, permit.CanonicalJSON = "dpermit_"+hash, hash, encoded
	return permit, nil
}

func equalDigests(left, right []RecordDigest) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func ValidateDerivedSnapshot(value DerivedSnapshot) error {
	rebuilt, err := BuildDerivedSnapshot(DerivedInput{TenantID: value.TenantID, ProjectionEpoch: value.ProjectionEpoch, Compatibility: value.Compatibility, SelectionCredits: value.SelectionCredits, LifecycleDecisions: value.LifecycleDecisions, LifecycleHeads: value.LifecycleHeads})
	if err != nil || rebuilt.ID != value.ID || rebuilt.ContentHash != value.ContentHash || !bytes.Equal(rebuilt.CanonicalJSON, value.CanonicalJSON) {
		return ErrInvalidDerivedSnapshot
	}
	return nil
}

func HydrateDerivedSnapshot(canonicalJSON []byte, id, contentHash string) (DerivedSnapshot, error) {
	if len(canonicalJSON) == 0 || len(canonicalJSON) > 32<<20 || id == "" || !digestPattern.MatchString(contentHash) {
		return DerivedSnapshot{}, ErrInvalidDerivedSnapshot
	}
	var value DerivedSnapshot
	if err := json.Unmarshal(canonicalJSON, &value); err != nil {
		return DerivedSnapshot{}, ErrInvalidDerivedSnapshot
	}
	value.ID, value.ContentHash, value.CanonicalJSON = id, contentHash, append([]byte(nil), canonicalJSON...)
	if ValidateDerivedSnapshot(value) != nil {
		return DerivedSnapshot{}, ErrInvalidDerivedSnapshot
	}
	return value, nil
}

func ValidateActivationPermit(value ActivationPermit) error {
	if value.SchemaVersion != "derived-activation-permit.v1" || value.TenantID == "" || value.ProjectionEpoch == 0 || value.StoredSnapshotID == "" || value.RebuiltSnapshotID == "" || value.AuthorizedAt.IsZero() || !value.AuthorizedAt.Equal(value.AuthorizedAt.UTC()) || value.ID != "dpermit_"+value.ContentHash || !digestPattern.MatchString(value.ContentHash) {
		return ErrInvalidDerivedSnapshot
	}
	copyOf := value
	copyOf.ID, copyOf.ContentHash, copyOf.CanonicalJSON = "", "", nil
	encoded, hash, err := canonical.MarshalAndHash(copyOf)
	if err != nil || hash != value.ContentHash || !bytes.Equal(encoded, value.CanonicalJSON) {
		return ErrInvalidDerivedSnapshot
	}
	return nil
}

func normalizeDigests(source []RecordDigest) ([]RecordDigest, error) {
	result := append([]RecordDigest(nil), source...)
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	for index, value := range result {
		if value.ID == "" || !digestPattern.MatchString(value.ContentHash) || index > 0 && result[index-1].ID >= value.ID {
			return nil, ErrInvalidDerivedSnapshot
		}
	}
	return result, nil
}
