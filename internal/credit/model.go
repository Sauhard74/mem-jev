package credit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"sort"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

var ErrInvalidCredit = errors.New("invalid outcome credit")

var hashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type Class string

const (
	CausalSuccess     Class = "causal_success"
	AssociatedSuccess Class = "associated_success"
	CausalFailure     Class = "causal_failure"
	AssociatedFailure Class = "associated_failure"
	Unattributable    Class = "unattributable"
)

type RuleManifest struct {
	SchemaVersion             string `json:"schema_version"`
	Version                   string `json:"version"`
	MaximumClockSkewSeconds   uint32 `json:"maximum_clock_skew_seconds"`
	RequireCompleteCausalPlan bool   `json:"require_complete_causal_plan"`
	RequireGoalEvidence       bool   `json:"require_goal_evidence"`
	ID                        string `json:"-"`
	ContentHash               string `json:"-"`
	CanonicalJSON             []byte `json:"-"`
}

func NewRuleManifest(version string, maximumClockSkew time.Duration) (RuleManifest, error) {
	if version == "" || maximumClockSkew < 0 || maximumClockSkew > 5*time.Minute {
		return RuleManifest{}, ErrInvalidCredit
	}
	manifest := RuleManifest{SchemaVersion: "credit-rule-manifest.v1", Version: version, MaximumClockSkewSeconds: uint32(maximumClockSkew / time.Second), RequireCompleteCausalPlan: true, RequireGoalEvidence: true}
	encoded, hash, err := canonical.MarshalAndHash(manifest)
	if err != nil {
		return RuleManifest{}, err
	}
	manifest.ID, manifest.ContentHash, manifest.CanonicalJSON = "crman_"+hash, hash, encoded
	return manifest, nil
}

func ValidateRuleManifest(manifest RuleManifest) error {
	rebuilt, err := NewRuleManifest(manifest.Version, time.Duration(manifest.MaximumClockSkewSeconds)*time.Second)
	if err != nil || manifest.SchemaVersion != rebuilt.SchemaVersion || manifest.ID != rebuilt.ID || manifest.ContentHash != rebuilt.ContentHash || !bytes.Equal(manifest.CanonicalJSON, rebuilt.CanonicalJSON) || !manifest.RequireCompleteCausalPlan || !manifest.RequireGoalEvidence {
		return ErrInvalidCredit
	}
	return nil
}

type Record struct {
	SchemaVersion      string           `json:"schema_version"`
	ID                 string           `json:"outcome_credit_id"`
	TenantID           domain.TenantID  `json:"tenant_id"`
	OutcomeID          domain.OutcomeID `json:"outcome_id"`
	InjectionID        string           `json:"injection_id"`
	TaskExecutionID    string           `json:"task_execution_id"`
	ProcedureVersionID string           `json:"procedure_version_id,omitempty"`
	Class              Class            `json:"credit_class"`
	RuleManifestID     string           `json:"rule_manifest_id"`
	SelectionHash      string           `json:"selection_hash"`
	ReasonCodes        []string         `json:"reason_codes"`
	CreatedAt          time.Time        `json:"created_at"`
	ExpiresAt          time.Time        `json:"expires_at"`
	ContentHash        string           `json:"content_hash,omitempty"`
	CanonicalJSON      []byte           `json:"-"`
}

func newRecord(value Record) (Record, error) {
	value.SchemaVersion = "outcome-credit.v1"
	value.ID, value.ContentHash, value.CanonicalJSON = "", "", nil
	sort.Strings(value.ReasonCodes)
	if value.TenantID == "" || value.OutcomeID == "" || value.InjectionID == "" || value.TaskExecutionID == "" || value.RuleManifestID == "" || !hashPattern.MatchString(value.SelectionHash) || !validClass(value.Class) || value.CreatedAt.IsZero() || !value.CreatedAt.Equal(value.CreatedAt.UTC()) || !value.ExpiresAt.After(value.CreatedAt) {
		return Record{}, ErrInvalidCredit
	}
	for index, code := range value.ReasonCodes {
		if code == "" || index > 0 && value.ReasonCodes[index-1] >= code {
			return Record{}, ErrInvalidCredit
		}
	}
	identity := sha256.Sum256([]byte(string(value.TenantID) + "\x00" + string(value.OutcomeID)))
	value.ID = "ocred_" + hex.EncodeToString(identity[:])
	encoded, hash, err := canonical.MarshalAndHash(value)
	if err != nil {
		return Record{}, err
	}
	value.ContentHash, value.CanonicalJSON = hash, encoded
	return value, nil
}

func Validate(record Record) error {
	copyOf := record
	copyOf.ID, copyOf.ContentHash, copyOf.CanonicalJSON = "", "", nil
	rebuilt, err := newRecord(copyOf)
	if err != nil || rebuilt.ID != record.ID || rebuilt.ContentHash != record.ContentHash || !bytes.Equal(rebuilt.CanonicalJSON, record.CanonicalJSON) {
		return ErrInvalidCredit
	}
	return nil
}

func validClass(value Class) bool {
	return value == CausalSuccess || value == AssociatedSuccess || value == CausalFailure || value == AssociatedFailure || value == Unattributable
}
