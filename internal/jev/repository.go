package jev

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

const (
	judgmentSchemaV1 = "jev-judgment.v1"
	judgmentSchemaV2 = "jev-judgment.v2"
)

var ErrJudgmentNotFound = errors.New("jev judgment not found")

type JudgmentRecordInput struct {
	KeyInput      JudgmentKeyInput
	Judgment      Judgment
	Usage         Usage
	CreatedAt     time.Time
	ReusableUntil time.Time
}

type JudgmentRecord struct {
	SchemaVersion      string          `json:"schema_version"`
	Key                string          `json:"key"`
	BaseKey            string          `json:"base_key,omitempty"`
	PredecessorHash    string          `json:"predecessor_hash,omitempty"`
	TenantID           domain.TenantID `json:"tenant_id"`
	QueryHash          string          `json:"query_hash"`
	ProcedureVersionID string          `json:"procedure_version_id"`
	DocumentHash       string          `json:"document_hash"`
	EnvironmentHash    string          `json:"environment_hash"`
	PolicyManifestID   string          `json:"policy_manifest_id"`
	RubricManifestID   string          `json:"rubric_manifest_id"`
	Provider           string          `json:"provider"`
	Model              string          `json:"model"`
	Judgment           Judgment        `json:"judgment"`
	Features           []Feature       `json:"features"`
	Usage              Usage           `json:"usage"`
	CreatedAt          time.Time       `json:"created_at"`
	ReusableUntil      time.Time       `json:"reusable_until"`
	ContentHash        string          `json:"-"`
	CanonicalJSON      []byte          `json:"-"`
}

func NewJudgmentRecord(input JudgmentRecordInput) (JudgmentRecord, error) {
	key, err := NewJudgmentKey(input.KeyInput)
	baseInput := input.KeyInput
	baseInput.PredecessorHash = ""
	baseKey, baseErr := NewJudgmentKey(baseInput)
	if err != nil || baseErr != nil || input.CreatedAt.IsZero() || !input.ReusableUntil.After(input.CreatedAt) || input.ReusableUntil.After(input.CreatedAt.Add(365*24*time.Hour)) || input.Usage.InputTokens < 0 || input.Usage.OutputTokens < 0 || input.Usage.InputTokens > 100_000_000 || input.Usage.OutputTokens > 100_000_000 {
		return JudgmentRecord{}, ErrInvalidJudgment
	}
	features, err := input.Judgment.Features()
	if err != nil {
		return JudgmentRecord{}, err
	}
	record := JudgmentRecord{
		SchemaVersion: judgmentSchemaV2, Key: key, BaseKey: baseKey, PredecessorHash: input.KeyInput.PredecessorHash, TenantID: input.KeyInput.TenantID,
		QueryHash: input.KeyInput.QueryHash, ProcedureVersionID: strings.TrimSpace(input.KeyInput.ProcedureVersionID),
		DocumentHash: input.KeyInput.DocumentHash, EnvironmentHash: input.KeyInput.EnvironmentHash,
		PolicyManifestID: strings.TrimSpace(input.KeyInput.PolicyManifestID), RubricManifestID: strings.TrimSpace(input.KeyInput.RubricManifestID),
		Provider: strings.TrimSpace(input.KeyInput.Provider), Model: strings.TrimSpace(input.KeyInput.Model),
		Judgment: cloneJudgment(input.Judgment), Features: append([]Feature(nil), features...), Usage: input.Usage,
		CreatedAt: input.CreatedAt.UTC(), ReusableUntil: input.ReusableUntil.UTC(),
	}
	encoded, hash, err := canonical.MarshalAndHash(record)
	if err != nil {
		return JudgmentRecord{}, fmt.Errorf("canonicalize Jev judgment: %w", err)
	}
	record.ContentHash, record.CanonicalJSON = hash, encoded
	return record, nil
}

func ValidateJudgmentRecord(record JudgmentRecord) error {
	if record.SchemaVersion != judgmentSchemaV1 && record.SchemaVersion != judgmentSchemaV2 || !strings.HasPrefix(record.Key, "jevj_") || !sha256Pattern.MatchString(record.ContentHash) || len(record.CanonicalJSON) == 0 {
		return ErrInvalidJudgment
	}
	if record.SchemaVersion == judgmentSchemaV1 {
		return validateLegacyJudgmentRecord(record)
	}
	rebuilt, err := NewJudgmentRecord(JudgmentRecordInput{
		KeyInput: JudgmentKeyInput{TenantID: record.TenantID, QueryHash: record.QueryHash, ProcedureVersionID: record.ProcedureVersionID, DocumentHash: record.DocumentHash, EnvironmentHash: record.EnvironmentHash, PolicyManifestID: record.PolicyManifestID, RubricManifestID: record.RubricManifestID, Provider: record.Provider, Model: record.Model, PredecessorHash: record.PredecessorHash},
		Judgment: record.Judgment, Usage: record.Usage, CreatedAt: record.CreatedAt, ReusableUntil: record.ReusableUntil,
	})
	if err != nil || rebuilt.Key != record.Key || rebuilt.ContentHash != record.ContentHash || !bytes.Equal(rebuilt.CanonicalJSON, record.CanonicalJSON) {
		return ErrInvalidJudgment
	}
	return nil
}

func validateLegacyJudgmentRecord(record JudgmentRecord) error {
	if record.BaseKey != "" || record.PredecessorHash != "" {
		return ErrInvalidJudgment
	}
	keyInput := JudgmentKeyInput{TenantID: record.TenantID, QueryHash: record.QueryHash, ProcedureVersionID: record.ProcedureVersionID, DocumentHash: record.DocumentHash, EnvironmentHash: record.EnvironmentHash, PolicyManifestID: record.PolicyManifestID, RubricManifestID: record.RubricManifestID, Provider: record.Provider, Model: record.Model}
	key, err := NewJudgmentKey(keyInput)
	if err != nil || key != record.Key || record.CreatedAt.IsZero() || !record.ReusableUntil.After(record.CreatedAt) || record.Usage.InputTokens < 0 || record.Usage.OutputTokens < 0 {
		return ErrInvalidJudgment
	}
	if _, err = record.Judgment.Features(); err != nil {
		return ErrInvalidJudgment
	}
	copyOfRecord := record
	copyOfRecord.ContentHash, copyOfRecord.CanonicalJSON = "", nil
	encoded, hash, err := canonical.MarshalAndHash(copyOfRecord)
	if err != nil || hash != record.ContentHash || !bytes.Equal(encoded, record.CanonicalJSON) {
		return ErrInvalidJudgment
	}
	return nil
}

type Repository interface {
	LookupReusable(context.Context, domain.TenantID, string, time.Time) (JudgmentRecord, error)
	Current(context.Context, domain.TenantID, string) (JudgmentRecord, error)
	Commit(context.Context, string, string, JudgmentRecord) (winner JudgmentRecord, created bool, err error)
}

type MemoryRepository struct {
	mu      sync.RWMutex
	records map[string]JudgmentRecord
	heads   map[string]string
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{records: make(map[string]JudgmentRecord), heads: make(map[string]string)}
}

func (repository *MemoryRepository) LookupReusable(ctx context.Context, tenantID domain.TenantID, key string, now time.Time) (JudgmentRecord, error) {
	if err := ctx.Err(); err != nil {
		return JudgmentRecord{}, err
	}
	if repository == nil || tenantID == "" || !strings.HasPrefix(key, "jevj_") || now.IsZero() {
		return JudgmentRecord{}, ErrJudgmentNotFound
	}
	record, err := repository.Current(ctx, tenantID, key)
	if err != nil {
		if !errors.Is(err, ErrJudgmentNotFound) {
			return JudgmentRecord{}, err
		}
		return JudgmentRecord{}, ErrJudgmentNotFound
	}
	if !now.UTC().Before(record.ReusableUntil) {
		return JudgmentRecord{}, ErrJudgmentNotFound
	}
	return cloneRecord(record), nil
}

func (repository *MemoryRepository) Current(ctx context.Context, tenantID domain.TenantID, baseKey string) (JudgmentRecord, error) {
	if err := ctx.Err(); err != nil {
		return JudgmentRecord{}, err
	}
	if repository == nil || tenantID == "" || !strings.HasPrefix(baseKey, "jevj_") {
		return JudgmentRecord{}, ErrJudgmentNotFound
	}
	repository.mu.RLock()
	key := repository.heads[judgmentRepositoryKey(tenantID, baseKey)]
	if key == "" {
		key = baseKey
	}
	record, ok := repository.records[judgmentRepositoryKey(tenantID, key)]
	repository.mu.RUnlock()
	if !ok || ValidateJudgmentRecord(record) != nil {
		return JudgmentRecord{}, ErrJudgmentNotFound
	}
	return cloneRecord(record), nil
}

func (repository *MemoryRepository) Commit(ctx context.Context, baseKey, expectedPredecessorHash string, record JudgmentRecord) (JudgmentRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return JudgmentRecord{}, false, err
	}
	if repository == nil || ValidateJudgmentRecord(record) != nil || record.BaseKey != baseKey || record.PredecessorHash != expectedPredecessorHash {
		return JudgmentRecord{}, false, ErrInvalidJudgment
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	headKey := judgmentRepositoryKey(record.TenantID, baseKey)
	currentKey := repository.heads[headKey]
	if currentKey == "" {
		currentKey = baseKey
	}
	if current, ok := repository.records[judgmentRepositoryKey(record.TenantID, currentKey)]; ok {
		if current.ContentHash != expectedPredecessorHash {
			return cloneRecord(current), false, nil
		}
	} else if expectedPredecessorHash != "" {
		return JudgmentRecord{}, false, ErrInvalidJudgment
	}
	key := judgmentRepositoryKey(record.TenantID, record.Key)
	if existing, ok := repository.records[key]; ok && existing.ContentHash != record.ContentHash {
		return JudgmentRecord{}, false, ErrInvalidJudgment
	}
	repository.records[key] = cloneRecord(record)
	repository.heads[headKey] = record.Key
	return cloneRecord(record), true, nil
}

func judgmentRepositoryKey(tenantID domain.TenantID, key string) string {
	return string(tenantID) + "\x00" + key
}

func cloneRecord(record JudgmentRecord) JudgmentRecord {
	record.Judgment = cloneJudgment(record.Judgment)
	record.Features = append([]Feature(nil), record.Features...)
	record.CanonicalJSON = append([]byte(nil), record.CanonicalJSON...)
	return record
}

func cloneJudgment(judgment Judgment) Judgment {
	judgment.IntentFit.ProbabilitiesMicros = append([]int32(nil), judgment.IntentFit.ProbabilitiesMicros...)
	judgment.TaskCoverage.ProbabilitiesMicros = append([]int32(nil), judgment.TaskCoverage.ProbabilitiesMicros...)
	return judgment
}

var _ Repository = (*MemoryRepository)(nil)
