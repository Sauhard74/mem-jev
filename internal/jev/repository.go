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

const judgmentSchemaV1 = "jev-judgment.v1"

var ErrJudgmentNotFound = errors.New("Jev judgment not found")

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
	if err != nil || input.CreatedAt.IsZero() || !input.ReusableUntil.After(input.CreatedAt) || input.ReusableUntil.After(input.CreatedAt.Add(365*24*time.Hour)) || input.Usage.InputTokens < 0 || input.Usage.OutputTokens < 0 || input.Usage.InputTokens > 100_000_000 || input.Usage.OutputTokens > 100_000_000 {
		return JudgmentRecord{}, ErrInvalidJudgment
	}
	features, err := input.Judgment.Features()
	if err != nil {
		return JudgmentRecord{}, err
	}
	record := JudgmentRecord{
		SchemaVersion: judgmentSchemaV1, Key: key, TenantID: input.KeyInput.TenantID,
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
	if record.SchemaVersion != judgmentSchemaV1 || !strings.HasPrefix(record.Key, "jevj_") || !sha256Pattern.MatchString(record.ContentHash) || len(record.CanonicalJSON) == 0 {
		return ErrInvalidJudgment
	}
	rebuilt, err := NewJudgmentRecord(JudgmentRecordInput{
		KeyInput: JudgmentKeyInput{TenantID: record.TenantID, QueryHash: record.QueryHash, ProcedureVersionID: record.ProcedureVersionID, DocumentHash: record.DocumentHash, EnvironmentHash: record.EnvironmentHash, PolicyManifestID: record.PolicyManifestID, RubricManifestID: record.RubricManifestID, Provider: record.Provider, Model: record.Model},
		Judgment: record.Judgment, Usage: record.Usage, CreatedAt: record.CreatedAt, ReusableUntil: record.ReusableUntil,
	})
	if err != nil || rebuilt.Key != record.Key || rebuilt.ContentHash != record.ContentHash || !bytes.Equal(rebuilt.CanonicalJSON, record.CanonicalJSON) {
		return ErrInvalidJudgment
	}
	return nil
}

type Repository interface {
	LookupReusable(context.Context, domain.TenantID, string, time.Time) (JudgmentRecord, error)
	Commit(context.Context, JudgmentRecord) (winner JudgmentRecord, created bool, err error)
}

type MemoryRepository struct {
	mu      sync.RWMutex
	records map[string]JudgmentRecord
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{records: make(map[string]JudgmentRecord)}
}

func (repository *MemoryRepository) LookupReusable(ctx context.Context, tenantID domain.TenantID, key string, now time.Time) (JudgmentRecord, error) {
	if err := ctx.Err(); err != nil {
		return JudgmentRecord{}, err
	}
	if repository == nil || tenantID == "" || !strings.HasPrefix(key, "jevj_") || now.IsZero() {
		return JudgmentRecord{}, ErrJudgmentNotFound
	}
	repository.mu.RLock()
	record, ok := repository.records[judgmentRepositoryKey(tenantID, key)]
	repository.mu.RUnlock()
	if !ok || !now.UTC().Before(record.ReusableUntil) || ValidateJudgmentRecord(record) != nil {
		return JudgmentRecord{}, ErrJudgmentNotFound
	}
	return cloneRecord(record), nil
}

func (repository *MemoryRepository) Commit(ctx context.Context, record JudgmentRecord) (JudgmentRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return JudgmentRecord{}, false, err
	}
	if repository == nil || ValidateJudgmentRecord(record) != nil {
		return JudgmentRecord{}, false, ErrInvalidJudgment
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	key := judgmentRepositoryKey(record.TenantID, record.Key)
	if winner, ok := repository.records[key]; ok {
		if ValidateJudgmentRecord(winner) != nil {
			return JudgmentRecord{}, false, ErrInvalidJudgment
		}
		return cloneRecord(winner), false, nil
	}
	repository.records[key] = cloneRecord(record)
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
