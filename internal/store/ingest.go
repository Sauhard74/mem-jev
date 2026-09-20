package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/domain"
)

var (
	ErrInvalidCommit       = errors.New("invalid ingest commit")
	ErrIdempotencyConflict = errors.New("idempotency key was reused with different content")
	ErrTraceConflict       = errors.New("trace identity was reused with different content")
)

type IngestDisposition string

const (
	DispositionAccepted  IngestDisposition = "accepted"
	DispositionDuplicate IngestDisposition = "duplicate"
)

type IngestRepository interface {
	Commit(context.Context, CommitIngestRequest) (IngestReceipt, error)
}

type CommitIngestRequest struct {
	TenantID           domain.TenantID
	IdempotencyKeyHash string
	Batch              domain.CanonicalBatch
	Archive            archive.Object
}

type IngestReceipt struct {
	ID          domain.ReceiptID
	TenantID    domain.TenantID
	TraceID     domain.TraceID
	ContentHash string
	WorkflowID  string
	Disposition IngestDisposition
	CreatedAt   time.Time
}

type AggregateCounts struct {
	Receipts   int
	Traces     int
	Events     int
	Archives   int
	OutboxJobs int
}

type OpError struct {
	Operation string
	Retryable bool
	Err       error
}

func (e *OpError) Error() string {
	return fmt.Sprintf("store %s: %v", e.Operation, e.Err)
}

func (e *OpError) Unwrap() error { return e.Err }

var lowercaseSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)

func ValidateCommitRequest(request CommitIngestRequest) error {
	if request.TenantID == "" || request.Batch.TenantID != request.TenantID ||
		request.Batch.SchemaVersion == "" || request.Batch.Trace.ID == "" ||
		!lowercaseSHA256.MatchString(request.IdempotencyKeyHash) ||
		!lowercaseSHA256.MatchString(request.Batch.Hash) || len(request.Batch.CanonicalJSON) == 0 ||
		len(request.Batch.Events) == 0 || len(request.Batch.Events) > 5000 {
		return ErrInvalidCommit
	}
	sum := sha256.Sum256(request.Batch.CanonicalJSON)
	if hex.EncodeToString(sum[:]) != request.Batch.Hash ||
		request.Archive.Hash != request.Batch.Hash ||
		request.Archive.Size != int64(len(request.Batch.CanonicalJSON)) {
		return ErrInvalidCommit
	}
	expectedKey, err := archive.KeyFor(request.TenantID, request.Batch.SchemaVersion, request.Batch.Hash)
	if err != nil || expectedKey != request.Archive.Key {
		return ErrInvalidCommit
	}
	if !validDerivedID(string(request.Batch.Trace.ID), "tr_") {
		return ErrInvalidCommit
	}
	seen := make(map[domain.EventID]struct{}, len(request.Batch.Events))
	for index, event := range request.Batch.Events {
		if event.Position != uint32(index) || !validDerivedID(string(event.ID), "ev_") {
			return ErrInvalidCommit
		}
		if _, exists := seen[event.ID]; exists {
			return ErrInvalidCommit
		}
		seen[event.ID] = struct{}{}
	}
	return nil
}

func ReceiptID(tenantID domain.TenantID, idempotencyKeyHash string) domain.ReceiptID {
	return domain.ReceiptID("rcpt_" + hashIdentity(string(tenantID)+"\x00"+idempotencyKeyHash))
}

func WorkflowID(tenantID domain.TenantID, traceID domain.TraceID) string {
	tenantHash := hashIdentity(string(tenantID))[:16]
	return fmt.Sprintf("synthesize/%s/%s/v1", tenantHash, traceID)
}

func EventContentHash(eventID domain.EventID) string {
	return strings.TrimPrefix(string(eventID), "ev_")
}

func TraceContentHash(traceID domain.TraceID) string {
	return strings.TrimPrefix(string(traceID), "tr_")
}

func validDerivedID(value, prefix string) bool {
	return strings.HasPrefix(value, prefix) && lowercaseSHA256.MatchString(strings.TrimPrefix(value, prefix))
}

func hashIdentity(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
