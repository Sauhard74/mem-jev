package surreal

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/store"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

var errInjectedFailure = errors.New("injected commit failure")

type failurePoint uint8

const (
	failureNone failurePoint = iota
	failureAfterEvents
	failureAfterRetrievalChildren
)

type IngestRepository struct {
	db      *surrealdb.DB
	failure failurePoint
	now     func() time.Time
}

func NewIngestRepository(db *surrealdb.DB) *IngestRepository {
	return newIngestRepository(db, failureNone)
}

func newIngestRepository(db *surrealdb.DB, failure failurePoint) *IngestRepository {
	return &IngestRepository{db: db, failure: failure, now: time.Now}
}

func (r *IngestRepository) Commit(ctx context.Context, request store.CommitIngestRequest) (store.IngestReceipt, error) {
	if err := ctx.Err(); err != nil {
		return store.IngestReceipt{}, err
	}
	if r.db == nil {
		return store.IngestReceipt{}, errors.New("SurrealDB ingest repository is not configured")
	}
	if err := store.ValidateCommitRequest(request); err != nil {
		return store.IngestReceipt{}, err
	}

	const maxAttempts = 8
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		receipt, err := r.commitOnce(ctx, request)
		if err == nil {
			return receipt, nil
		}
		lastErr = err
		resolved, found, lookupErr := r.findReceipt(ctx, request.TenantID, request.IdempotencyKeyHash)
		if lookupErr != nil {
			return store.IngestReceipt{}, databaseFailure("resolve failed ingest commit", lookupErr)
		}
		if found {
			return resolveExisting(resolved, request.Batch.Hash)
		}
		if !surrealdb.IsTransactionConflict(err) {
			return store.IngestReceipt{}, databaseFailure("commit ingest ledger", err)
		}
		observability.RecordTransactionRetry(ctx)
		if err := waitForRetry(ctx, attempt); err != nil {
			return store.IngestReceipt{}, err
		}
	}
	return store.IngestReceipt{}, &store.OpError{Operation: "commit ingest ledger after conflict retries", Retryable: true, Err: lastErr}
}

func databaseFailure(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, store.ErrIdempotencyConflict) || errors.Is(err, store.ErrTraceConflict) || errors.Is(err, store.ErrInvalidCommit) {
		return err
	}
	retryable := !surrealdb.IsParseError(err) && !surrealdb.IsDeserialization(err) &&
		!surrealdb.IsNotAllowed(err) && !surrealdb.IsInvalidAuth(err)
	return &store.OpError{Operation: operation, Retryable: retryable, Err: err}
}

func (r *IngestRepository) commitOnce(ctx context.Context, request store.CommitIngestRequest) (_ store.IngestReceipt, err error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return store.IngestReceipt{}, fmt.Errorf("begin ingest commit: %w", err)
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()

	existing, found, err := findReceipt(ctx, tx, request.TenantID, request.IdempotencyKeyHash)
	if err != nil {
		return store.IngestReceipt{}, fmt.Errorf("find ingest receipt: %w", err)
	}
	if found {
		if cancelErr := tx.Cancel(ctx); cancelErr != nil {
			return store.IngestReceipt{}, fmt.Errorf("cancel duplicate ingest transaction: %w", cancelErr)
		}
		return resolveExisting(existing, request.Batch.Hash)
	}
	existingArchiveKey, traceFound, err := findTraceArchiveKey(ctx, tx, request.TenantID, request.Batch.Trace.ID)
	if err != nil {
		return store.IngestReceipt{}, fmt.Errorf("find trace run: %w", err)
	}
	if traceFound && existingArchiveKey != request.Archive.Key {
		if cancelErr := tx.Cancel(ctx); cancelErr != nil {
			return store.IngestReceipt{}, fmt.Errorf("cancel conflicting trace transaction: %w", cancelErr)
		}
		return store.IngestReceipt{}, store.ErrTraceConflict
	}

	createdAt := r.now().UTC()
	receipt := store.IngestReceipt{
		ID:          store.ReceiptID(request.TenantID, request.IdempotencyKeyHash),
		TenantID:    request.TenantID,
		TraceID:     request.Batch.Trace.ID,
		ContentHash: request.Batch.Hash,
		WorkflowID:  store.WorkflowID(request.TenantID, request.Batch.Trace.ID),
		Disposition: store.DispositionAccepted,
		CreatedAt:   createdAt,
	}
	if err := createReceipt(ctx, tx, request, receipt); err != nil {
		return store.IngestReceipt{}, err
	}
	if traceFound {
		if err := tx.Commit(ctx); err != nil {
			return store.IngestReceipt{}, fmt.Errorf("commit aggregate reuse receipt: %w", err)
		}
		return receipt, nil
	}
	if err := createTrace(ctx, tx, request, createdAt); err != nil {
		return store.IngestReceipt{}, err
	}
	if err := createArchive(ctx, tx, request, createdAt); err != nil {
		return store.IngestReceipt{}, err
	}
	for _, event := range request.Batch.Events {
		if err := createEvent(ctx, tx, request, event, createdAt); err != nil {
			return store.IngestReceipt{}, err
		}
	}
	if r.failure == failureAfterEvents {
		return store.IngestReceipt{}, errInjectedFailure
	}
	if err := createOutbox(ctx, tx, request, receipt, createdAt); err != nil {
		return store.IngestReceipt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.IngestReceipt{}, fmt.Errorf("commit ingest transaction: %w", err)
	}
	observability.RecordOutboxCreated(ctx)
	return receipt, nil
}

type traceArchiveRow struct {
	ArchiveKey string `json:"archive_key"`
}

func findTraceArchiveKey[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, tenantID domain.TenantID, traceID domain.TraceID) (archive.Key, bool, error) {
	results, err := surrealdb.Query[[]traceArchiveRow](ctx, sender, `
		SELECT archive_key
		FROM trace_run
		WHERE tenant_id = $tenant_id AND trace_id = $trace_id
		LIMIT 1`, map[string]any{
		"tenant_id": string(tenantID),
		"trace_id":  string(traceID),
	})
	if err != nil {
		return "", false, err
	}
	if results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return "", false, nil
	}
	return archive.Key((*results)[0].Result[0].ArchiveKey), true, nil
}

type receiptRow struct {
	ReceiptID   string    `json:"receipt_id"`
	TenantID    string    `json:"tenant_id"`
	TraceID     string    `json:"trace_id"`
	ContentHash string    `json:"content_hash"`
	CreatedAt   time.Time `json:"created_at"`
}

func (r *IngestRepository) findReceipt(ctx context.Context, tenantID domain.TenantID, keyHash string) (store.IngestReceipt, bool, error) {
	return findReceipt(ctx, r.db, tenantID, keyHash)
}

func findReceipt[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, tenantID domain.TenantID, keyHash string) (store.IngestReceipt, bool, error) {
	results, err := surrealdb.Query[[]receiptRow](ctx, sender, `
		SELECT receipt_id, tenant_id, trace_id, content_hash, created_at
		FROM ingest_receipt
		WHERE tenant_id = $tenant_id AND idempotency_key_hash = $key_hash
		LIMIT 1`, map[string]any{
		"tenant_id": string(tenantID),
		"key_hash":  keyHash,
	})
	if err != nil {
		return store.IngestReceipt{}, false, err
	}
	if results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return store.IngestReceipt{}, false, nil
	}
	row := (*results)[0].Result[0]
	receipt := store.IngestReceipt{
		ID:          domain.ReceiptID(row.ReceiptID),
		TenantID:    domain.TenantID(row.TenantID),
		TraceID:     domain.TraceID(row.TraceID),
		ContentHash: row.ContentHash,
		WorkflowID:  store.WorkflowID(domain.TenantID(row.TenantID), domain.TraceID(row.TraceID)),
		Disposition: store.DispositionAccepted,
		CreatedAt:   row.CreatedAt,
	}
	return receipt, true, nil
}

func resolveExisting(receipt store.IngestReceipt, contentHash string) (store.IngestReceipt, error) {
	if receipt.ContentHash != contentHash {
		return store.IngestReceipt{}, store.ErrIdempotencyConflict
	}
	receipt.Disposition = store.DispositionDuplicate
	return receipt, nil
}

func createReceipt(ctx context.Context, tx *surrealdb.Transaction, request store.CommitIngestRequest, receipt store.IngestReceipt) error {
	return execute(ctx, tx, "create ingest receipt", "CREATE ONLY ingest_receipt CONTENT $record", map[string]any{"record": map[string]any{
		"tenant_id":            string(request.TenantID),
		"receipt_id":           string(receipt.ID),
		"trace_id":             string(receipt.TraceID),
		"idempotency_key_hash": request.IdempotencyKeyHash,
		"disposition":          string(store.DispositionAccepted),
		"created_at":           receipt.CreatedAt,
		"schema_version":       request.Batch.SchemaVersion,
		"content_hash":         request.Batch.Hash,
	}})
}

func createTrace(ctx context.Context, tx *surrealdb.Transaction, request store.CommitIngestRequest, createdAt time.Time) error {
	return execute(ctx, tx, "create trace run", "CREATE ONLY trace_run CONTENT $record", map[string]any{"record": map[string]any{
		"tenant_id":      string(request.TenantID),
		"trace_id":       string(request.Batch.Trace.ID),
		"event_count":    len(request.Batch.Events),
		"archive_key":    string(request.Archive.Key),
		"created_at":     createdAt,
		"schema_version": request.Batch.SchemaVersion,
		"content_hash":   store.TraceContentHash(request.Batch.Trace.ID),
	}})
}

func createArchive(ctx context.Context, tx *surrealdb.Transaction, request store.CommitIngestRequest, createdAt time.Time) error {
	return execute(ctx, tx, "create archive reference", "CREATE ONLY archive_object CONTENT $record", map[string]any{"record": map[string]any{
		"tenant_id":      string(request.TenantID),
		"archive_key":    string(request.Archive.Key),
		"byte_size":      request.Archive.Size,
		"created_at":     createdAt,
		"schema_version": request.Batch.SchemaVersion,
		"content_hash":   request.Archive.Hash,
	}})
}

func createEvent(ctx context.Context, tx *surrealdb.Transaction, request store.CommitIngestRequest, event domain.CanonicalEvent, createdAt time.Time) error {
	payload := map[string]any{
		"client_event_id": event.ClientEventID,
		"occurred_at":     event.OccurredAt,
		"tool_version":    event.ToolVersion,
		"fields":          event.Fields,
		"result":          event.Result,
	}
	record := map[string]any{
		"tenant_id":      string(request.TenantID),
		"trace_id":       string(request.Batch.Trace.ID),
		"event_id":       string(event.ID),
		"sequence":       event.Position,
		"kind":           event.Kind,
		"payload":        payload,
		"created_at":     createdAt,
		"schema_version": request.Batch.SchemaVersion,
		"content_hash":   store.EventContentHash(event.ID),
	}
	if event.ToolName != "" {
		record["tool_name"] = event.ToolName
	}
	if event.Result != nil && event.Result.ExitCode != nil {
		record["exit_code"] = int(*event.Result.ExitCode)
	}
	return execute(ctx, tx, "create canonical event", "CREATE ONLY canonical_event CONTENT $record", map[string]any{"record": record})
}

func createOutbox(ctx context.Context, tx *surrealdb.Transaction, request store.CommitIngestRequest, receipt store.IngestReceipt, createdAt time.Time) error {
	return execute(ctx, tx, "create outbox job", "CREATE ONLY outbox_job CONTENT $record", map[string]any{"record": map[string]any{
		"tenant_id":        string(request.TenantID),
		"workflow_id":      receipt.WorkflowID,
		"trace_id":         string(request.Batch.Trace.ID),
		"job_type":         "synthesize_trace",
		"state":            "pending",
		"attempt_count":    0,
		"lease_generation": 0,
		"available_at":     createdAt,
		"created_at":       createdAt,
		"schema_version":   request.Batch.SchemaVersion,
		"content_hash":     request.Batch.Hash,
	}})
}

func execute(ctx context.Context, tx *surrealdb.Transaction, operation, statement string, variables map[string]any) error {
	if _, err := surrealdb.Query[any](ctx, tx, statement, variables); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}

func waitForRetry(ctx context.Context, attempt int) error {
	base := min(10*time.Millisecond<<attempt, 250*time.Millisecond)
	jitter := time.Duration(rand.Int63n(max(1, int64(base/2))))
	timer := time.NewTimer(base + jitter)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *IngestRepository) Counts(ctx context.Context) (store.AggregateCounts, error) {
	if r.db == nil {
		return store.AggregateCounts{}, errors.New("SurrealDB ingest repository is not configured")
	}
	tables := []string{"ingest_receipt", "trace_run", "canonical_event", "archive_object", "outbox_job"}
	counts := make([]int, len(tables))
	for index, table := range tables {
		value, err := tableCount(ctx, r.db, table)
		if err != nil {
			return store.AggregateCounts{}, err
		}
		counts[index] = value
	}
	return store.AggregateCounts{Receipts: counts[0], Traces: counts[1], Events: counts[2], Archives: counts[3], OutboxJobs: counts[4]}, nil
}

func tableCount(ctx context.Context, db *surrealdb.DB, table string) (int, error) {
	type countRow struct {
		Count int `json:"count"`
	}
	statement := "SELECT count() AS count FROM " + table + " GROUP ALL"
	results, err := surrealdb.Query[[]countRow](ctx, db, statement, nil)
	if err != nil {
		return 0, fmt.Errorf("count %s: %w", table, err)
	}
	if results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return 0, nil
	}
	return (*results)[0].Result[0].Count, nil
}
