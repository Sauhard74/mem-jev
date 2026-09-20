package rebuild_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/rebuild"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLoaderRoundTripsVerifiedCanonicalBatch(t *testing.T) {
	batch := canonicalBatch(t)
	store := archive.NewMemoryStore()
	object, err := store.PutCanonical(context.Background(), archive.PutRequest{
		TenantID: batch.TenantID, SchemaVersion: batch.SchemaVersion, Hash: batch.Hash, Body: batch.CanonicalJSON,
	})
	if err != nil {
		t.Fatal(err)
	}
	loader := rebuild.NewLoader(store, rebuild.Config{MaximumBytes: 2 << 20, AcceptedSchemas: []string{"canonical.v1"}})
	got, err := loader.Load(context.Background(), rebuild.Request{
		TenantID: batch.TenantID, TraceID: batch.Trace.ID, SchemaVersion: batch.SchemaVersion,
		ContentHash: batch.Hash, ArchiveKey: object.Key,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Hash != batch.Hash || got.Trace.ID != batch.Trace.ID || len(got.Events) != len(batch.Events) || string(got.CanonicalJSON) != string(batch.CanonicalJSON) {
		t.Fatalf("Load() = %#v", got)
	}
}

func TestLoaderFailsClosedOnArchiveCorruption(t *testing.T) {
	batch := canonicalBatch(t)
	tests := []struct {
		name   string
		mutate func(*domain.CanonicalBatch)
		want   error
	}{
		{name: "forged trace id", mutate: func(value *domain.CanonicalBatch) { value.Trace.ID = domain.TraceID("tr_" + strings.Repeat("f", 64)) }, want: rebuild.ErrIdentityMismatch},
		{name: "reordered events", mutate: func(value *domain.CanonicalBatch) {
			value.Events[0], value.Events[1] = value.Events[1], value.Events[0]
		}, want: rebuild.ErrIdentityMismatch},
		{name: "wrong tenant", mutate: func(value *domain.CanonicalBatch) { value.TenantID = "tenant_b" }, want: rebuild.ErrIdentityMismatch},
		{name: "unknown schema", mutate: func(value *domain.CanonicalBatch) { value.SchemaVersion = "canonical.v999" }, want: rebuild.ErrUnsupportedSchema},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			forged := batch
			forged.Events = append([]domain.CanonicalEvent(nil), batch.Events...)
			tt.mutate(&forged)
			body, hash, err := canonical.MarshalAndHash(forged)
			if err != nil {
				t.Fatal(err)
			}
			key, err := archive.KeyFor(batch.TenantID, batch.SchemaVersion, hash)
			if err != nil {
				t.Fatal(err)
			}
			loader := rebuild.NewLoader(staticReader{body: body}, rebuild.Config{MaximumBytes: 2 << 20, AcceptedSchemas: []string{"canonical.v1"}})
			_, err = loader.Load(context.Background(), rebuild.Request{
				TenantID: batch.TenantID, TraceID: batch.Trace.ID, SchemaVersion: batch.SchemaVersion,
				ContentHash: hash, ArchiveKey: key,
			})
			if !errors.Is(err, tt.want) {
				t.Fatalf("Load() error = %v; want %v", err, tt.want)
			}
		})
	}
}

func TestLoaderRejectsOversizedAndCanceledReads(t *testing.T) {
	batch := canonicalBatch(t)
	key, err := archive.KeyFor(batch.TenantID, batch.SchemaVersion, batch.Hash)
	if err != nil {
		t.Fatal(err)
	}
	loader := rebuild.NewLoader(staticReader{body: batch.CanonicalJSON}, rebuild.Config{MaximumBytes: 8, AcceptedSchemas: []string{"canonical.v1"}})
	_, err = loader.Load(context.Background(), rebuild.Request{TenantID: batch.TenantID, TraceID: batch.Trace.ID, SchemaVersion: batch.SchemaVersion, ContentHash: batch.Hash, ArchiveKey: key})
	if !errors.Is(err, archive.ErrTooLarge) {
		t.Fatalf("Load() error = %v; want too large", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = rebuild.NewLoader(staticReader{body: batch.CanonicalJSON}, rebuild.Config{MaximumBytes: 2 << 20, AcceptedSchemas: []string{"canonical.v1"}}).Load(ctx,
		rebuild.Request{TenantID: batch.TenantID, TraceID: batch.Trace.ID, SchemaVersion: batch.SchemaVersion, ContentHash: batch.Hash, ArchiveKey: key})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Load() error = %v; want canceled", err)
	}
}

type staticReader struct{ body []byte }

func (r staticReader) GetBounded(ctx context.Context, _ archive.Key, maximumBytes int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if int64(len(r.body)) > maximumBytes {
		return nil, archive.ErrTooLarge
	}
	return append([]byte(nil), r.body...), nil
}

func canonicalBatch(t *testing.T) domain.CanonicalBatch {
	t.Helper()
	batch, err := canonical.Build("tenant_a", &memjevv1.IngestTraceRequest{
		ClientTraceId: "trace-1", Harness: "test", Task: "verify",
		Events: []*memjevv1.TraceEvent{
			{ClientEventId: "one", OccurredAt: timestamppb.New(time.Unix(1, 0).UTC()), Kind: memjevv1.EventKind_EVENT_KIND_EXECUTE, ToolName: "shell", Result: &memjevv1.ToolResult{State: memjevv1.ToolResultState_TOOL_RESULT_STATE_SUCCESS}},
			{ClientEventId: "two", OccurredAt: timestamppb.New(time.Unix(2, 0).UTC()), Kind: memjevv1.EventKind_EVENT_KIND_VERIFY, ToolName: "test", Result: &memjevv1.ToolResult{State: memjevv1.ToolResultState_TOOL_RESULT_STATE_SUCCESS}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return batch
}
