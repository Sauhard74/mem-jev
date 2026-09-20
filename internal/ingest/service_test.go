package ingest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/policy"
	"github.com/sauhard74/mem-jev/internal/security"
	"github.com/sauhard74/mem-jev/internal/store"
	storememory "github.com/sauhard74/mem-jev/internal/store/memory"
)

func TestIngestArchiveSuccessDatabaseFailureConvergesOnRetry(t *testing.T) {
	archives := archive.NewMemoryStore()
	repository := newFailOnceRepository()
	service := NewService(archives, repository, DefaultPolicy())
	command := validCommand()

	if _, err := service.Ingest(context.Background(), command); err == nil {
		t.Fatal("first call must fail")
	}
	got, err := service.Ingest(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if archives.PutCount() != 1 {
		t.Fatalf("archive writes = %d, want 1", archives.PutCount())
	}
	if got.Disposition != DispositionAccepted {
		t.Fatalf("disposition = %v", got.Disposition)
	}
}

func TestIngestOrdersArchiveBeforeRepository(t *testing.T) {
	sequence := make([]string, 0, 2)
	archives := &recordingArchive{delegate: archive.NewMemoryStore(), sequence: &sequence}
	repository := &recordingRepository{delegate: storememory.NewIngestRepository(), sequence: &sequence}
	service := NewService(archives, repository, DefaultPolicy())
	if _, err := service.Ingest(context.Background(), validCommand()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(sequence, ",") != "archive,repository" {
		t.Fatalf("sequence = %v", sequence)
	}
}

func TestIngestRejectsBeforeExternalIO(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Command)
		want   error
	}{
		{name: "consent", mutate: func(command *Command) { command.Principal.Consent = policy.RecallOnly }, want: policy.ErrConsentDenied},
		{name: "scope", mutate: func(command *Command) { command.Principal.Scopes = nil }, want: ErrPermissionDenied},
		{name: "missing tenant", mutate: func(command *Command) { command.Principal.TenantID = "" }, want: ErrInvalidCommand},
		{name: "invalid request", mutate: func(command *Command) { command.Request.Events = nil }, want: ErrInvalidTrace},
		{name: "invalid idempotency hash", mutate: func(command *Command) { command.IdempotencyKeyHash = "raw-key" }, want: ErrInvalidCommand},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			archives := &recordingArchive{delegate: archive.NewMemoryStore()}
			repository := &recordingRepository{delegate: storememory.NewIngestRepository()}
			service := NewService(archives, repository, DefaultPolicy())
			command := validCommand()
			tt.mutate(&command)
			_, err := service.Ingest(context.Background(), command)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if archives.calls != 0 || repository.calls != 0 {
				t.Fatalf("external calls: archive=%d repository=%d", archives.calls, repository.calls)
			}
		})
	}
}

func TestIngestSanitizesWithoutMutatingInput(t *testing.T) {
	const secret = "sk-secret-value"
	archives := archive.NewMemoryStore()
	service := NewService(archives, storememory.NewIngestRepository(), DefaultPolicy())
	command := validCommand()
	command.Request.Task = "use " + secret
	command.Request.Events[0].Fields[0].StringValue = "Authorization: Bearer " + secret

	result, err := service.Ingest(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(command.Request.Task, secret) {
		t.Fatal("service mutated caller request")
	}
	body, err := archives.Get(context.Background(), result.ArchiveKey)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), secret) {
		t.Fatal("canonical archive contains secret")
	}
	if result.Redactions < 2 {
		t.Fatalf("redactions = %d, want at least 2", result.Redactions)
	}
}

func TestIngestDuplicateAndConflict(t *testing.T) {
	service := NewService(archive.NewMemoryStore(), storememory.NewIngestRepository(), DefaultPolicy())
	command := validCommand()
	first, err := service.Ingest(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Ingest(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if second.ReceiptID != first.ReceiptID || second.Disposition != DispositionDuplicate {
		t.Fatalf("first=%#v second=%#v", first, second)
	}

	command.Request.Task = "different safe task"
	_, err = service.Ingest(context.Background(), command)
	if !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("error = %v, want %v", err, store.ErrIdempotencyConflict)
	}
}

func TestIngestCancellationBoundaries(t *testing.T) {
	t.Run("before archive", func(t *testing.T) {
		archives := archive.NewMemoryStore()
		repository := &recordingRepository{delegate: storememory.NewIngestRepository()}
		service := NewService(archives, repository, DefaultPolicy())
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := service.Ingest(ctx, validCommand())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want canceled", err)
		}
		if archives.PutCount() != 0 || repository.calls != 0 {
			t.Fatalf("archive=%d repository=%d", archives.PutCount(), repository.calls)
		}
	})

	t.Run("after archive", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		archives := &cancelingArchive{delegate: archive.NewMemoryStore(), cancel: cancel}
		repository := &recordingRepository{delegate: storememory.NewIngestRepository()}
		service := NewService(archives, repository, DefaultPolicy())
		_, err := service.Ingest(ctx, validCommand())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want canceled", err)
		}
		if archives.delegate.PutCount() != 1 || repository.calls != 0 {
			t.Fatalf("archive=%d repository=%d", archives.delegate.PutCount(), repository.calls)
		}
	})
}

func TestIngestSafeErrorsDoNotEchoRequestValues(t *testing.T) {
	const sensitive = "sensitive-task-value"
	archiveErr := errors.New("storage unavailable")
	repository := &recordingRepository{delegate: storememory.NewIngestRepository()}
	service := NewService(&failingArchive{err: archiveErr}, repository, DefaultPolicy())
	command := validCommand()
	command.Request.Task = sensitive
	_, err := service.Ingest(context.Background(), command)
	if !errors.Is(err, archiveErr) || strings.Contains(err.Error(), sensitive) {
		t.Fatalf("unsafe error: %v", err)
	}
	if repository.calls != 0 {
		t.Fatalf("repository called %d times after archive failure", repository.calls)
	}
}

func validCommand() Command {
	request := traceWithField("command", "go test ./...")
	return Command{
		Principal: security.Principal{
			TenantID: domain.TenantID("tenant_a"),
			Region:   "local",
			Scopes:   map[string]struct{}{security.ScopeIngestWrite: {}},
			Consent:  policy.LearnAndRecall,
		},
		IdempotencyKeyHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Request:            request,
	}
}

type failOnceRepository struct {
	mu       sync.Mutex
	failed   bool
	delegate store.IngestRepository
}

func newFailOnceRepository() *failOnceRepository {
	return &failOnceRepository{delegate: storememory.NewIngestRepository()}
}

func (r *failOnceRepository) Commit(ctx context.Context, request store.CommitIngestRequest) (store.IngestReceipt, error) {
	r.mu.Lock()
	if !r.failed {
		r.failed = true
		r.mu.Unlock()
		return store.IngestReceipt{}, errors.New("repository temporarily unavailable")
	}
	r.mu.Unlock()
	return r.delegate.Commit(ctx, request)
}

type recordingArchive struct {
	delegate archive.Store
	sequence *[]string
	calls    int
}

func (s *recordingArchive) PutCanonical(ctx context.Context, request archive.PutRequest) (archive.Object, error) {
	s.calls++
	if s.sequence != nil {
		*s.sequence = append(*s.sequence, "archive")
	}
	return s.delegate.PutCanonical(ctx, request)
}

func (s *recordingArchive) Get(ctx context.Context, key archive.Key) ([]byte, error) {
	return s.delegate.Get(ctx, key)
}

type recordingRepository struct {
	delegate store.IngestRepository
	sequence *[]string
	calls    int
}

func (r *recordingRepository) Commit(ctx context.Context, request store.CommitIngestRequest) (store.IngestReceipt, error) {
	r.calls++
	if r.sequence != nil {
		*r.sequence = append(*r.sequence, "repository")
	}
	return r.delegate.Commit(ctx, request)
}

type cancelingArchive struct {
	delegate *archive.MemoryStore
	cancel   context.CancelFunc
}

func (s *cancelingArchive) PutCanonical(ctx context.Context, request archive.PutRequest) (archive.Object, error) {
	object, err := s.delegate.PutCanonical(ctx, request)
	s.cancel()
	return object, err
}

func (s *cancelingArchive) Get(ctx context.Context, key archive.Key) ([]byte, error) {
	return s.delegate.Get(ctx, key)
}

type failingArchive struct{ err error }

func (s *failingArchive) PutCanonical(context.Context, archive.PutRequest) (archive.Object, error) {
	return archive.Object{}, s.err
}

func (s *failingArchive) Get(context.Context, archive.Key) ([]byte, error) {
	return nil, s.err
}
