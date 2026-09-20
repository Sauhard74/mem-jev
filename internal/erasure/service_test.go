package erasure

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestServiceErasesArchiveBeforeDatabaseAndReturnsStableReceipt(t *testing.T) {
	order := []string{}
	archives := &recordingArchive{order: &order, deleted: 3}
	repository := &recordingRepository{order: &order}
	now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	service, err := NewService(archives, repository, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := Request{TenantID: "tenant_a", RequestID: "erase_01JABCDE1234567890", Confirmation: "erase:tenant_a:erase_01JABCDE1234567890"}
	receipt, err := service.Erase(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0] != "archive" || order[1] != "database" {
		t.Fatalf("operation order = %#v", order)
	}
	if receipt.RequestID != request.RequestID || receipt.ArchiveObjectsDeleted != 3 || receipt.CompletedAt != now || receipt.TenantHash == "" || receipt.ContentHash == "" {
		t.Fatalf("receipt = %#v", receipt)
	}
	replayed, err := service.Erase(context.Background(), request)
	if err != nil || replayed.ContentHash != receipt.ContentHash {
		t.Fatalf("replay = %#v, %v", replayed, err)
	}
}

func TestServiceStopsBeforeDatabaseWhenArchiveErasureFails(t *testing.T) {
	order := []string{}
	service, err := NewService(&recordingArchive{order: &order, err: errors.New("s3 unavailable")}, &recordingRepository{order: &order}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Erase(context.Background(), Request{TenantID: "tenant_a", RequestID: "erase_01JABCDE1234567890", Confirmation: "erase:tenant_a:erase_01JABCDE1234567890"})
	if err == nil || len(order) != 1 || order[0] != "archive" {
		t.Fatalf("error=%v order=%#v", err, order)
	}
}

func TestServiceRejectsMalformedOrUnconfirmedRequest(t *testing.T) {
	service, err := NewService(&recordingArchive{}, &recordingRepository{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []Request{
		{TenantID: "../tenant", RequestID: "erase_01JABCDE1234567890", Confirmation: "erase:../tenant:erase_01JABCDE1234567890"},
		{TenantID: "tenant_a", RequestID: "bad id", Confirmation: "erase:tenant_a:bad id"},
		{TenantID: "tenant_a", RequestID: "erase_01JABCDE1234567890", Confirmation: "yes"},
	} {
		if _, err := service.Erase(context.Background(), request); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("request %#v error = %v", request, err)
		}
	}
}

type recordingArchive struct {
	order   *[]string
	deleted uint64
	err     error
}

func (a *recordingArchive) EraseTenant(context.Context, string) (uint64, error) {
	if a.order != nil {
		*a.order = append(*a.order, "archive")
	}
	deleted := a.deleted
	a.deleted = 0
	return deleted, a.err
}

type recordingRepository struct {
	order   *[]string
	receipt Receipt
}

func (r *recordingRepository) FindErasure(_ context.Context, _ string) (Receipt, bool, error) {
	return r.receipt, r.receipt.ContentHash != "", nil
}

func (r *recordingRepository) CommitErasure(_ context.Context, _ string, receipt Receipt) (Receipt, error) {
	if r.order != nil {
		*r.order = append(*r.order, "database")
	}
	if r.receipt.ContentHash != "" {
		if r.receipt.ContentHash != receipt.ContentHash {
			return Receipt{}, ErrConflict
		}
		return r.receipt, nil
	}
	r.receipt = receipt
	return receipt, nil
}
