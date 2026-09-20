package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sauhard74/mem-jev/internal/erasure"
)

func TestJournalErasureIntentIsDurableAndIdempotent(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MEMJEV_ERASURE_LEDGER_DIR", directory)
	request := erasure.Request{TenantID: "tenant_a", RequestID: "erase_01JABCDE1234567890", Confirmation: "erase:tenant_a:erase_01JABCDE1234567890"}
	if err := journalErasureIntent(request); err != nil {
		t.Fatal(err)
	}
	if err := journalErasureIntent(request); err != nil {
		t.Fatalf("idempotent journal: %v", err)
	}
	path := filepath.Join(directory, request.RequestID+".json")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("journal info=%v err=%v", info, err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var stored erasure.Request
	if json.Unmarshal(encoded, &stored) != nil || stored != request {
		t.Fatalf("stored request = %#v", stored)
	}
	conflict := request
	conflict.TenantID = "tenant_b"
	conflict.Confirmation = "erase:tenant_b:" + conflict.RequestID
	if err = journalErasureIntent(conflict); !errors.Is(err, erasure.ErrConflict) {
		t.Fatalf("conflicting journal error = %v", err)
	}
}

func TestJournalErasureIntentRejectsLooseDirectory(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MEMJEV_ERASURE_LEDGER_DIR", directory)
	request := erasure.Request{TenantID: "tenant_a", RequestID: "erase_01JABCDE1234567890", Confirmation: "erase:tenant_a:erase_01JABCDE1234567890"}
	if err := journalErasureIntent(request); err == nil {
		t.Fatal("loose ledger directory accepted")
	}
}
