package jev

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileSecretLoadsAndRotatesWithoutExposingValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jev.key")
	if err := os.WriteFile(path, []byte("first-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	secret, err := NewFileSecret(path, 128)
	if err != nil {
		t.Fatal(err)
	}
	first, err := secret.Token()
	if err != nil || first != "first-secret" {
		t.Fatalf("Token() = %q, %v", first, err)
	}
	if err = os.WriteFile(path, []byte("rotated-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := secret.Token()
	if err != nil || second != "rotated-secret" {
		t.Fatalf("rotated Token() = %q, %v", second, err)
	}
}

func TestFileSecretRejectsUnsafeFilesAndRedactsErrors(t *testing.T) {
	directory := t.TempDir()
	unsafe := filepath.Join(directory, "unsafe.key")
	value := "never-print-this-secret"
	if err := os.WriteFile(unsafe, []byte(value), 0o644); err != nil {
		t.Fatal(err)
	}
	secret, err := NewFileSecret(unsafe, 128)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = secret.Token(); !errors.Is(err, ErrSecretUnavailable) || strings.Contains(err.Error(), value) {
		t.Fatalf("unsafe secret error = %v", err)
	}

	target := filepath.Join(directory, "target.key")
	if err = os.WriteFile(target, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "link.key")
	if err = os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	linked, err := NewFileSecret(link, 128)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = linked.Token(); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("symlink error = %v", err)
	}
}
