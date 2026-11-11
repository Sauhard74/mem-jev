package security

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestReloadingCredentialResolverPicksUpNewPeople(t *testing.T) {
	hash := func(token string) string {
		sum := sha256.Sum256([]byte(token))
		return hex.EncodeToString(sum[:])
	}
	entry := func(token, subject string) string {
		return fmt.Sprintf(`{"credential_sha256":%q,"tenant_id":"tenant_a","subject_id":%q,"region":"local","scopes":["retrievals:read"],"consent":"learn_and_recall"}`, hash(token), subject)
	}
	path := filepath.Join(t.TempDir(), "credentials.json")
	write := func(content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"credentials":[` + entry("alice-token", "alice") + `]}`)
	resolver, err := NewReloadingCredentialResolver(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveCredentialHash(hash("bob-token")); !errors.Is(err, ErrUnknownCredential) {
		t.Fatalf("bob before reload: %v", err)
	}
	if changed, err := resolver.Reload(); changed || err != nil {
		t.Fatalf("unchanged file reload = %v, %v", changed, err)
	}
	write(`{"credentials":[` + entry("alice-token", "alice") + `,` + entry("bob-token", "bob") + `]}`)
	if changed, err := resolver.Reload(); !changed || err != nil {
		t.Fatalf("changed file reload = %v, %v", changed, err)
	}
	if principal, err := resolver.ResolveCredentialHash(hash("bob-token")); err != nil || principal.SubjectID != "bob" {
		t.Fatalf("bob after reload = %#v, %v", principal, err)
	}
	// A broken or half-written file keeps the loaded credentials.
	write(`{"credentials":[`)
	if _, err := resolver.Reload(); err == nil {
		t.Fatal("broken file was accepted")
	}
	if _, err := resolver.ResolveCredentialHash(hash("alice-token")); err != nil {
		t.Fatalf("alice after a broken reload: %v", err)
	}
}
