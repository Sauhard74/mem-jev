package security

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileCredentialResolverRejectsMalformedHashAndConsent(t *testing.T) {
	tests := []struct {
		name    string
		hash    string
		consent string
	}{
		{name: "non hex hash", hash: strings.Repeat("z", 64), consent: "learn_and_recall"},
		{name: "unknown consent", hash: strings.Repeat("a", 64), consent: "always"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "credentials.json")
			document := `{"credentials":[{"credential_sha256":"` + tt.hash + `","tenant_id":"tenant","region":"local","scopes":["ingest:write"],"consent":"` + tt.consent + `"}]}`
			if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := NewFileCredentialResolver(path); err == nil || errors.Is(err, ErrUnknownCredential) {
				t.Fatalf("error = %v, want configuration rejection", err)
			}
		})
	}
}

func writeCredentials(t *testing.T, document string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFileCredentialResolverScopesSubjectsSeparately(t *testing.T) {
	alice, bob, shared := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	path := writeCredentials(t, `{"credentials":[`+
		`{"credential_sha256":"`+alice+`","tenant_id":"acme","subject_id":"alice","region":"local","scopes":["retrievals:read"],"consent":"learn_and_recall"},`+
		`{"credential_sha256":"`+bob+`","tenant_id":"acme","subject_id":"bob","region":"local","scopes":["retrievals:read"],"consent":"learn_and_recall"},`+
		`{"credential_sha256":"`+shared+`","tenant_id":"acme","region":"local","scopes":["retrievals:read"],"consent":"learn_and_recall"}]}`)
	resolver, err := NewFileCredentialResolver(path)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := resolver.ResolveCredentialHash(alice)
	b, _ := resolver.ResolveCredentialHash(bob)
	c, _ := resolver.ResolveCredentialHash(shared)
	if a.TenantID != "acme__alice" || b.TenantID != "acme__bob" || c.TenantID != "acme" {
		t.Fatalf("scopes = %q %q %q", a.TenantID, b.TenantID, c.TenantID)
	}
	if a.OrganizationID != "acme" || a.SubjectID != "alice" || c.SubjectID != "" {
		t.Fatalf("principal identity = %#v / %#v", a, c)
	}
}

func TestFileCredentialResolverRejectsInvalidOrCollidingSubjects(t *testing.T) {
	entry := func(hash, tenant, subject string) string {
		value := `{"credential_sha256":"` + hash + `","tenant_id":"` + tenant + `","region":"local","scopes":["retrievals:read"],"consent":"learn_and_recall"`
		if subject != "" {
			value += `,"subject_id":"` + subject + `"`
		}
		return value + `}`
	}
	tests := map[string]string{
		"underscore in subject":        entry(strings.Repeat("a", 64), "acme", "al_ice"),
		"separator in tenant":          entry(strings.Repeat("a", 64), "ac__me", "alice"),
		"tenant collides with subject": entry(strings.Repeat("a", 64), "acme", "alice") + `,` + entry(strings.Repeat("b", 64), "acme__alice", ""),
	}
	for name, entries := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NewFileCredentialResolver(writeCredentials(t, `{"credentials":[`+entries+`]}`)); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}
