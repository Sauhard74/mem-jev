package security

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
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
		"control character in subject": entry(strings.Repeat("a", 64), "acme", `al\u0007ice`),
		"subject too long":             entry(strings.Repeat("a", 64), "acme", strings.Repeat("x", MaximumSubjectLength+1)),
		"separator in tenant":          entry(strings.Repeat("a", 64), "ac__me", "alice"),
		"tenant collides with subject": entry(strings.Repeat("a", 64), "acme", "alice") + `,` + entry(strings.Repeat("b", 64), "acme__alice", ""),
		"tenant collides with asserted subject": `{"credential_sha256":"` + strings.Repeat("a", 64) + `","tenant_id":"acme","region":"local","scopes":["retrievals:read"],"consent":"learn_and_recall","may_assert_subject":true},` +
			entry(strings.Repeat("b", 64), "acme__bob", ""),
		"subject and may_assert_subject": `{"credential_sha256":"` + strings.Repeat("a", 64) + `","tenant_id":"acme","subject_id":"alice","region":"local","scopes":["retrievals:read"],"consent":"learn_and_recall","may_assert_subject":true}`,
	}
	for name, entries := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NewFileCredentialResolver(writeCredentials(t, `{"credentials":[`+entries+`]}`)); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestScopedTenantIDHashesLongOrUnderscoredSubjects(t *testing.T) {
	plain, err := ScopedTenantID("acme", "alice")
	if err != nil || plain != "acme__alice" {
		t.Fatalf("plain scope = %q, %v", plain, err)
	}
	long := "milkyway_" + strings.Repeat("p", 100)
	first, err := ScopedTenantID("acme", long)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := ScopedTenantID("acme", long)
	other, _ := ScopedTenantID("acme", long+"x")
	if first != again || first == other || !strings.HasPrefix(string(first), "acme__h_") || len(first) != len("acme__h_")+40 {
		t.Fatalf("hashed scopes = %q %q %q", first, again, other)
	}
	if OrganizationOf(first) != "acme" || OrganizationOf("acme") != "acme" {
		t.Fatalf("organization = %q", OrganizationOf(first))
	}
	if _, err := ScopedTenantID("acme", " alice"); err == nil {
		t.Fatal("padded subject accepted")
	}
}

func TestBearerAuthenticatorAppliesSubjectHeaderOnlyForTrustedCredentials(t *testing.T) {
	trustedToken, plainToken, personToken := "trusted-token-0123456789", "plain-token-0123456789", "person-token-0123456789"
	hash := func(token string) string {
		sum := sha256.Sum256([]byte(token))
		return hex.EncodeToString(sum[:])
	}
	path := writeCredentials(t, `{"credentials":[`+
		`{"credential_sha256":"`+hash(trustedToken)+`","tenant_id":"acme","region":"local","scopes":["retrievals:read"],"consent":"learn_and_recall","may_assert_subject":true},`+
		`{"credential_sha256":"`+hash(plainToken)+`","tenant_id":"acme","region":"local","scopes":["retrievals:read"],"consent":"learn_and_recall"},`+
		`{"credential_sha256":"`+hash(personToken)+`","tenant_id":"acme","subject_id":"alice","region":"local","scopes":["retrievals:read"],"consent":"learn_and_recall"}]}`)
	resolver, err := NewFileCredentialResolver(path)
	if err != nil {
		t.Fatal(err)
	}
	authenticator := NewBearerAuthenticator(resolver)
	request := func(token string, subjects ...string) (Principal, error) {
		header := http.Header{}
		header.Set("Authorization", "Bearer "+token)
		for _, subject := range subjects {
			header.Add(SubjectHeader, subject)
		}
		return authenticator.Authenticate(header)
	}
	if principal, err := request(trustedToken); err != nil || principal.TenantID != "acme" {
		t.Fatalf("no header = %#v, %v", principal, err)
	}
	if principal, err := request(trustedToken, "bob"); err != nil || principal.TenantID != "acme__bob" || principal.SubjectID != "bob" || principal.OrganizationID != "acme" {
		t.Fatalf("asserted subject = %#v, %v", principal, err)
	}
	if principal, err := request(trustedToken, "alice"); err != nil || principal.TenantID != "acme__alice" {
		t.Fatalf("asserted alice should share the alice token scope: %#v, %v", principal, err)
	}
	if _, err := request(plainToken, "bob"); !errors.Is(err, ErrSubjectNotAllowed) {
		t.Fatalf("untrusted credential error = %v", err)
	}
	if _, err := request(personToken, "bob"); !errors.Is(err, ErrSubjectNotAllowed) {
		t.Fatalf("person credential error = %v", err)
	}
	if _, err := request(trustedToken, "bob", "carol"); !errors.Is(err, ErrInvalidSubject) {
		t.Fatalf("repeated header error = %v", err)
	}
	if _, err := request(trustedToken, ""); !errors.Is(err, ErrInvalidSubject) {
		t.Fatalf("empty header error = %v", err)
	}
}
