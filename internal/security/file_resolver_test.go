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
