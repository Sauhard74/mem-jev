package surreal

import "testing"

func TestAuthenticationDataUsesConfiguredScope(t *testing.T) {
	tests := []struct {
		scope         AuthScope
		wantNamespace string
		wantDatabase  string
	}{
		{scope: AuthScopeRoot},
		{scope: AuthScopeNamespace, wantNamespace: "tenant_ns"},
		{scope: AuthScopeDatabase, wantNamespace: "tenant_ns", wantDatabase: "memjev"},
	}
	for _, tt := range tests {
		t.Run(string(tt.scope), func(t *testing.T) {
			auth, err := authenticationData(Config{
				Namespace: "tenant_ns", Database: "memjev", Username: "app", Password: "secret", AuthScope: tt.scope,
			})
			if err != nil {
				t.Fatal(err)
			}
			if auth.Namespace != tt.wantNamespace || auth.Database != tt.wantDatabase || auth.Username != "app" || auth.Password != "secret" {
				t.Fatalf("auth = %#v", auth)
			}
		})
	}
}

func TestAuthenticationDataRejectsUnknownScope(t *testing.T) {
	if _, err := authenticationData(Config{Namespace: "ns", Database: "db", Username: "app", Password: "secret", AuthScope: "unknown"}); err == nil {
		t.Fatal("authenticationData accepted unknown scope")
	}
}
