package retrieval

import (
	"strings"
	"testing"
)

func TestSemanticStateRejectsCredentialMaterial(t *testing.T) {
	fixture := newServiceFixture(t, false)
	query, err := BuildQuery(fixture.request.TenantID, fixture.request.CurrentPolicyVersion, AliasSet{}, fixture.request.Input)
	if err != nil {
		t.Fatal(err)
	}
	query.Task = "use Authorization: Bearer secret-token-value"
	if _, _, err = semanticStateFor(query, fixture.document); err == nil {
		t.Fatal("credential-bearing state was accepted")
	}
}

func TestSemanticStateExcludesResourceIdentitiesAndCanonicalizes(t *testing.T) {
	fixture := newServiceFixture(t, false)
	fixture.request.Input.Resources = []Resource{{Type: "repository", Namespace: "source", Identity: "private/customer/repo"}}
	query, err := BuildQuery(fixture.request.TenantID, fixture.request.CurrentPolicyVersion, AliasSet{}, fixture.request.Input)
	if err != nil {
		t.Fatal(err)
	}
	_, encoded, err := semanticStateFor(query, fixture.document)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "private/customer/repo") || !strings.Contains(string(encoded), `"task":"release"`) {
		t.Fatalf("unsafe or incomplete semantic state: %s", encoded)
	}
}
