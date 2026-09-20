package buildinfo

import "testing"

func TestCurrentHasNonEmptyVersionFields(t *testing.T) {
	got := Current()
	if got.Version == "" || got.Commit == "" || got.BuiltAt == "" {
		t.Fatalf("Current() = %#v; all fields must be non-empty", got)
	}
}
