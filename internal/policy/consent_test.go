package policy

import (
	"errors"
	"testing"
)

func TestAuthorizeIngestFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		mode ConsentMode
		want error
	}{
		{name: "unspecified", mode: "", want: ErrConsentDenied},
		{name: "deny", mode: Deny, want: ErrConsentDenied},
		{name: "recall only", mode: RecallOnly, want: ErrConsentDenied},
		{name: "learn and recall", mode: LearnAndRecall, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := AuthorizeIngest(test.mode)
			if !errors.Is(err, test.want) {
				t.Fatalf("AuthorizeIngest(%q) = %v; want %v", test.mode, err, test.want)
			}
		})
	}
}
