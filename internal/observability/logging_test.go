package observability

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRequestLogContainsOnlyAllowListedFacts(t *testing.T) {
	buffer := new(bytes.Buffer)
	logger := NewJSONLogger(buffer)
	LogRequest(context.Background(), logger, RequestFacts{
		RequestID: "req_1", TenantHash: "abc", Bytes: 42, Events: 2, ResultCode: "accepted",
	})
	logged := buffer.String()
	for _, forbidden := range []string{"task", "command", "prompt", "authorization"} {
		if strings.Contains(strings.ToLower(logged), forbidden) {
			t.Fatalf("unsafe log contains %q: %s", forbidden, logged)
		}
	}
	for _, required := range []string{"req_1", "accepted", "42"} {
		if !strings.Contains(logged, required) {
			t.Fatalf("log missing %q: %s", required, logged)
		}
	}
}
