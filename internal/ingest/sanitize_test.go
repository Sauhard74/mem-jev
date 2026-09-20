package ingest

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSanitizeRedactsCredentialInsideAllowedCommand(t *testing.T) {
	const secret = "sk-secret-value"
	req := traceWithField("command", "curl -H 'Authorization: Bearer "+secret+"' https://example.test")
	clean, report, err := Sanitize(req, DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	got := clean.Events[0].Fields[0].StringValue
	if strings.Contains(got, secret) {
		t.Fatalf("secret survived: %q", got)
	}
	if report.Redactions != 1 {
		t.Fatalf("redactions = %d", report.Redactions)
	}
	if !strings.Contains(req.Events[0].Fields[0].StringValue, secret) {
		t.Fatal("Sanitize mutated the raw request")
	}
}

func TestSanitizeRejectsUnknownField(t *testing.T) {
	req := traceWithField("raw_stdout", "sensitive")
	_, _, err := Sanitize(req, DefaultPolicy())
	if !errors.Is(err, ErrForbiddenField) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("error leaked field value: %v", err)
	}
}

func TestSanitizeRejectsSecretsInEveryPersistedIdentity(t *testing.T) {
	const secret = "sk-live-AbCdEfGhIjKlMnOpQrStUvWx"
	tests := []struct {
		name   string
		mutate func(*memjevv1.IngestTraceRequest)
	}{
		{name: "client trace ID", mutate: func(request *memjevv1.IngestTraceRequest) { request.ClientTraceId = secret }},
		{name: "harness", mutate: func(request *memjevv1.IngestTraceRequest) { request.Harness = secret }},
		{name: "harness version", mutate: func(request *memjevv1.IngestTraceRequest) { request.HarnessVersion = secret }},
		{name: "client event ID", mutate: func(request *memjevv1.IngestTraceRequest) { request.Events[0].ClientEventId = secret }},
		{name: "tool name", mutate: func(request *memjevv1.IngestTraceRequest) { request.Events[0].ToolName = secret }},
		{name: "tool version", mutate: func(request *memjevv1.IngestTraceRequest) { request.Events[0].ToolVersion = secret }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := traceWithField("command", "true")
			tt.mutate(request)
			_, _, err := Sanitize(request, DefaultPolicy())
			if !errors.Is(err, ErrInvalidTrace) {
				t.Fatalf("error = %v, want ErrInvalidTrace", err)
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("error leaked secret: %v", err)
			}
		})
	}
}

func TestSanitizeRejectsControlCharactersInPersistedIdentity(t *testing.T) {
	request := traceWithField("command", "true")
	request.Events[0].ToolName = "sh\x00ell"
	_, _, err := Sanitize(request, DefaultPolicy())
	if !errors.Is(err, ErrInvalidTrace) {
		t.Fatalf("error = %v, want ErrInvalidTrace", err)
	}
}

func TestSanitizeNormalizesControlsAndLineEndings(t *testing.T) {
	req := traceWithField("assertion", "first\r\nsecond\x00\x1f\u0085third\tfourth")
	clean, _, err := Sanitize(req, DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	want := "first\nsecondthird\tfourth"
	if got := clean.Events[0].Fields[0].StringValue; got != want {
		t.Fatalf("sanitized value = %q; want %q", got, want)
	}
}

func TestSanitizeSecretCorpusNeverLeaksSecret(t *testing.T) {
	data, err := os.ReadFile("testdata/secret-corpus.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []secretCase
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			req := traceWithField("command", test.Input)
			req.Task = "task with " + test.Secret
			clean, report, sanitizeErr := Sanitize(req, DefaultPolicy())
			if sanitizeErr != nil {
				if strings.Contains(sanitizeErr.Error(), test.Secret) {
					t.Fatalf("error leaked secret: %v", sanitizeErr)
				}
				t.Fatal(sanitizeErr)
			}
			encoded, err := protojson.Marshal(clean)
			if err != nil {
				t.Fatal(err)
			}
			reportJSON, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), test.Secret) {
				t.Fatalf("protobuf JSON leaked secret: %s", encoded)
			}
			if strings.Contains(string(reportJSON), test.Secret) {
				t.Fatalf("report leaked secret: %s", reportJSON)
			}
			if report.Redactions < 2 {
				t.Fatalf("redactions = %d; want task and field redacted", report.Redactions)
			}
		})
	}
}

func TestSanitizeRejectsOversizedValueWithoutScanningOrLeaking(t *testing.T) {
	const secret = "sk-oversized-secret"
	policy := DefaultPolicy()
	policy.MaxValueBytes = 32
	req := traceWithField("command", strings.Repeat("x", 64)+secret)
	_, report, err := Sanitize(req, policy)
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("error = %v; want ErrValueTooLarge", err)
	}
	if report.Redactions != 0 {
		t.Fatalf("redactions = %d; size rejection must happen before scanning", report.Redactions)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked secret: %v", err)
	}
}

type secretCase struct {
	Name   string `json:"name"`
	Input  string `json:"input"`
	Secret string `json:"secret"`
}

func traceWithField(name, value string) *memjevv1.IngestTraceRequest {
	return &memjevv1.IngestTraceRequest{
		ClientTraceId: "trace-1",
		Harness:       "test-harness",
		Task:          "safe task",
		Events: []*memjevv1.TraceEvent{{
			ClientEventId: "event-1",
			OccurredAt:    timestamppb.New(time.Unix(1, 0).UTC()),
			Kind:          memjevv1.EventKind_EVENT_KIND_EXECUTE,
			ToolName:      "shell",
			Fields:        []*memjevv1.Field{{Name: name, StringValue: value}},
			Result:        &memjevv1.ToolResult{State: memjevv1.ToolResultState_TOOL_RESULT_STATE_SUCCESS},
		}},
	}
}
