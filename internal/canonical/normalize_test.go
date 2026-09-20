package canonical

import (
	"bytes"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/domain"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type field struct {
	Name  string
	Value string
}

func TestBuildNormalizesOrderUnicodeAndPaths(t *testing.T) {
	a := requestWithFields([]field{{"z", "e\u0301"}, {"path", "./src/../src/main.go"}})
	b := requestWithFields([]field{{"path", "src/main.go"}, {"z", "é"}})

	gotA, err := Build(domain.TenantID("tenant_a"), a)
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := Build(domain.TenantID("tenant_a"), b)
	if err != nil {
		t.Fatal(err)
	}
	if gotA.Hash != gotB.Hash {
		t.Fatalf("hashes differ: %s != %s", gotA.Hash, gotB.Hash)
	}
	if !bytes.Equal(gotA.CanonicalJSON, gotB.CanonicalJSON) {
		t.Fatalf("canonical JSON differs:\n%s\n%s", gotA.CanonicalJSON, gotB.CanonicalJSON)
	}
}

func TestBuildIncludesTenantInIdentity(t *testing.T) {
	req := requestWithFields(nil)
	a, err := Build(domain.TenantID("tenant_a"), req)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Build(domain.TenantID("tenant_b"), req)
	if err != nil {
		t.Fatal(err)
	}
	if a.Hash == b.Hash {
		t.Fatal("cross-tenant canonical hashes must differ")
	}
	if a.Trace.ID == b.Trace.ID {
		t.Fatal("cross-tenant trace IDs must differ")
	}
}

func TestBuildRejectsDuplicateFieldNames(t *testing.T) {
	req := requestWithFields([]field{{"path", "a"}, {" path ", "b"}})
	_, err := Build(domain.TenantID("tenant_a"), req)
	if err == nil || !strings.Contains(err.Error(), "events[0].fields.path") {
		t.Fatalf("Build() error = %v; want duplicate canonical field", err)
	}
}

func TestBuildRejectsDuplicateNormalizedEventIdentifiers(t *testing.T) {
	req := requestWithFields(nil)
	second := proto.Clone(req.Events[0]).(*memjevv1.TraceEvent)
	second.ClientEventId = " event-1 "
	req.Events = append(req.Events, second)
	_, err := Build(domain.TenantID("tenant_a"), req)
	if err == nil || !strings.Contains(err.Error(), "client_event_id") {
		t.Fatalf("Build() error = %v; want normalized duplicate rejection", err)
	}
}

func TestBuildRejectsWhitespaceOnlyRequiredIdentifiers(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*memjevv1.IngestTraceRequest)
	}{
		{name: "client trace", mutate: func(request *memjevv1.IngestTraceRequest) { request.ClientTraceId = " \t " }},
		{name: "harness", mutate: func(request *memjevv1.IngestTraceRequest) { request.Harness = " \t " }},
		{name: "client event", mutate: func(request *memjevv1.IngestTraceRequest) { request.Events[0].ClientEventId = " \t " }},
		{name: "tool name", mutate: func(request *memjevv1.IngestTraceRequest) { request.Events[0].ToolName = " \t " }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := requestWithFields(nil)
			tt.mutate(req)
			if _, err := Build(domain.TenantID("tenant_a"), req); err == nil {
				t.Fatal("Build() accepted whitespace-only required identity")
			}
		})
	}
}

func TestBuildMatchesGoldenAndIgnoresFieldOrder(t *testing.T) {
	want, err := os.ReadFile("testdata/canonical_trace.golden.json")
	if err != nil {
		t.Fatal(err)
	}

	seed := rand.New(rand.NewSource(42))
	baseFields := []field{{"path", "./src/../src/main.go"}, {"query_shape", "status = ?"}, {"z", "e\u0301"}}
	baseEnvironment := []*memjevv1.Field{{Name: "region", StringValue: "us-east-1"}, {Name: "repository", StringValue: "demo"}}
	for iteration := 0; iteration < 500; iteration++ {
		fields := append([]field(nil), baseFields...)
		environment := append([]*memjevv1.Field(nil), baseEnvironment...)
		seed.Shuffle(len(fields), func(i, j int) { fields[i], fields[j] = fields[j], fields[i] })
		seed.Shuffle(len(environment), func(i, j int) { environment[i], environment[j] = environment[j], environment[i] })
		req := requestWithFields(fields)
		req.Environment = environment

		got, buildErr := Build(domain.TenantID("tenant_a"), req)
		if buildErr != nil {
			t.Fatalf("iteration %d: %v", iteration, buildErr)
		}
		if !bytes.Equal(got.CanonicalJSON, bytes.TrimSpace(want)) {
			t.Fatalf("iteration %d canonical JSON:\n%s\nwant:\n%s", iteration, got.CanonicalJSON, want)
		}
	}
}

func requestWithFields(fields []field) *memjevv1.IngestTraceRequest {
	protoFields := make([]*memjevv1.Field, len(fields))
	for index, item := range fields {
		protoFields[index] = &memjevv1.Field{Name: item.Name, StringValue: item.Value}
	}
	return &memjevv1.IngestTraceRequest{
		ClientTraceId:  "trace-1",
		Harness:        "test-harness",
		HarnessVersion: "1.0.0",
		Task:           "Run e\u0301 checks",
		Events: []*memjevv1.TraceEvent{{
			ClientEventId: "event-1",
			OccurredAt:    timestamppb.New(time.Date(2026, time.September, 20, 8, 30, 0, 123456789, time.FixedZone("GST", 4*60*60))),
			Kind:          memjevv1.EventKind_EVENT_KIND_EXECUTE,
			ToolName:      "shell",
			ToolVersion:   "1.0.0",
			Fields:        protoFields,
			Result: &memjevv1.ToolResult{
				State:    memjevv1.ToolResultState_TOOL_RESULT_STATE_SUCCESS,
				ExitCode: int32Pointer(0),
				Evidence: []*memjevv1.Field{{Name: "assertion", StringValue: "passed"}},
			},
		}},
	}
}

func int32Pointer(value int32) *int32 {
	return &value
}
