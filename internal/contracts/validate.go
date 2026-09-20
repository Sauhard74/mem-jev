package contracts

import (
	"fmt"
	"strings"

	"buf.build/go/protovalidate"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"golang.org/x/text/unicode/norm"
	"google.golang.org/protobuf/proto"
)

const maxIngestRequestBytes = 1 << 20

type ViolationError struct {
	Field string
	Rule  string
}

func (e *ViolationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Rule)
}

func ValidateIngest(request *memjevv1.IngestTraceRequest) error {
	if request == nil {
		return &ViolationError{Field: "request", Rule: "required"}
	}
	if proto.Size(request) > maxIngestRequestBytes {
		return &ViolationError{Field: "request_size", Rule: "must not exceed 1048576 bytes"}
	}
	if err := protovalidate.Validate(request); err != nil {
		return fmt.Errorf("validate ingest request: %w", err)
	}
	for _, required := range []struct{ field, value string }{
		{field: "client_trace_id", value: request.GetClientTraceId()},
		{field: "harness", value: request.GetHarness()},
	} {
		if canonicalToken(required.value) == "" {
			return &ViolationError{Field: required.field, Rule: "must not be empty after normalization"}
		}
	}

	seen := make(map[string]struct{}, len(request.GetEvents()))
	for index, event := range request.GetEvents() {
		id := canonicalToken(event.GetClientEventId())
		if id == "" {
			return &ViolationError{Field: fmt.Sprintf("events[%d].client_event_id", index), Rule: "must not be empty after normalization"}
		}
		if canonicalToken(event.GetToolName()) == "" {
			return &ViolationError{Field: fmt.Sprintf("events[%d].tool_name", index), Rule: "must not be empty after normalization"}
		}
		if _, ok := seen[id]; ok {
			return &ViolationError{
				Field: fmt.Sprintf("events[%d].client_event_id", index),
				Rule:  "must be unique within the trace",
			}
		}
		seen[id] = struct{}{}
	}
	return nil
}

func canonicalToken(value string) string {
	return norm.NFC.String(strings.TrimSpace(value))
}
