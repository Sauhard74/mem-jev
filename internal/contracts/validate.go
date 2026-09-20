package contracts

import (
	"fmt"

	"buf.build/go/protovalidate"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
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

	seen := make(map[string]struct{}, len(request.GetEvents()))
	for index, event := range request.GetEvents() {
		id := event.GetClientEventId()
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
