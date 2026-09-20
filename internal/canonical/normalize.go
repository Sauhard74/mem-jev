package canonical

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/contracts"
	"github.com/sauhard74/mem-jev/internal/domain"
	"golang.org/x/text/unicode/norm"
)

const schemaVersion = "canonical.v1"

type traceIdentity struct {
	TenantID domain.TenantID       `json:"tenant_id"`
	Trace    domain.CanonicalTrace `json:"trace"`
}

type eventIdentity struct {
	TenantID domain.TenantID       `json:"tenant_id"`
	TraceID  domain.TraceID        `json:"trace_id"`
	Position uint32                `json:"position"`
	Event    domain.CanonicalEvent `json:"event"`
}

func Build(tenantID domain.TenantID, request *memjevv1.IngestTraceRequest) (domain.CanonicalBatch, error) {
	if tenantID == "" {
		return domain.CanonicalBatch{}, fmt.Errorf("tenant_id: required")
	}
	if err := contracts.ValidateIngest(request); err != nil {
		return domain.CanonicalBatch{}, err
	}

	environment, err := normalizeFields("environment", request.GetEnvironment())
	if err != nil {
		return domain.CanonicalBatch{}, err
	}
	trace := domain.CanonicalTrace{
		ClientTraceID:  normalizeToken(request.GetClientTraceId()),
		Harness:        normalizeToken(request.GetHarness()),
		HarnessVersion: normalizeToken(request.GetHarnessVersion()),
		Task:           normalizeText(request.GetTask()),
		Environment:    environment,
	}
	traceID, err := deriveID("tr", traceIdentity{TenantID: tenantID, Trace: trace})
	if err != nil {
		return domain.CanonicalBatch{}, fmt.Errorf("derive trace ID: %w", err)
	}
	trace.ID = domain.TraceID(traceID)

	events := make([]domain.CanonicalEvent, len(request.GetEvents()))
	for index, source := range request.GetEvents() {
		event, normalizeErr := normalizeEvent(index, source)
		if normalizeErr != nil {
			return domain.CanonicalBatch{}, normalizeErr
		}
		eventID, deriveErr := deriveID("ev", eventIdentity{
			TenantID: tenantID,
			TraceID:  trace.ID,
			Position: uint32(index),
			Event:    event,
		})
		if deriveErr != nil {
			return domain.CanonicalBatch{}, fmt.Errorf("derive event ID at position %d: %w", index, deriveErr)
		}
		event.ID = domain.EventID(eventID)
		events[index] = event
	}

	batch := domain.CanonicalBatch{
		SchemaVersion: schemaVersion,
		TenantID:      tenantID,
		Trace:         trace,
		Events:        events,
	}
	canonicalJSON, hash, err := MarshalAndHash(batch)
	if err != nil {
		return domain.CanonicalBatch{}, err
	}
	batch.Hash = hash
	batch.CanonicalJSON = canonicalJSON
	return batch, nil
}

func normalizeEvent(index int, source *memjevv1.TraceEvent) (domain.CanonicalEvent, error) {
	if source == nil {
		return domain.CanonicalEvent{}, fmt.Errorf("events[%d]: required", index)
	}
	if err := source.GetOccurredAt().CheckValid(); err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("events[%d].occurred_at: %w", index, err)
	}
	fields, err := normalizeFields(fmt.Sprintf("events[%d].fields", index), source.GetFields())
	if err != nil {
		return domain.CanonicalEvent{}, err
	}

	event := domain.CanonicalEvent{
		Position:      uint32(index),
		ClientEventID: normalizeToken(source.GetClientEventId()),
		OccurredAt:    source.GetOccurredAt().AsTime().UTC().Format(time.RFC3339Nano),
		Kind:          source.GetKind().String(),
		ToolName:      normalizeToken(source.GetToolName()),
		ToolVersion:   normalizeToken(source.GetToolVersion()),
		Fields:        fields,
	}
	if source.GetResult() != nil {
		evidence, evidenceErr := normalizeFields(fmt.Sprintf("events[%d].result.evidence", index), source.GetResult().GetEvidence())
		if evidenceErr != nil {
			return domain.CanonicalEvent{}, evidenceErr
		}
		result := &domain.CanonicalResult{
			State:    source.GetResult().GetState().String(),
			Evidence: evidence,
		}
		if source.GetResult().ExitCode != nil {
			exitCode := source.GetResult().GetExitCode()
			result.ExitCode = &exitCode
		}
		event.Result = result
	}
	return event, nil
}

func normalizeFields(location string, source []*memjevv1.Field) ([]domain.CanonicalField, error) {
	if len(source) == 0 {
		return nil, nil
	}
	fields := make([]domain.CanonicalField, 0, len(source))
	seen := make(map[string]struct{}, len(source))
	for index, item := range source {
		if item == nil {
			return nil, fmt.Errorf("%s[%d]: required", location, index)
		}
		name := normalizeToken(item.GetName())
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("%s.%s: duplicate canonical field", location, name)
		}
		seen[name] = struct{}{}
		value := norm.NFC.String(item.GetStringValue())
		if name == "path" {
			var err error
			value, err = normalizePath(value)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", location, name, err)
			}
		}
		fields = append(fields, domain.CanonicalField{Name: name, Value: value})
	}
	sort.Slice(fields, func(i, j int) bool {
		if fields[i].Name == fields[j].Name {
			return fields[i].Value < fields[j].Value
		}
		return fields[i].Name < fields[j].Name
	})
	return fields, nil
}

func normalizePath(value string) (string, error) {
	normalized := strings.ReplaceAll(norm.NFC.String(strings.TrimSpace(value)), "\\", "/")
	if strings.HasPrefix(normalized, "/") {
		return "", fmt.Errorf("must be relative")
	}
	normalized = path.Clean(normalized)
	if normalized == ".." || strings.HasPrefix(normalized, "../") {
		return "", fmt.Errorf("must not escape its root")
	}
	return normalized, nil
}

func normalizeToken(value string) string {
	return norm.NFC.String(strings.TrimSpace(value))
}

func normalizeText(value string) string {
	return norm.NFC.String(strings.TrimSpace(strings.ReplaceAll(value, "\r\n", "\n")))
}
