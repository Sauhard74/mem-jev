package domain

type CanonicalField struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type CanonicalResult struct {
	State    string           `json:"state"`
	ExitCode *int32           `json:"exit_code,omitempty"`
	Evidence []CanonicalField `json:"evidence,omitempty"`
}

type CanonicalEvent struct {
	ID            EventID          `json:"id"`
	Position      uint32           `json:"position"`
	ClientEventID string           `json:"client_event_id"`
	OccurredAt    string           `json:"occurred_at"`
	Kind          string           `json:"kind"`
	ToolName      string           `json:"tool_name"`
	ToolVersion   string           `json:"tool_version,omitempty"`
	Fields        []CanonicalField `json:"fields,omitempty"`
	Result        *CanonicalResult `json:"result,omitempty"`
}

type CanonicalTrace struct {
	ID             TraceID          `json:"id"`
	ClientTraceID  string           `json:"client_trace_id"`
	Harness        string           `json:"harness"`
	HarnessVersion string           `json:"harness_version,omitempty"`
	Task           string           `json:"task,omitempty"`
	Environment    []CanonicalField `json:"environment,omitempty"`
}

type CanonicalBatch struct {
	SchemaVersion string           `json:"schema_version"`
	TenantID      TenantID         `json:"tenant_id"`
	Trace         CanonicalTrace   `json:"trace"`
	Events        []CanonicalEvent `json:"events"`
	Hash          string           `json:"-"`
	CanonicalJSON []byte           `json:"-"`
}
