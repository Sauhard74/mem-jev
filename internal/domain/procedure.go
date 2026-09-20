package domain

type ProcedureStep struct {
	EventID               EventID `json:"event_id"`
	Ordinal               uint32  `json:"ordinal"`
	OriginalPosition      uint32  `json:"original_position"`
	ToolName              string  `json:"tool_name"`
	ToolVersion           string  `json:"tool_version,omitempty"`
	ToolContractVersionID string  `json:"tool_contract_version_id"`
	UncertainNecessity    bool    `json:"uncertain_necessity"`
	CompensationBoundary  bool    `json:"compensation_boundary"`
}
