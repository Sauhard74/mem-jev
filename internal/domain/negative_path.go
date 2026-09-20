package domain

type CompatibilityScope struct {
	EnvironmentHash string `json:"environment_hash"`
	ToolHash        string `json:"tool_hash"`
	ResourceHash    string `json:"resource_hash"`
}

func (s CompatibilityScope) Valid() bool {
	return s.EnvironmentHash != "" && s.ToolHash != "" && s.ResourceHash != ""
}

type NegativePath struct {
	ID                 string             `json:"id"`
	FailurePredicateID string             `json:"failure_predicate_id"`
	EventIDs           []EventID          `json:"event_ids"`
	Scope              CompatibilityScope `json:"scope"`
}

func (p NegativePath) CompatibleWith(scope CompatibilityScope) bool {
	return p.Scope.Valid() && scope.Valid() && p.Scope == scope
}
