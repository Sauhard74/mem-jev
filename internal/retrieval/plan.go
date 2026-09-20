package retrieval

import (
	"context"
	"errors"

	"github.com/sauhard74/mem-jev/internal/domain"
)

var (
	ErrPlanUnavailable = errors.New("executable plan unavailable")
	ErrPlanNotFound    = errors.New("executable plan not found")
)

type PlanIssuer interface {
	Issue(context.Context, Run, []Document) (PlanArtifact, error)
	FindByRetrievalRunID(context.Context, domain.TenantID, string) (PlanArtifact, error)
}

type PlanNode struct {
	Ordinal       uint32
	VersionID     string
	InterfaceHash string
	Bridge        bool
}

type PlanDependency struct {
	CompatibilityEdgeID     string
	SourceVersionID         string
	TargetVersionID         string
	SourceProvisionIDs      []string
	SatisfiedRequirementIDs []string
}

type ParallelGroup struct {
	Ordinal        uint32
	NodeVersionIDs []string
}

type PlanGap struct {
	VersionID, RequirementID, GoalPredicateID, Code string
}

type PlanArtifact struct {
	InjectionID, TaskExecutionID, SelectionHash, PlanHash string
	CompatibilityGraphID, CompatibilityGraphHash          string
	CompatibilityMatrixHash, CandidateSetHash             string
	PlannerManifestID, PolicyManifestID, RankerManifestID string
	ServingConfigID, DocumentSetHash                      string
	TenantID                                              domain.TenantID
	RetrievalRunID, QueryHash, RequestContextHash         string
	ProjectionEpoch                                       uint64
	NoveltyClass                                          string
	Complete                                              bool
	Nodes                                                 []PlanNode
	Dependencies                                          []PlanDependency
	ParallelGroups                                        []ParallelGroup
	Gaps                                                  []PlanGap
	LimitCodes                                            []string
}
