package projection

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/sauhard74/mem-jev/internal/canonical"
)

var (
	ErrInvalidProjection  = errors.New("invalid projection")
	ErrProjectionConflict = errors.New("immutable projection conflicts with stored content")
	ErrProjectionNotFound = errors.New("projection not found")
)

type Disposition string

const (
	DispositionPublished Disposition = "published"
	DispositionDuplicate Disposition = "duplicate"
)

type PublishReceipt struct {
	ManifestID         string
	ProcedureID        string
	ProcedureVersionID string
	Disposition        Disposition
	NewFamily          bool
	NewVersion         bool
	EvidenceAdded      bool
}

type Repository interface {
	Publish(context.Context, Projection) (PublishReceipt, error)
}

type Counts struct {
	Families      int
	Versions      int
	Steps         int
	Edges         int
	NegativePaths int
	Manifests     int
	EvidenceLinks int
}

func Validate(value Projection) error {
	if value.TenantID == "" || value.Manifest.ID == "" || value.Manifest.TenantID != value.TenantID || value.Manifest.ContentHash == "" || value.CreatedAt.IsZero() {
		return ErrInvalidProjection
	}
	if value.Manifest.Status == "abstained" {
		if value.Manifest.AbstentionCode == "" || value.Version.ID != "" || len(value.Steps) != 0 || len(value.CanonicalProjectionJSON) != 0 {
			return ErrInvalidProjection
		}
		return validateManifest(value.Manifest)
	}
	if value.Manifest.Status != "synthesized" || value.Family.ID == "" || value.Version.ID == "" ||
		value.Version.ProcedureID != value.Family.ID || value.Manifest.ProcedureVersionID != value.Version.ID ||
		len(value.Steps) == 0 || len(value.CanonicalProjectionJSON) == 0 {
		return ErrInvalidProjection
	}
	for index, step := range value.Steps {
		if step.ID == "" || step.Ordinal != uint32(index) || step.ContentHash == "" {
			return ErrInvalidProjection
		}
	}
	for _, edge := range value.Edges {
		if edge.ID == "" || edge.FromOrdinal >= uint32(len(value.Steps)) || edge.ToOrdinal >= uint32(len(value.Steps)) || edge.FromOrdinal >= edge.ToOrdinal {
			return ErrInvalidProjection
		}
	}
	for _, path := range value.NegativePaths {
		if path.ID == "" || path.FailurePredicateID == "" || len(path.EventIDs) == 0 || !path.Scope.Valid() {
			return ErrInvalidProjection
		}
		copyOfPath := path
		copyOfPath.ID = ""
		_, hash, err := canonical.MarshalAndHash(copyOfPath)
		if err != nil || path.ID != "neg_"+hash {
			return ErrInvalidProjection
		}
		for _, eventID := range path.EventIDs {
			if eventID == "" {
				return ErrInvalidProjection
			}
		}
	}
	if err := validateManifest(value.Manifest); err != nil {
		return err
	}
	return validateProcedureProjection(value)
}

func validateManifest(manifest Manifest) error {
	copyOfManifest := manifest
	copyOfManifest.ID, copyOfManifest.ContentHash = "", ""
	_, hash, err := canonical.MarshalAndHash(copyOfManifest)
	if err != nil || manifest.ID != "syn_"+hash || manifest.ContentHash != hash {
		return ErrInvalidProjection
	}
	return nil
}

func validateProcedureProjection(value Projection) error {
	familyIdentity := struct {
		TenantID            string `json:"tenant_id"`
		IntentHash          string `json:"intent_hash"`
		EffectSignatureHash string `json:"effect_signature_hash"`
	}{string(value.TenantID), value.Family.IntentHash, value.Family.EffectSignatureHash}
	_, familyHash, err := canonical.MarshalAndHash(familyIdentity)
	if err != nil || value.Family.ID != "proc_"+familyHash || value.Family.ContentHash != familyHash {
		return ErrInvalidProjection
	}
	logicalSteps := make([]logicalStep, len(value.Steps))
	for index, step := range value.Steps {
		logicalSteps[index] = logicalStep{
			Ordinal: step.Ordinal, ToolName: step.ToolName, ToolVersion: step.ToolVersion,
			ToolContractVersionID: step.ToolContractVersionID, UncertainNecessity: step.UncertainNecessity,
			CompensationBoundary: step.CompensationBoundary,
		}
		_, hash, hashErr := canonical.MarshalAndHash(struct {
			VersionID string      `json:"version_id"`
			Step      logicalStep `json:"step"`
		}{value.Version.ID, logicalSteps[index]})
		if hashErr != nil || step.ID != "step_"+hash || step.ContentHash != hash {
			return ErrInvalidProjection
		}
	}
	logicalEdges := make([]logicalEdge, len(value.Edges))
	for index, edge := range value.Edges {
		logicalEdges[index] = logicalEdge{
			FromOrdinal: edge.FromOrdinal, ToOrdinal: edge.ToOrdinal, Type: edge.Type,
			ResourceName: edge.ResourceName, ResourceType: edge.ResourceType, ResourceNamespace: edge.ResourceNamespace,
		}
	}
	sort.Slice(logicalEdges, func(i, j int) bool {
		left := fmt.Sprintf("%010d\x00%010d\x00%s\x00%s\x00%s\x00%s", logicalEdges[i].FromOrdinal, logicalEdges[i].ToOrdinal, logicalEdges[i].Type, logicalEdges[i].ResourceNamespace, logicalEdges[i].ResourceType, logicalEdges[i].ResourceName)
		right := fmt.Sprintf("%010d\x00%010d\x00%s\x00%s\x00%s\x00%s", logicalEdges[j].FromOrdinal, logicalEdges[j].ToOrdinal, logicalEdges[j].Type, logicalEdges[j].ResourceNamespace, logicalEdges[j].ResourceType, logicalEdges[j].ResourceName)
		return left < right
	})
	for index, logical := range logicalEdges {
		_, hash, hashErr := canonical.MarshalAndHash(struct {
			VersionID string      `json:"version_id"`
			Edge      logicalEdge `json:"edge"`
		}{value.Version.ID, logical})
		if hashErr != nil || value.Edges[index].ID != "edge_"+hash || value.Edges[index].ContentHash != hash {
			return ErrInvalidProjection
		}
	}
	goals := append([]string(nil), value.GoalPredicates...)
	sort.Strings(goals)
	graph := logicalGraph{Steps: logicalSteps, Edges: logicalEdges, GoalPredicates: goals, EnvironmentScopeHash: value.Version.EnvironmentScopeHash}
	_, graphHash, err := canonical.MarshalAndHash(graph)
	if err != nil || value.Version.GraphHash != graphHash {
		return ErrInvalidProjection
	}
	versionIdentity := struct {
		TenantID         string `json:"tenant_id"`
		ProcedureID      string `json:"procedure_id"`
		GraphHash        string `json:"graph_hash"`
		PolicyVersion    string `json:"policy_version"`
		ObservedEndToEnd bool   `json:"observed_end_to_end"`
	}{string(value.TenantID), value.Family.ID, graphHash, value.Version.PolicyVersion, value.Version.ObservedEndToEnd}
	_, versionHash, err := canonical.MarshalAndHash(versionIdentity)
	if err != nil || value.Version.ID != "pv_"+versionHash || value.Version.ContentHash != versionHash {
		return ErrInvalidProjection
	}
	canonicalProjection := struct {
		Family  Family  `json:"family"`
		Version Version `json:"version"`
		Steps   []Step  `json:"steps"`
		Edges   []Edge  `json:"edges,omitempty"`
	}{value.Family, value.Version, value.Steps, value.Edges}
	encoded, _, err := canonical.MarshalAndHash(canonicalProjection)
	if err != nil || !bytes.Equal(encoded, value.CanonicalProjectionJSON) {
		return ErrInvalidProjection
	}
	return nil
}
