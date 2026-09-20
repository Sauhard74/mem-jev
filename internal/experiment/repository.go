package experiment

import (
	"context"
	"errors"
)

var (
	ErrExperimentConflict = errors.New("experiment reservation conflict")
	ErrExperimentNotFound = errors.New("experiment assignment not found")
)

type Disposition string

const (
	DispositionReserved  Disposition = "reserved"
	DispositionDuplicate Disposition = "duplicate"
)

type Reservation struct {
	Assignment  Assignment
	Disposition Disposition
}

type Repository interface {
	Reserve(context.Context, AssignmentRequest) (Reservation, error)
	Find(context.Context, string, string, string) (Assignment, error)
}

func EquivalentRouting(left, right Assignment) bool {
	return left.TenantID == right.TenantID && left.ManifestID == right.ManifestID && left.AssignmentKeyHash == right.AssignmentKeyHash && left.QueryBucketHash == right.QueryBucketHash && left.AssignedProcedureVersionID == right.AssignedProcedureVersionID && left.Bucket == right.Bucket && left.Challenger == right.Challenger && left.Risk == right.Risk && left.WindowStart.Equal(right.WindowStart)
}
