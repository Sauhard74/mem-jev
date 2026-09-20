package ingest

import (
	"errors"
	"fmt"

	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/store"
)

var (
	ErrInvalidCommand   = errors.New("invalid ingest command")
	ErrPermissionDenied = errors.New("principal lacks ingest permission")
	ErrMisconfigured    = errors.New("ingest service is not configured")
)

type Disposition = store.IngestDisposition

const (
	DispositionAccepted  = store.DispositionAccepted
	DispositionDuplicate = store.DispositionDuplicate
)

type Result struct {
	ReceiptID      domain.ReceiptID
	TraceID        domain.TraceID
	CanonicalHash  string
	ArchiveKey     archive.Key
	Disposition    Disposition
	AcceptedEvents int
	Redactions     int
}

type OperationError struct {
	Operation string
	Err       error
}

func (e *OperationError) Error() string {
	return fmt.Sprintf("%s: %v", e.Operation, e.Err)
}

func (e *OperationError) Unwrap() error { return e.Err }

func operationError(operation string, err error) error {
	return &OperationError{Operation: operation, Err: err}
}
