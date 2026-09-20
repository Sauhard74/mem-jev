package canonical

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/sauhard74/mem-jev/internal/domain"
)

var ErrInvalidCanonicalBatch = errors.New("invalid canonical batch")

func VerifyBatch(batch domain.CanonicalBatch, body []byte) error {
	if batch.SchemaVersion != schemaVersion || batch.TenantID == "" || batch.Trace.ID == "" || len(batch.Events) == 0 {
		return ErrInvalidCanonicalBatch
	}
	trace := batch.Trace
	wantTraceID := trace.ID
	trace.ID = ""
	derivedTraceID, err := deriveID("tr", traceIdentity{TenantID: batch.TenantID, Trace: trace})
	if err != nil || wantTraceID != domain.TraceID(derivedTraceID) {
		return fmt.Errorf("%w: trace identity", ErrInvalidCanonicalBatch)
	}
	seen := make(map[domain.EventID]struct{}, len(batch.Events))
	for index, source := range batch.Events {
		if source.Position != uint32(index) {
			return fmt.Errorf("%w: event order", ErrInvalidCanonicalBatch)
		}
		event := source
		wantEventID := event.ID
		event.ID = ""
		derivedEventID, deriveErr := deriveID("ev", eventIdentity{TenantID: batch.TenantID, TraceID: batch.Trace.ID, Position: uint32(index), Event: event})
		if deriveErr != nil || wantEventID != domain.EventID(derivedEventID) {
			return fmt.Errorf("%w: event identity", ErrInvalidCanonicalBatch)
		}
		if _, duplicate := seen[wantEventID]; duplicate {
			return fmt.Errorf("%w: duplicate event", ErrInvalidCanonicalBatch)
		}
		seen[wantEventID] = struct{}{}
	}
	copyOfBatch := batch
	copyOfBatch.Hash = ""
	copyOfBatch.CanonicalJSON = nil
	encoded, hash, err := MarshalAndHash(copyOfBatch)
	if err != nil || !bytes.Equal(encoded, body) {
		return fmt.Errorf("%w: canonical encoding", ErrInvalidCanonicalBatch)
	}
	if batch.Hash != "" && batch.Hash != hash {
		return fmt.Errorf("%w: content hash", ErrInvalidCanonicalBatch)
	}
	return nil
}
