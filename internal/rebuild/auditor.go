package rebuild

import (
	"bytes"
	"context"
	"errors"

	"github.com/sauhard74/mem-jev/internal/observability"
)

var ErrProjectionMismatch = errors.New("rebuilt projection bytes differ from stored canonical projection")

// CompareProjection is the fail-closed boundary used by projection rebuilds
// before a rebuilt generation can be promoted for serving.
func CompareProjection(ctx context.Context, stored, rebuilt []byte) error {
	if len(stored) == 0 || len(rebuilt) == 0 || !bytes.Equal(stored, rebuilt) {
		observability.RecordRebuildMismatch(ctx, "canonical_projection")
		return ErrProjectionMismatch
	}
	return nil
}
