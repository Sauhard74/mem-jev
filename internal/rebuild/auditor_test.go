package rebuild_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sauhard74/mem-jev/internal/rebuild"
)

func TestCompareProjectionFailsClosedOnAnyByteDifference(t *testing.T) {
	if err := rebuild.CompareProjection(context.Background(), []byte(`{"a":1}`), []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	for _, rebuilt := range [][]byte{nil, []byte(`{"a":2}`), []byte(" {\"a\":1}")} {
		if err := rebuild.CompareProjection(context.Background(), []byte(`{"a":1}`), rebuilt); !errors.Is(err, rebuild.ErrProjectionMismatch) {
			t.Fatalf("error = %v", err)
		}
	}
}
