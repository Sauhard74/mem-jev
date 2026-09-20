package rebuild_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/rebuild"
)

func TestDerivedSnapshotIsPermutationInvariantAndAuthorizesExactRebuild(t *testing.T) {
	input := derivedInput()
	want, err := rebuild.BuildDerivedSnapshot(input)
	if err != nil {
		t.Fatal(err)
	}
	for seed := int64(0); seed < 100; seed++ {
		shuffled := input
		shuffled.Compatibility = slices.Clone(input.Compatibility)
		shuffled.SelectionCredits = slices.Clone(input.SelectionCredits)
		rand.New(rand.NewSource(seed)).Shuffle(len(shuffled.Compatibility), func(i, j int) {
			shuffled.Compatibility[i], shuffled.Compatibility[j] = shuffled.Compatibility[j], shuffled.Compatibility[i]
		})
		rand.New(rand.NewSource(seed+1)).Shuffle(len(shuffled.SelectionCredits), func(i, j int) {
			shuffled.SelectionCredits[i], shuffled.SelectionCredits[j] = shuffled.SelectionCredits[j], shuffled.SelectionCredits[i]
		})
		got, err := rebuild.BuildDerivedSnapshot(shuffled)
		if err != nil || !bytes.Equal(got.CanonicalJSON, want.CanonicalJSON) {
			t.Fatalf("seed %d changed snapshot: %v", seed, err)
		}
	}
	permit, err := rebuild.CompareDerivedSnapshots(context.Background(), want, want, time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC))
	if err != nil || permit.ID == "" || permit.StoredSnapshotID != want.ID {
		t.Fatalf("permit=%#v err=%v", permit, err)
	}
}

func TestDerivedSnapshotMismatchFailsClosedByComponent(t *testing.T) {
	stored, err := rebuild.BuildDerivedSnapshot(derivedInput())
	if err != nil {
		t.Fatal(err)
	}
	changed := derivedInput()
	changed.LifecycleHeads[0].ContentHash = strings.Repeat("9", 64)
	rebuilt, err := rebuild.BuildDerivedSnapshot(changed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rebuild.CompareDerivedSnapshots(context.Background(), stored, rebuilt, time.Now().UTC()); !errors.Is(err, rebuild.ErrProjectionMismatch) {
		t.Fatalf("error=%v", err)
	}
}

func derivedInput() rebuild.DerivedInput {
	return rebuild.DerivedInput{TenantID: "tenant_a", ProjectionEpoch: 42,
		Compatibility:      []rebuild.RecordDigest{{ID: "edge_b", ContentHash: strings.Repeat("b", 64)}, {ID: "edge_a", ContentHash: strings.Repeat("a", 64)}},
		SelectionCredits:   []rebuild.RecordDigest{{ID: "credit_b", ContentHash: strings.Repeat("d", 64)}, {ID: "credit_a", ContentHash: strings.Repeat("c", 64)}},
		LifecycleDecisions: []rebuild.RecordDigest{{ID: "decision_a", ContentHash: strings.Repeat("e", 64)}},
		LifecycleHeads:     []rebuild.RecordDigest{{ID: "head_a", ContentHash: strings.Repeat("f", 64)}},
	}
}
