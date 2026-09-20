package jev

import (
	"reflect"
	"testing"
)

func TestAdmitAmbiguousCandidatesIsDeterministic(t *testing.T) {
	policy := AdmissionPolicy{MaximumCandidates: 3, AmbiguityScoreDistance: 100}
	closeScores := []RankedCandidate{{VersionID: "c", Rank: 3, FinalScore: 850}, {VersionID: "a", Rank: 1, FinalScore: 1_000}, {VersionID: "b", Rank: 2, FinalScore: 950}, {VersionID: "d", Rank: 4, FinalScore: 840}}
	got, err := AdmitAmbiguousCandidates(policy, closeScores)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("admitted = %#v; want %#v", got, want)
	}

	clearWinner := []RankedCandidate{{VersionID: "a", Rank: 1, FinalScore: 1_000}, {VersionID: "b", Rank: 2, FinalScore: 899}}
	got, err = AdmitAmbiguousCandidates(policy, clearWinner)
	if err != nil || len(got) != 0 {
		t.Fatalf("clear winner admitted = %#v, %v", got, err)
	}
}

func TestAdmitAmbiguousCandidatesRejectsMalformedRanks(t *testing.T) {
	_, err := AdmitAmbiguousCandidates(AdmissionPolicy{MaximumCandidates: 2, AmbiguityScoreDistance: 100}, []RankedCandidate{{VersionID: "a", Rank: 1, FinalScore: 1}, {VersionID: "b", Rank: 1, FinalScore: 1}})
	if err == nil {
		t.Fatal("duplicate rank accepted")
	}
}
