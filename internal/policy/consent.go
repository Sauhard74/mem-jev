package policy

import "errors"

type ConsentMode string

const (
	Deny           ConsentMode = "deny"
	RecallOnly     ConsentMode = "recall_only"
	LearnAndRecall ConsentMode = "learn_and_recall"
)

var ErrConsentDenied = errors.New("consent does not permit learning")

func AuthorizeIngest(mode ConsentMode) error {
	if mode != LearnAndRecall {
		return ErrConsentDenied
	}
	return nil
}
