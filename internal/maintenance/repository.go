package maintenance

import (
	"context"
	"time"
)

type QueueStats struct {
	Kind             Kind
	Pending          int64
	OldestPendingAge time.Duration
}

type Repository interface {
	Enqueue(context.Context, Spec) error
	Claim(context.Context, ClaimRequest) ([]Lease, error)
	Complete(context.Context, FinishRequest) error
	Fail(context.Context, FailRequest) error
	Stats(context.Context) ([]QueueStats, error)
}
