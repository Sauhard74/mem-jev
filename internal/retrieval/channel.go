package retrieval

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
)

type ChannelName string

const (
	ChannelExact   ChannelName = "exact"
	ChannelLexical ChannelName = "lexical"
	ChannelFacet   ChannelName = "facet"
	ChannelGraph   ChannelName = "graph"
	ChannelVector  ChannelName = "vector"
)

type ChannelRequest struct {
	TenantID        domain.TenantID
	ProjectionEpoch uint64
	Query           Query
	Limit           uint32
}

type Hit struct {
	VersionID         string
	RawScoreQuantized int64
	IndexManifestID   string
	Approximate       bool
}

type Channel interface {
	Name() ChannelName
	ManifestID() string
	Approximate() bool
	Search(context.Context, ChannelRequest) ([]Hit, error)
}

type ChannelError struct {
	Code string
	Err  error
}

func (e *ChannelError) Error() string { return fmt.Sprintf("candidate channel %s: %v", e.Code, e.Err) }
func (e *ChannelError) Unwrap() error { return e.Err }

func validChannel(value ChannelName) bool {
	return value == ChannelExact || value == ChannelLexical || value == ChannelFacet || value == ChannelGraph || value == ChannelVector
}

func channelFailure(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var channelErr *ChannelError
	if errors.As(err, &channelErr) && channelErr.Code != "" {
		return channelErr.Code
	}
	return "channel_failed"
}

type CollectRequest struct {
	Request  ChannelRequest
	Channels []Channel
	Timeout  time.Duration
}
