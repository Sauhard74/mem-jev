package api

import (
	"context"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/buildinfo"
)

type healthHandler struct {
	info      buildinfo.Info
	readiness func(context.Context) error
}

func (h *healthHandler) Check(ctx context.Context, _ *connect.Request[memjevv1.CheckRequest]) (*connect.Response[memjevv1.CheckResponse], error) {
	status := memjevv1.CheckResponse_STATUS_SERVING
	if h.readiness != nil {
		if err := h.readiness(ctx); err != nil {
			status = memjevv1.CheckResponse_STATUS_NOT_SERVING
		}
	}
	return connect.NewResponse(&memjevv1.CheckResponse{
		Status:  status,
		Version: h.info.Version,
		Commit:  h.info.Commit,
		BuiltAt: h.info.BuiltAt,
	}), nil
}
