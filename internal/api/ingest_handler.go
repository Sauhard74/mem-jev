package api

import (
	"context"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/ingest"
	"github.com/sauhard74/mem-jev/internal/security"
)

type ingestHandler struct {
	service *ingest.Service
}

func (h *ingestHandler) IngestTrace(ctx context.Context, request *connect.Request[memjevv1.IngestTraceRequest]) (*connect.Response[memjevv1.IngestTraceResponse], error) {
	principal, ok := security.PrincipalFromContext(ctx)
	if !ok {
		return nil, safeConnectError(ctx, connect.CodeUnauthenticated, "authentication_required", false)
	}
	metadata, ok := security.RequestMetadataFromContext(ctx)
	if !ok {
		return nil, safeConnectError(ctx, connect.CodeInvalidArgument, "idempotency_key_required", false)
	}
	result, err := h.service.Ingest(ctx, ingest.Command{
		Principal:          principal,
		IdempotencyKeyHash: metadata.IdempotencyKeyHash,
		Request:            request.Msg,
	})
	if err != nil {
		return nil, mapDomainError(ctx, err)
	}
	disposition := memjevv1.IngestDisposition_INGEST_DISPOSITION_UNSPECIFIED
	switch result.Disposition {
	case ingest.DispositionAccepted:
		disposition = memjevv1.IngestDisposition_INGEST_DISPOSITION_ACCEPTED
	case ingest.DispositionDuplicate:
		disposition = memjevv1.IngestDisposition_INGEST_DISPOSITION_DUPLICATE
	}
	return connect.NewResponse(&memjevv1.IngestTraceResponse{
		ReceiptId:      string(result.ReceiptID),
		TraceId:        string(result.TraceID),
		CanonicalHash:  result.CanonicalHash,
		Disposition:    disposition,
		AcceptedEvents: uint32(result.AcceptedEvents),
	}), nil
}
