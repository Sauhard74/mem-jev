package api

import (
	"context"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/credit"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/outcome"
	"github.com/sauhard74/mem-jev/internal/security"
	"github.com/sauhard74/mem-jev/internal/store"
)

type outcomeHandler struct {
	service *outcome.Service
}

func (h *outcomeHandler) RecordOutcome(ctx context.Context, request *connect.Request[memjevv1.RecordOutcomeRequest]) (*connect.Response[memjevv1.RecordOutcomeResponse], error) {
	principal, ok := security.PrincipalFromContext(ctx)
	if !ok {
		return nil, safeConnectError(ctx, connect.CodeUnauthenticated, "authentication_required", false)
	}
	metadata, ok := security.RequestMetadataFromContext(ctx)
	if !ok {
		return nil, safeConnectError(ctx, connect.CodeInvalidArgument, "idempotency_key_required", false)
	}
	result, err := h.service.Record(ctx, outcome.Command{Principal: principal, IdempotencyKeyHash: metadata.IdempotencyKeyHash, Request: request.Msg})
	if err != nil {
		return nil, mapDomainError(ctx, err)
	}
	return connect.NewResponse(&memjevv1.RecordOutcomeResponse{
		OutcomeId: string(result.OutcomeID), TraceId: string(result.TraceID), State: outcomeState(result.State),
		Disposition: outcomeDisposition(result.Disposition), PromotionEligible: result.PromotionEligible, PolicyVersion: result.PolicyVersion,
		OutcomeCreditId: result.OutcomeCreditID, CreditClass: outcomeCreditClass(result.CreditClass),
	}), nil
}

func outcomeCreditClass(value credit.Class) memjevv1.OutcomeCreditClass {
	switch value {
	case credit.CausalSuccess:
		return memjevv1.OutcomeCreditClass_OUTCOME_CREDIT_CLASS_CAUSAL_SUCCESS
	case credit.AssociatedSuccess:
		return memjevv1.OutcomeCreditClass_OUTCOME_CREDIT_CLASS_ASSOCIATED_SUCCESS
	case credit.CausalFailure:
		return memjevv1.OutcomeCreditClass_OUTCOME_CREDIT_CLASS_CAUSAL_FAILURE
	case credit.AssociatedFailure:
		return memjevv1.OutcomeCreditClass_OUTCOME_CREDIT_CLASS_ASSOCIATED_FAILURE
	case credit.Unattributable:
		return memjevv1.OutcomeCreditClass_OUTCOME_CREDIT_CLASS_UNATTRIBUTABLE
	default:
		return memjevv1.OutcomeCreditClass_OUTCOME_CREDIT_CLASS_UNSPECIFIED
	}
}

func outcomeState(state domain.OutcomeState) memjevv1.OutcomeState {
	switch state {
	case domain.OutcomeStateVerifiedSuccess:
		return memjevv1.OutcomeState_OUTCOME_STATE_VERIFIED_SUCCESS
	case domain.OutcomeStateProvisionalSuccess:
		return memjevv1.OutcomeState_OUTCOME_STATE_PROVISIONAL_SUCCESS
	case domain.OutcomeStateInconclusive:
		return memjevv1.OutcomeState_OUTCOME_STATE_INCONCLUSIVE
	case domain.OutcomeStateVerifiedFailure:
		return memjevv1.OutcomeState_OUTCOME_STATE_VERIFIED_FAILURE
	default:
		return memjevv1.OutcomeState_OUTCOME_STATE_UNSPECIFIED
	}
}

func outcomeDisposition(disposition store.OutcomeDisposition) memjevv1.OutcomeDisposition {
	switch disposition {
	case store.OutcomeDispositionAccepted:
		return memjevv1.OutcomeDisposition_OUTCOME_DISPOSITION_ACCEPTED
	case store.OutcomeDispositionDuplicate:
		return memjevv1.OutcomeDisposition_OUTCOME_DISPOSITION_DUPLICATE
	default:
		return memjevv1.OutcomeDisposition_OUTCOME_DISPOSITION_UNSPECIFIED
	}
}
