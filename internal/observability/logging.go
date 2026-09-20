package observability

import (
	"context"
	"io"
	"log/slog"
)

type RequestFacts struct {
	RequestID, TenantHash, ResultCode string
	Bytes, Events                     int
	LatencyMilliseconds               int64
}

func NewJSONLogger(writer io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func LogRequest(ctx context.Context, logger *slog.Logger, facts RequestFacts) {
	logger.InfoContext(ctx, "request completed",
		"request_id", facts.RequestID,
		"tenant_hash", facts.TenantHash,
		"result_code", facts.ResultCode,
		"bytes", facts.Bytes,
		"events", facts.Events,
		"latency_ms", facts.LatencyMilliseconds,
	)
}
