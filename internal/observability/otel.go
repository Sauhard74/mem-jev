package observability

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type TelemetryConfig struct {
	Endpoint, ServiceName, ServiceVersion, Environment string
}

type Shutdown func(context.Context) error

func Setup(ctx context.Context, config TelemetryConfig) (Shutdown, error) {
	if config.Endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", config.ServiceName),
		attribute.String("service.version", config.ServiceVersion),
		attribute.String("deployment.environment.name", config.Environment),
	))
	if err != nil {
		return nil, err
	}
	traceEndpoint, err := signalEndpoint(config.Endpoint, "v1/traces")
	if err != nil {
		return nil, err
	}
	metricEndpoint, err := signalEndpoint(config.Endpoint, "v1/metrics")
	if err != nil {
		return nil, err
	}
	traceExporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(traceEndpoint))
	if err != nil {
		return nil, err
	}
	metricExporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(metricEndpoint))
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		return nil, err
	}
	traceProvider := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(traceExporter))
	metricProvider := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)))
	otel.SetTracerProvider(traceProvider)
	otel.SetMeterProvider(metricProvider)
	return func(ctx context.Context) error {
		return errors.Join(metricProvider.Shutdown(ctx), traceProvider.Shutdown(ctx))
	}, nil
}

func signalEndpoint(base, signalPath string) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("invalid OTLP base endpoint")
	}
	parsed.Path = path.Join(parsed.Path, signalPath)
	return parsed.String(), nil
}

func StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return otel.Tracer("github.com/sauhard74/mem-jev").Start(ctx, name)
}

type IngestMetrics struct {
	ResultCode   string
	Latency      time.Duration
	Events       int
	Bytes        int
	ArchiveReuse bool
}

type HTTPMetrics struct {
	ResultCode string
	Latency    time.Duration
	Bytes      int
}

type OutcomeMetrics struct {
	ResultCode string
	Latency    time.Duration
	Evidence   int
	Conflict   bool
}

type RetrievalMetrics struct {
	ResultCode, Disposition string
	Latency                 time.Duration
	Candidates              int
	Replayed                bool
}

type JevJudgmentMetrics struct {
	Disposition    string
	ProviderCalled bool
	Latency        time.Duration
	InputTokens    int64
	OutputTokens   int64
}

var (
	metricsOnce                 sync.Once
	requests                    metric.Int64Counter
	latency                     metric.Float64Histogram
	events                      metric.Int64Histogram
	requestBytes                metric.Int64Histogram
	archiveReuse                metric.Int64Counter
	transactionRetries          metric.Int64Counter
	outboxCreated               metric.Int64Counter
	httpRequests                metric.Int64Counter
	httpLatency                 metric.Float64Histogram
	httpBytes                   metric.Int64Histogram
	outcomeRequests             metric.Int64Counter
	outcomeLatency              metric.Float64Histogram
	outcomeEvidence             metric.Int64Histogram
	outcomeConflicts            metric.Int64Counter
	archiveCorruptions          metric.Int64Counter
	synthesisAbstentions        metric.Int64Counter
	outboxLeaseAge              metric.Float64Histogram
	outboxRetries               metric.Int64Counter
	outboxDeadLetters           metric.Int64Counter
	projectionLag               metric.Float64Histogram
	projectionRebuildMismatches metric.Int64Counter
	retrievalRequests           metric.Int64Counter
	retrievalLatency            metric.Float64Histogram
	retrievalCandidates         metric.Int64Histogram
	retrievalReplays            metric.Int64Counter
	retrievalChannels           metric.Int64Counter
	retrievalChannelLatency     metric.Float64Histogram
	retrievalGateRejections     metric.Int64Counter
	retrievalPersistenceFailure metric.Int64Counter
	retrievalReplayMismatch     metric.Int64Counter
	retrievalSnapshotAge        metric.Float64Histogram
	retrievalRankerRequests     metric.Int64Counter
	retrievalVectorStaleness    metric.Float64Histogram
	maintenanceBacklog          metric.Int64Gauge
	maintenanceOldestPending    metric.Float64Gauge
	maintenanceFailures         metric.Int64Counter
	maintenanceDeadLetters      metric.Int64Counter
	lifecycleTransitions        metric.Int64Counter
	experimentBudgetRejections  metric.Int64Counter
	jevJudgments                metric.Int64Counter
	jevAdmissions               metric.Int64Counter
	jevAdmittedCandidates       metric.Int64Histogram
	jevLatency                  metric.Float64Histogram
	jevInputTokens              metric.Int64Counter
	jevOutputTokens             metric.Int64Counter
)

func initializeMetrics() {
	meter := otel.Meter("github.com/sauhard74/mem-jev")
	requests, _ = meter.Int64Counter("memjev.requests")
	latency, _ = meter.Float64Histogram("memjev.request.duration", metric.WithUnit("ms"))
	events, _ = meter.Int64Histogram("memjev.request.events")
	requestBytes, _ = meter.Int64Histogram("memjev.request.bytes", metric.WithUnit("By"))
	archiveReuse, _ = meter.Int64Counter("memjev.archive.reuse")
	transactionRetries, _ = meter.Int64Counter("memjev.transaction.retries")
	outboxCreated, _ = meter.Int64Counter("memjev.outbox.created")
	httpRequests, _ = meter.Int64Counter("memjev.http.requests")
	httpLatency, _ = meter.Float64Histogram("memjev.http.request.duration", metric.WithUnit("ms"))
	httpBytes, _ = meter.Int64Histogram("memjev.http.request.bytes", metric.WithUnit("By"))
	outcomeRequests, _ = meter.Int64Counter("memjev.outcome.requests")
	outcomeLatency, _ = meter.Float64Histogram("memjev.outcome.duration", metric.WithUnit("ms"))
	outcomeEvidence, _ = meter.Int64Histogram("memjev.outcome.evidence")
	outcomeConflicts, _ = meter.Int64Counter("memjev.outcome.conflicts")
	archiveCorruptions, _ = meter.Int64Counter("memjev.archive.corruptions")
	synthesisAbstentions, _ = meter.Int64Counter("memjev.synthesis.abstentions")
	outboxLeaseAge, _ = meter.Float64Histogram("memjev.outbox.lease_age", metric.WithUnit("s"))
	outboxRetries, _ = meter.Int64Counter("memjev.outbox.retries")
	outboxDeadLetters, _ = meter.Int64Counter("memjev.outbox.dead_letters")
	projectionLag, _ = meter.Float64Histogram("memjev.projection.lag", metric.WithUnit("s"))
	projectionRebuildMismatches, _ = meter.Int64Counter("memjev.projection.rebuild_mismatches")
	retrievalRequests, _ = meter.Int64Counter("memjev.retrieval.requests")
	retrievalLatency, _ = meter.Float64Histogram("memjev.retrieval.duration", metric.WithUnit("ms"))
	retrievalCandidates, _ = meter.Int64Histogram("memjev.retrieval.candidates")
	retrievalReplays, _ = meter.Int64Counter("memjev.retrieval.replays")
	retrievalChannels, _ = meter.Int64Counter("memjev.retrieval.channels")
	retrievalChannelLatency, _ = meter.Float64Histogram("memjev.retrieval.channel.duration", metric.WithUnit("ms"))
	retrievalGateRejections, _ = meter.Int64Counter("memjev.retrieval.gate_rejections")
	retrievalPersistenceFailure, _ = meter.Int64Counter("memjev.retrieval.persistence_failures")
	retrievalReplayMismatch, _ = meter.Int64Counter("memjev.retrieval.replay_mismatches")
	retrievalSnapshotAge, _ = meter.Float64Histogram("memjev.retrieval.snapshot_age", metric.WithUnit("s"))
	retrievalRankerRequests, _ = meter.Int64Counter("memjev.retrieval.ranker_requests")
	retrievalVectorStaleness, _ = meter.Float64Histogram("memjev.retrieval.vector_staleness", metric.WithUnit("s"))
	maintenanceBacklog, _ = meter.Int64Gauge("memjev.maintenance.backlog")
	maintenanceOldestPending, _ = meter.Float64Gauge("memjev.maintenance.oldest_pending", metric.WithUnit("s"))
	maintenanceFailures, _ = meter.Int64Counter("memjev.maintenance.failures")
	maintenanceDeadLetters, _ = meter.Int64Counter("memjev.maintenance.dead_letters")
	lifecycleTransitions, _ = meter.Int64Counter("memjev.lifecycle.transitions")
	experimentBudgetRejections, _ = meter.Int64Counter("memjev.experiment.budget_rejections")
	jevJudgments, _ = meter.Int64Counter("memjev.jev.judgments")
	jevAdmissions, _ = meter.Int64Counter("memjev.jev.admissions")
	jevAdmittedCandidates, _ = meter.Int64Histogram("memjev.jev.admitted_candidates")
	jevLatency, _ = meter.Float64Histogram("memjev.jev.duration", metric.WithUnit("ms"))
	jevInputTokens, _ = meter.Int64Counter("memjev.jev.input_tokens")
	jevOutputTokens, _ = meter.Int64Counter("memjev.jev.output_tokens")
}

func RecordJevAdmission(ctx context.Context, disposition string, candidates int) {
	metricsOnce.Do(initializeMetrics)
	options := metric.WithAttributes(attribute.String("disposition", disposition))
	jevAdmissions.Add(ctx, 1, options)
	jevAdmittedCandidates.Record(ctx, int64(candidates), options)
}

func RecordJevJudgment(ctx context.Context, facts JevJudgmentMetrics) {
	metricsOnce.Do(initializeMetrics)
	options := metric.WithAttributes(
		attribute.String("disposition", facts.Disposition),
		attribute.Bool("provider.called", facts.ProviderCalled),
	)
	jevJudgments.Add(ctx, 1, options)
	jevLatency.Record(ctx, float64(facts.Latency.Microseconds())/1000, options)
	if facts.InputTokens > 0 {
		jevInputTokens.Add(ctx, facts.InputTokens, options)
	}
	if facts.OutputTokens > 0 {
		jevOutputTokens.Add(ctx, facts.OutputTokens, options)
	}
}

func RecordMaintenanceQueue(ctx context.Context, kind string, pending int64, oldest time.Duration) {
	metricsOnce.Do(initializeMetrics)
	options := metric.WithAttributes(attribute.String("job.kind", kind))
	maintenanceBacklog.Record(ctx, pending, options)
	maintenanceOldestPending.Record(ctx, oldest.Seconds(), options)
}

func RecordMaintenanceFailure(ctx context.Context, kind string, deadLetter bool) {
	metricsOnce.Do(initializeMetrics)
	options := metric.WithAttributes(attribute.String("job.kind", kind))
	maintenanceFailures.Add(ctx, 1, options)
	if deadLetter {
		maintenanceDeadLetters.Add(ctx, 1, options)
	}
}

func RecordLifecycleTransition(ctx context.Context, prior, next, reason string) {
	metricsOnce.Do(initializeMetrics)
	lifecycleTransitions.Add(ctx, 1, metric.WithAttributes(attribute.String("prior.state", prior), attribute.String("next.state", next), attribute.String("reason.code", reason)))
}

func RecordExperimentBudgetRejection(ctx context.Context, scope string) {
	metricsOnce.Do(initializeMetrics)
	experimentBudgetRejections.Add(ctx, 1, metric.WithAttributes(attribute.String("scope", scope)))
}

func RecordRetrieval(ctx context.Context, facts RetrievalMetrics) {
	metricsOnce.Do(initializeMetrics)
	options := metric.WithAttributes(attribute.String("result.code", facts.ResultCode), attribute.String("disposition", facts.Disposition))
	retrievalRequests.Add(ctx, 1, options)
	retrievalLatency.Record(ctx, float64(facts.Latency.Microseconds())/1000, options)
	retrievalCandidates.Record(ctx, int64(facts.Candidates), options)
	if facts.Replayed {
		retrievalReplays.Add(ctx, 1, options)
	}
}

func RecordRetrievalChannel(ctx context.Context, channel, result string, latency time.Duration) {
	metricsOnce.Do(initializeMetrics)
	options := metric.WithAttributes(attribute.String("channel", channel), attribute.String("result.code", result))
	retrievalChannels.Add(ctx, 1, options)
	retrievalChannelLatency.Record(ctx, float64(latency.Microseconds())/1000, options)
}

func RecordRetrievalGateRejection(ctx context.Context, code string) {
	metricsOnce.Do(initializeMetrics)
	retrievalGateRejections.Add(ctx, 1, metric.WithAttributes(attribute.String("reason.code", code)))
}

func RecordRetrievalPersistenceFailure(ctx context.Context, code string) {
	metricsOnce.Do(initializeMetrics)
	retrievalPersistenceFailure.Add(ctx, 1, metric.WithAttributes(attribute.String("reason.code", code)))
}

func RecordRetrievalReplayMismatch(ctx context.Context) {
	metricsOnce.Do(initializeMetrics)
	retrievalReplayMismatch.Add(ctx, 1)
}

func RecordRetrievalSnapshot(ctx context.Context, now, projectionCreatedAt, vectorIndexCreatedAt time.Time, rankerManifestID string) {
	metricsOnce.Do(initializeMetrics)
	options := metric.WithAttributes(attribute.String("ranker.manifest_id", rankerManifestID))
	retrievalRankerRequests.Add(ctx, 1, options)
	if !projectionCreatedAt.IsZero() && !now.Before(projectionCreatedAt) {
		retrievalSnapshotAge.Record(ctx, now.Sub(projectionCreatedAt).Seconds(), options)
	}
	if !vectorIndexCreatedAt.IsZero() && !now.Before(vectorIndexCreatedAt) {
		retrievalVectorStaleness.Record(ctx, now.Sub(vectorIndexCreatedAt).Seconds(), options)
	}
}

func RecordArchiveCorruption(ctx context.Context, code string) {
	metricsOnce.Do(initializeMetrics)
	archiveCorruptions.Add(ctx, 1, metric.WithAttributes(attribute.String("reason.code", code)))
}

func RecordSynthesisAbstention(ctx context.Context, code string) {
	metricsOnce.Do(initializeMetrics)
	synthesisAbstentions.Add(ctx, 1, metric.WithAttributes(attribute.String("reason.code", code)))
}

func RecordOutboxLeaseAge(ctx context.Context, age time.Duration) {
	metricsOnce.Do(initializeMetrics)
	outboxLeaseAge.Record(ctx, age.Seconds())
}

func RecordOutboxRetry(ctx context.Context, code string, deadLetter bool) {
	metricsOnce.Do(initializeMetrics)
	options := metric.WithAttributes(attribute.String("reason.code", code))
	if deadLetter {
		outboxDeadLetters.Add(ctx, 1, options)
		return
	}
	outboxRetries.Add(ctx, 1, options)
}

func RecordProjectionLag(ctx context.Context, lag time.Duration) {
	metricsOnce.Do(initializeMetrics)
	projectionLag.Record(ctx, lag.Seconds())
}

func RecordRebuildMismatch(ctx context.Context, component string) {
	metricsOnce.Do(initializeMetrics)
	projectionRebuildMismatches.Add(ctx, 1, metric.WithAttributes(attribute.String("component", component)))
}

func RecordOutcome(ctx context.Context, facts OutcomeMetrics) {
	metricsOnce.Do(initializeMetrics)
	options := metric.WithAttributes(attribute.String("result.code", facts.ResultCode))
	outcomeRequests.Add(ctx, 1, options)
	outcomeLatency.Record(ctx, float64(facts.Latency.Microseconds())/1000, options)
	outcomeEvidence.Record(ctx, int64(facts.Evidence), options)
	if facts.Conflict {
		outcomeConflicts.Add(ctx, 1)
	}
}

func RecordHTTPRequest(ctx context.Context, facts HTTPMetrics) {
	metricsOnce.Do(initializeMetrics)
	options := metric.WithAttributes(attribute.String("result.code", facts.ResultCode))
	httpRequests.Add(ctx, 1, options)
	httpLatency.Record(ctx, float64(facts.Latency.Microseconds())/1000, options)
	httpBytes.Record(ctx, int64(facts.Bytes), options)
}

func RecordIngest(ctx context.Context, facts IngestMetrics) {
	metricsOnce.Do(initializeMetrics)
	options := metric.WithAttributes(attribute.String("result.code", facts.ResultCode))
	requests.Add(ctx, 1, options)
	latency.Record(ctx, float64(facts.Latency.Microseconds())/1000, options)
	events.Record(ctx, int64(facts.Events), options)
	requestBytes.Record(ctx, int64(facts.Bytes), options)
	if facts.ArchiveReuse {
		archiveReuse.Add(ctx, 1)
	}
}

func RecordTransactionRetry(ctx context.Context) {
	metricsOnce.Do(initializeMetrics)
	transactionRetries.Add(ctx, 1)
}

func RecordOutboxCreated(ctx context.Context) {
	metricsOnce.Do(initializeMetrics)
	outboxCreated.Add(ctx, 1)
}
