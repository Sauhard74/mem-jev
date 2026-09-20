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

var (
	metricsOnce        sync.Once
	requests           metric.Int64Counter
	latency            metric.Float64Histogram
	events             metric.Int64Histogram
	requestBytes       metric.Int64Histogram
	archiveReuse       metric.Int64Counter
	transactionRetries metric.Int64Counter
	outboxCreated      metric.Int64Counter
	httpRequests       metric.Int64Counter
	httpLatency        metric.Float64Histogram
	httpBytes          metric.Int64Histogram
	outcomeRequests    metric.Int64Counter
	outcomeLatency     metric.Float64Histogram
	outcomeEvidence    metric.Int64Histogram
	outcomeConflicts   metric.Int64Counter
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
