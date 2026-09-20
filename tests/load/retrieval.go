package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
)

type report struct {
	Requests, Successes, Failures   int64
	Selected, Abstained, Explained  int64
	OtherTenant                     int64
	ValidationFailures, MaxInFlight int64
	DurationSeconds                 float64
	AchievedRPS                     float64
	P50Millis, P95Millis, P99Millis float64
}

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:18080", "Connect API endpoint")
	token := flag.String("token", os.Getenv("MEMJEV_E2E_TOKEN"), "retrieval bearer token")
	otherToken := flag.String("other-token", os.Getenv("MEMJEV_E2E_OTHER_TOKEN"), "second-tenant retrieval bearer token")
	rate := flag.Int("rps", 100, "target requests per second")
	duration := flag.Duration("duration", 60*time.Second, "test duration")
	concurrency := flag.Int("concurrency", 64, "maximum in-flight requests")
	requireVector := flag.Bool("require-vector", false, "require the vector channel to execute successfully on every retrieval path")
	flag.Parse()
	if *token == "" || *otherToken == "" || *rate <= 0 || *duration <= 0 || *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "two tenant tokens, positive rps, duration, and concurrency are required")
		os.Exit(2)
	}
	client := memjevv1connect.NewRetrievalServiceClient(http.DefaultClient, *endpoint)
	warmup := connect.NewRequest(&memjevv1.RetrieveRequest{Task: "write output", Tools: []*memjevv1.AvailableTool{{Name: "writer", ContractVersionId: "tcv_retrieval_e2e"}}, Harness: &memjevv1.HarnessIdentity{Name: "e2e", Version: "1"}, RiskClass: memjevv1.RiskClass_RISK_CLASS_LOW, LatencyClass: memjevv1.LatencyClass_LATENCY_CLASS_INTERACTIVE, MaxCandidates: 10})
	warmup.Header().Set("Authorization", "Bearer "+*token)
	warmup.Header().Set("Idempotency-Key", fmt.Sprintf("load-warmup-%d", time.Now().UnixNano()))
	warmupResponse, err := client.Retrieve(context.Background(), warmup)
	if err != nil || warmupResponse.Msg.GetDisposition() != memjevv1.RetrievalDisposition_RETRIEVAL_DISPOSITION_SELECTED || warmupResponse.Msg.GetRetrievalRunId() == "" {
		fmt.Fprintf(os.Stderr, "warmup retrieval failed: response=%v error=%v\n", warmupResponse, err)
		os.Exit(1)
	}
	if *requireVector && !healthyVectorProvenance(warmupResponse.Msg.GetProvenance()) {
		fmt.Fprintf(os.Stderr, "warmup retrieval did not execute a healthy vector channel: provenance=%v\n", warmupResponse.Msg.GetProvenance())
		os.Exit(1)
	}
	warmupRunID := warmupResponse.Msg.GetRetrievalRunId()
	warmupEpoch := warmupResponse.Msg.GetProvenance().GetProjectionEpoch()
	otherWarmup := loadRetrieveRequest(*otherToken, -1, time.Now(), nil)
	otherWarmupResponse, err := client.Retrieve(context.Background(), otherWarmup)
	if err != nil || otherWarmupResponse.Msg.GetDisposition() != memjevv1.RetrievalDisposition_RETRIEVAL_DISPOSITION_SELECTED || len(otherWarmupResponse.Msg.GetCandidates()) != 1 || otherWarmupResponse.Msg.GetCandidates()[0].GetProcedureVersionId() != "pv_retrieval_other" {
		fmt.Fprintf(os.Stderr, "second-tenant warmup failed: response=%v error=%v\n", otherWarmupResponse, err)
		os.Exit(1)
	}
	if *requireVector && !healthyVectorProvenance(otherWarmupResponse.Msg.GetProvenance()) {
		fmt.Fprintf(os.Stderr, "second-tenant warmup did not execute a healthy vector channel: provenance=%v\n", otherWarmupResponse.Msg.GetProvenance())
		os.Exit(1)
	}
	otherEpoch := otherWarmupResponse.Msg.GetProvenance().GetProjectionEpoch()
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), *duration+30*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()
	deadline := time.NewTimer(*duration)
	defer deadline.Stop()
	semaphore := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	var requests, successes, failures, selected, abstained, explained, otherTenant, validationFailures atomic.Int64
	var inFlight, maxInFlight atomic.Int64
	var latencyMu sync.Mutex
	latencies := make([]time.Duration, 0, *rate*int(duration.Seconds()))
	loop := true
	for loop {
		select {
		case <-deadline.C:
			loop = false
		case at := <-ticker.C:
			sequence := requests.Add(1)
			wg.Add(1)
			go func(id int64, scheduled time.Time) {
				defer wg.Done()
				semaphore <- struct{}{}
				defer func() { <-semaphore }()
				current := inFlight.Add(1)
				defer inFlight.Add(-1)
				for observed := maxInFlight.Load(); current > observed && !maxInFlight.CompareAndSwap(observed, current); observed = maxInFlight.Load() {
				}
				valid := false
				var err error
				if id%20 == 19 {
					request := loadRetrieveRequest(*otherToken, id, scheduled, nil)
					var response *connect.Response[memjevv1.RetrieveResponse]
					response, err = client.Retrieve(ctx, request)
					valid = err == nil && response.Msg.GetDisposition() == memjevv1.RetrievalDisposition_RETRIEVAL_DISPOSITION_SELECTED && len(response.Msg.GetCandidates()) == 1 && response.Msg.GetCandidates()[0].GetProcedureVersionId() == "pv_retrieval_other" && response.Msg.GetProvenance().GetProjectionEpoch() == otherEpoch && (!*requireVector || healthyVectorProvenance(response.Msg.GetProvenance()))
					if valid {
						otherTenant.Add(1)
					}
				} else {
					switch id % 10 {
					case 0, 1:
						request := connect.NewRequest(&memjevv1.ExplainRetrievalRequest{RetrievalRunId: warmupRunID})
						request.Header().Set("Authorization", "Bearer "+*token)
						var response *connect.Response[memjevv1.ExplainRetrievalResponse]
						response, err = client.ExplainRetrieval(ctx, request)
						valid = err == nil && response.Msg.GetProvenance().GetProjectionEpoch() == warmupEpoch && len(response.Msg.GetCandidates()) > 0 && (!*requireVector || healthyVectorProvenance(response.Msg.GetProvenance()))
						if valid {
							explained.Add(1)
						}
					case 2:
						request := loadRetrieveRequest(*token, id, scheduled, []string{"filesystem.write"})
						var response *connect.Response[memjevv1.RetrieveResponse]
						response, err = client.Retrieve(ctx, request)
						valid = err == nil && response.Msg.GetDisposition() == memjevv1.RetrievalDisposition_RETRIEVAL_DISPOSITION_ABSTAINED && response.Msg.GetAbstentionCode() == "no_eligible_candidates" && response.Msg.GetProvenance().GetProjectionEpoch() == warmupEpoch && (!*requireVector || healthyVectorProvenance(response.Msg.GetProvenance()))
						if valid {
							abstained.Add(1)
						}
					default:
						request := loadRetrieveRequest(*token, id, scheduled, nil)
						var response *connect.Response[memjevv1.RetrieveResponse]
						response, err = client.Retrieve(ctx, request)
						valid = err == nil && response.Msg.GetDisposition() == memjevv1.RetrievalDisposition_RETRIEVAL_DISPOSITION_SELECTED && len(response.Msg.GetCandidates()) >= 1 && response.Msg.GetCandidates()[0].GetProcedureVersionId() == "pv_retrieval_e2e" && response.Msg.GetProvenance().GetProjectionEpoch() == warmupEpoch && (!*requireVector || healthyVectorProvenance(response.Msg.GetProvenance()))
						if valid {
							selected.Add(1)
						}
					}
				}
				latencyMu.Lock()
				latencies = append(latencies, time.Since(scheduled))
				latencyMu.Unlock()
				if err != nil || !valid {
					failures.Add(1)
					if err == nil {
						validationFailures.Add(1)
					}
				} else {
					successes.Add(1)
				}
			}(sequence, at)
		}
	}
	wg.Wait()
	elapsed := time.Since(started)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	result := report{Requests: requests.Load(), Successes: successes.Load(), Failures: failures.Load(), Selected: selected.Load(), Abstained: abstained.Load(), Explained: explained.Load(), OtherTenant: otherTenant.Load(), ValidationFailures: validationFailures.Load(), MaxInFlight: maxInFlight.Load(), DurationSeconds: elapsed.Seconds(), AchievedRPS: float64(successes.Load()) / elapsed.Seconds(), P50Millis: percentile(latencies, 0.50), P95Millis: percentile(latencies, 0.95), P99Millis: percentile(latencies, 0.99)}
	_ = json.NewEncoder(os.Stdout).Encode(result)
	if result.Failures > 0 || result.OtherTenant == 0 || result.P95Millis > 250 || result.AchievedRPS < float64(*rate)*0.95 {
		os.Exit(1)
	}
}

func healthyVectorProvenance(provenance *memjevv1.RetrievalProvenance) bool {
	if provenance == nil || len(provenance.GetIndexManifestIds()) != 5 {
		return false
	}
	for _, degraded := range provenance.GetDegradedChannels() {
		if degraded == "vector" || len(degraded) > len("vector:") && degraded[:len("vector:")] == "vector:" {
			return false
		}
	}
	return true
}

func loadRetrieveRequest(token string, id int64, scheduled time.Time, forbidden []string) *connect.Request[memjevv1.RetrieveRequest] {
	request := connect.NewRequest(&memjevv1.RetrieveRequest{Task: "write output", Tools: []*memjevv1.AvailableTool{{Name: "writer", ContractVersionId: "tcv_retrieval_e2e"}}, Harness: &memjevv1.HarnessIdentity{Name: "e2e", Version: "1"}, ForbiddenEffects: forbidden, RiskClass: memjevv1.RiskClass_RISK_CLASS_LOW, LatencyClass: memjevv1.LatencyClass_LATENCY_CLASS_INTERACTIVE, MaxCandidates: 10})
	request.Header().Set("Authorization", "Bearer "+token)
	request.Header().Set("Idempotency-Key", fmt.Sprintf("load-%020d-%d", id, scheduled.UnixNano()))
	return request
}

func percentile(values []time.Duration, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * quantile)
	return float64(values[index].Microseconds()) / 1000
}
