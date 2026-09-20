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
	DurationSeconds                 float64
	AchievedRPS                     float64
	P50Millis, P95Millis, P99Millis float64
}

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:18080", "Connect API endpoint")
	token := flag.String("token", os.Getenv("MEMJEV_E2E_TOKEN"), "retrieval bearer token")
	rate := flag.Int("rps", 100, "target requests per second")
	duration := flag.Duration("duration", 60*time.Second, "test duration")
	concurrency := flag.Int("concurrency", 64, "maximum in-flight requests")
	flag.Parse()
	if *token == "" || *rate <= 0 || *duration <= 0 || *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "token, positive rps, duration, and concurrency are required")
		os.Exit(2)
	}
	client := memjevv1connect.NewRetrievalServiceClient(http.DefaultClient, *endpoint)
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), *duration+30*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()
	deadline := time.NewTimer(*duration)
	defer deadline.Stop()
	semaphore := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	var requests, successes, failures atomic.Int64
	var latencyMu sync.Mutex
	latencies := make([]time.Duration, 0, *rate*int(duration.Seconds()))
	loop := true
	for loop {
		select {
		case <-deadline.C:
			loop = false
		case at := <-ticker.C:
			sequence := requests.Add(1)
			semaphore <- struct{}{}
			wg.Add(1)
			go func(id int64, scheduled time.Time) {
				defer wg.Done()
				defer func() { <-semaphore }()
				request := connect.NewRequest(&memjevv1.RetrieveRequest{Task: "write output", Tools: []*memjevv1.AvailableTool{{Name: "writer", ContractVersionId: "tcv_retrieval_e2e"}}, Harness: &memjevv1.HarnessIdentity{Name: "e2e", Version: "1"}, RiskClass: memjevv1.RiskClass_RISK_CLASS_LOW, LatencyClass: memjevv1.LatencyClass_LATENCY_CLASS_INTERACTIVE, MaxCandidates: 10})
				request.Header().Set("Authorization", "Bearer "+*token)
				request.Header().Set("Idempotency-Key", fmt.Sprintf("load-%020d-%d", id, scheduled.UnixNano()))
				callStarted := time.Now()
				_, err := client.Retrieve(ctx, request)
				latencyMu.Lock()
				latencies = append(latencies, time.Since(callStarted))
				latencyMu.Unlock()
				if err != nil {
					failures.Add(1)
				} else {
					successes.Add(1)
				}
			}(sequence, at)
		}
	}
	wg.Wait()
	elapsed := time.Since(started)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	result := report{Requests: requests.Load(), Successes: successes.Load(), Failures: failures.Load(), DurationSeconds: elapsed.Seconds(), AchievedRPS: float64(requests.Load()) / elapsed.Seconds(), P50Millis: percentile(latencies, 0.50), P95Millis: percentile(latencies, 0.95), P99Millis: percentile(latencies, 0.99)}
	_ = json.NewEncoder(os.Stdout).Encode(result)
	if result.Failures > 0 || result.P95Millis > 250 || result.AchievedRPS < float64(*rate)*0.95 {
		os.Exit(1)
	}
}

func percentile(values []time.Duration, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * quantile)
	return float64(values[index].Microseconds()) / 1000
}
