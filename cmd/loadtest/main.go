// Command loadtest hammers a running API with concurrent transfers, reports throughput and
// latency percentiles, then checks that no money was created or destroyed.
//
//	go run ./cmd/loadtest -url http://localhost:8080 -key lf_demo_key -accounts 20 -workers 32 -duration 15s
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	baseURL  = flag.String("url", "http://localhost:8080", "API base URL")
	apiKey   = flag.String("key", "lf_demo_key", "API key")
	nAccts   = flag.Int("accounts", 20, "number of accounts (fewer = more lock contention)")
	workers  = flag.Int("workers", 32, "concurrent clients")
	duration = flag.Duration("duration", 10*time.Second, "test duration")
	initial  = flag.Int64("initial", 1_000_000, "initial balance per account")
)

var client = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 256}}

func call(method, path string, body any, idem string, out any) (int, error) {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, *baseURL+path, &buf)
	req.Header.Set("Authorization", "Bearer "+*apiKey)
	req.Header.Set("Content-Type", "application/json")
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	res, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if out != nil {
		_ = json.NewDecoder(res.Body).Decode(out)
	}
	return res.StatusCode, nil
}

type account struct {
	ID      string `json:"id"`
	Balance int64  `json:"balance"`
}

func main() {
	flag.Parse()
	run := uuid.NewString()[:8]
	fmt.Printf("setting up %d accounts...\n", *nAccts)
	accts := make([]account, *nAccts)
	for i := range accts {
		if code, err := call("POST", "/v1/accounts", map[string]string{"owner_name": fmt.Sprintf("load-%s-%d", run, i), "currency": "XOF"}, "", &accts[i]); err != nil || code != 201 {
			fmt.Println("create account failed:", code, err)
			os.Exit(1)
		}
		if code, err := call("POST", "/v1/deposits", map[string]any{"account_id": accts[i].ID, "amount": *initial}, uuid.NewString(), nil); err != nil || code != 201 {
			fmt.Println("deposit failed:", code, err)
			os.Exit(1)
		}
	}

	var mu sync.Mutex
	var latencies []time.Duration
	codes := map[int]int{}
	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	fmt.Printf("running %d workers for %s...\n", *workers, *duration)
	start := time.Now()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				from, to := rand.IntN(*nAccts), rand.IntN(*nAccts)
				if from == to {
					continue
				}
				t0 := time.Now()
				code, err := call("POST", "/v1/transfers", map[string]any{
					"from_account_id": accts[from].ID, "to_account_id": accts[to].ID, "amount": 1 + rand.IntN(5000),
				}, uuid.NewString(), nil)
				d := time.Since(t0)
				mu.Lock()
				if err != nil {
					codes[-1]++
				} else {
					codes[code]++
					latencies = append(latencies, d)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	slices.Sort(latencies)
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*p)]
	}
	fmt.Printf("\nrequests: %d in %s → %.0f req/s\n", len(latencies), elapsed.Round(time.Millisecond), float64(len(latencies))/elapsed.Seconds())
	fmt.Printf("latency  p50=%s  p95=%s  p99=%s  max=%s\n", pct(.5), pct(.95), pct(.99), pct(1))
	fmt.Printf("status codes: %v  (201=ok, 422=insufficient funds, 429=rate limited, -1=network error)\n", codes)

	var total int64
	for _, a := range accts {
		var got account
		if code, err := call("GET", "/v1/accounts/"+a.ID, nil, "", &got); err != nil || code != 200 {
			fmt.Printf("cannot verify the invariant: API unreachable (code=%d err=%v)\n", code, err)
			os.Exit(2)
		}
		total += got.Balance
	}
	want := *initial * int64(*nAccts)
	if total != want {
		fmt.Printf("INVARIANT BROKEN: total=%d want=%d\n", total, want)
		os.Exit(1)
	}
	fmt.Printf("invariant OK: total balance still %d across %d accounts\n", total, *nAccts)
}
