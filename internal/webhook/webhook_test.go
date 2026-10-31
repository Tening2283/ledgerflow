package webhook_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"ledgerflow/internal/ledger"
	"ledgerflow/internal/testutil"
	"ledgerflow/internal/webhook"
)

func TestSignAndVerify(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	body := []byte(`{"hello":"world"}`)
	h := webhook.Sign("secret", now, body)

	if err := webhook.Verify("secret", h, body, 5*time.Minute, now.Add(time.Minute)); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	if err := webhook.Verify("wrong", h, body, 5*time.Minute, now); err != webhook.ErrBadSignature {
		t.Fatalf("wrong secret accepted: %v", err)
	}
	if err := webhook.Verify("secret", h, []byte(`{"hello":"evil"}`), 5*time.Minute, now); err != webhook.ErrBadSignature {
		t.Fatalf("tampered body accepted: %v", err)
	}
	if err := webhook.Verify("secret", h, body, 5*time.Minute, now.Add(time.Hour)); err != webhook.ErrTooOld {
		t.Fatalf("replayed old message accepted: %v", err)
	}
	if err := webhook.Verify("secret", "garbage", body, 5*time.Minute, now); err != webhook.ErrBadSignature {
		t.Fatalf("garbage header accepted: %v", err)
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	for n := 1; n <= 30; n++ {
		d := webhook.Backoff(n)
		full := 2 * time.Second << (n - 1)
		if full > time.Hour || full <= 0 {
			full = time.Hour
		}
		if d < full/2 || d > full {
			t.Fatalf("attempt %d: %v not in [%v, %v]", n, d, full/2, full)
		}
	}
}

type env struct {
	svc    *ledger.Service
	disp   *webhook.Dispatcher
	client ledger.Client
	ctx    context.Context
}

func newEnv(t *testing.T, url string, maxAttempts int) *env {
	pool := testutil.NewPool(t)
	svc := ledger.NewService(pool)
	ctx := context.Background()
	c, _, err := svc.CreateClient(ctx, "c", url)
	if err != nil {
		t.Fatal(err)
	}
	d := webhook.NewDispatcher(pool, webhook.Options{
		BatchSize: 10, Concurrency: 4, PollInterval: 10 * time.Millisecond,
		Lease: 30 * time.Second, MaxAttempts: maxAttempts, Timeout: 2 * time.Second,
	}, testutil.Logger())
	return &env{svc: svc, disp: d, client: c, ctx: ctx}
}

func (e *env) deposit(t *testing.T) {
	a, err := e.svc.CreateAccount(e.ctx, e.client.ID, "x", "XOF")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.svc.Deposit(e.ctx, e.client.ID, a.ID, 100, "", ledger.Idempotency{Key: uuid.NewString(), RequestHash: []byte("h")}); err != nil {
		t.Fatal(err)
	}
}

func (e *env) status(t *testing.T) ledger.Event {
	evs, err := e.svc.ListEvents(e.ctx, e.client.ID, 0, 10)
	if err != nil || len(evs) != 1 {
		t.Fatalf("events: %v %d", err, len(evs))
	}
	return evs[0]
}

func TestDeliverySignedAndMarkedDelivered(t *testing.T) {
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got.Store([2]string{r.Header.Get(webhook.HeaderSignature), string(body)})
	}))
	defer srv.Close()

	e := newEnv(t, srv.URL, 5)
	e.deposit(t)
	if n, err := e.disp.RunOnce(e.ctx); err != nil || n != 1 {
		t.Fatalf("RunOnce: %d %v", n, err)
	}
	v := got.Load().([2]string)
	if err := webhook.Verify(e.client.WebhookSecret, v[0], []byte(v[1]), time.Minute, time.Now()); err != nil {
		t.Fatalf("receiver could not verify signature: %v", err)
	}
	var env webhook.Envelope
	if err := json.Unmarshal([]byte(v[1]), &env); err != nil || env.Type != ledger.EventTransferCreated {
		t.Fatalf("bad envelope: %v %s", err, v[1])
	}
	if s := e.status(t); s.Status != "delivered" || s.Attempts != 1 {
		t.Fatalf("status = %s attempts = %d", s.Status, s.Attempts)
	}
	// Nothing left to do.
	if n, _ := e.disp.RunOnce(e.ctx); n != 0 {
		t.Fatalf("delivered event claimed again")
	}
}

func TestFailureIsRetriedLaterThenDeadLettered(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	e := newEnv(t, srv.URL, 1)
	e.deposit(t)
	if _, err := e.disp.RunOnce(e.ctx); err != nil {
		t.Fatal(err)
	}
	if s := e.status(t); s.Status != "failed" {
		t.Fatalf("status = %s, want failed (dead letter after max attempts)", s.Status)
	}
}

func TestFailureSchedulesRetryInTheFuture(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	e := newEnv(t, srv.URL, 5)
	e.deposit(t)
	_, _ = e.disp.RunOnce(e.ctx)
	if s := e.status(t); s.Status != "pending" || s.Attempts != 1 {
		t.Fatalf("status=%s attempts=%d", s.Status, s.Attempts)
	}
	// The retry is scheduled with backoff (>= 1s), so an immediate second pass finds nothing due.
	if n, _ := e.disp.RunOnce(e.ctx); n != 0 {
		t.Fatalf("event retried immediately, backoff ignored")
	}
}

// Two workers running concurrently must never deliver the same event twice in the same round.
func TestConcurrentWorkersDoNotDoubleClaim(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.Header.Get(webhook.HeaderEventID)]++
		mu.Unlock()
	}))
	defer srv.Close()
	e := newEnv(t, srv.URL, 5)
	for i := 0; i < 30; i++ {
		e.deposit(t)
	}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if n, err := e.disp.RunOnce(e.ctx); err != nil || n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()
	if len(seen) != 30 {
		t.Fatalf("delivered %d distinct events, want 30", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("event %s delivered %d times", id, n)
		}
	}
}

func TestNoEndpoint(t *testing.T) {
	e := newEnv(t, "", 5)
	e.deposit(t)
	_, _ = e.disp.RunOnce(e.ctx)
	if s := e.status(t); s.Status != "no_endpoint" {
		t.Fatalf("status = %s", s.Status)
	}
}
