package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"ledgerflow/internal/observability"
)

type Options struct {
	BatchSize    int
	Concurrency  int
	PollInterval time.Duration
	Lease        time.Duration // "visibility timeout": how long a claimed event is hidden from other workers
	MaxAttempts  int
	Timeout      time.Duration
}

type Dispatcher struct {
	pool *pgxpool.Pool
	http *http.Client
	opts Options
	log  *slog.Logger
	now  func() time.Time
}

func NewDispatcher(pool *pgxpool.Pool, opts Options, log *slog.Logger) *Dispatcher {
	return &Dispatcher{
		pool: pool,
		// Never use http.DefaultClient for outbound calls: it has no timeout,
		// so one slow receiver could block a worker goroutine forever.
		http: &http.Client{Timeout: opts.Timeout},
		opts: opts,
		log:  log,
		now:  time.Now,
	}
}

type job struct {
	id         int64
	eventID    uuid.UUID
	typ        string
	payload    json.RawMessage
	attempts   int
	createdAt  time.Time
	webhookURL *string
	secret     string
}

// Envelope is the JSON body POSTed to receivers.
type Envelope struct {
	ID        uuid.UUID       `json:"id"`
	Type      string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

// Run polls until ctx is cancelled. When a batch is full there is probably more work,
// so it loops immediately instead of sleeping.
func (d *Dispatcher) Run(ctx context.Context) {
	for {
		n, err := d.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			d.log.Error("dispatch batch failed", "err", err)
		}
		if n == d.opts.BatchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(d.opts.PollInterval):
		}
	}
}

// RunOnce claims one batch of due events, delivers them concurrently and records the results.
func (d *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	jobs, err := d.claim(ctx)
	if err != nil || len(jobs) == 0 {
		return 0, err
	}

	sem := make(chan struct{}, max(1, d.opts.Concurrency)) // bounded concurrency
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(j job) {
			defer wg.Done()
			defer func() { <-sem }()
			d.handle(ctx, j)
		}(j)
	}
	wg.Wait()
	return len(jobs), nil
}

// claim uses FOR UPDATE SKIP LOCKED: N workers can run this at the same time and each gets a
// DIFFERENT set of rows, without waiting for each other. It also pushes next_attempt_at into
// the future (a lease): if this worker crashes mid-delivery, the event becomes visible again
// once the lease expires and another worker retries it. Nothing is lost.
func (d *Dispatcher) claim(ctx context.Context) ([]job, error) {
	rows, err := d.pool.Query(ctx, `
		WITH due AS (
			SELECT id FROM outbox_events
			WHERE status = 'pending' AND next_attempt_at <= now()
			ORDER BY next_attempt_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events o
		SET attempts = o.attempts + 1,
		    next_attempt_at = now() + make_interval(secs => $2)
		FROM due, api_clients c
		WHERE o.id = due.id AND c.id = o.client_id
		RETURNING o.id, o.event_id, o.type, o.payload, o.attempts, o.created_at, c.webhook_url, c.webhook_secret`,
		d.opts.BatchSize, d.opts.Lease.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (job, error) {
		var j job
		err := r.Scan(&j.id, &j.eventID, &j.typ, &j.payload, &j.attempts, &j.createdAt, &j.webhookURL, &j.secret)
		return j, err
	})
}

func (d *Dispatcher) handle(ctx context.Context, j job) {
	log := d.log.With("event_id", j.eventID, "attempt", j.attempts)
	if j.webhookURL == nil || *j.webhookURL == "" {
		d.finish(ctx, j.id, `UPDATE outbox_events SET status = 'no_endpoint' WHERE id = $1`)
		observability.WebhookDeliveries.WithLabelValues("no_endpoint").Inc()
		return
	}

	err := d.deliver(ctx, j)
	if err == nil {
		d.finish(ctx, j.id, `UPDATE outbox_events SET status = 'delivered', delivered_at = now(), last_error = NULL WHERE id = $1`)
		observability.WebhookDeliveries.WithLabelValues("delivered").Inc()
		log.Info("webhook delivered", "url", *j.webhookURL)
		return
	}

	if j.attempts >= d.opts.MaxAttempts {
		// Dead letter: we stop retrying, but the row stays for inspection / manual replay.
		d.finish(ctx, j.id, `UPDATE outbox_events SET status = 'failed', last_error = $2 WHERE id = $1`, err.Error())
		observability.WebhookDeliveries.WithLabelValues("dead").Inc()
		log.Warn("webhook dead-lettered", "err", err)
		return
	}
	delay := Backoff(j.attempts)
	d.finish(ctx, j.id, `UPDATE outbox_events SET last_error = $2, next_attempt_at = now() + make_interval(secs => $3) WHERE id = $1`,
		err.Error(), delay.Seconds())
	observability.WebhookDeliveries.WithLabelValues("retry").Inc()
	log.Warn("webhook failed, will retry", "err", err, "retry_in", delay.String())
}

func (d *Dispatcher) deliver(ctx context.Context, j job) error {
	body, err := json.Marshal(Envelope{ID: j.eventID, Type: j.typ, CreatedAt: j.createdAt, Data: j.payload})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, *j.webhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Ledgerflow-Webhooks/1.0")
	req.Header.Set(HeaderEventID, j.eventID.String())
	req.Header.Set(HeaderEventType, j.typ)
	req.Header.Set(HeaderSignature, Sign(j.secret, d.now(), body))

	start := time.Now()
	resp, err := d.http.Do(req)
	observability.WebhookLatency.Observe(time.Since(start).Seconds())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // drain so the connection can be reused
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("%s", describeStatus(resp.StatusCode))
	}
	return nil
}

func (d *Dispatcher) finish(ctx context.Context, id int64, sql string, args ...any) {
	// Use a fresh context: even during shutdown we want to record the outcome of a delivery we made.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := d.pool.Exec(ctx, sql, append([]any{id}, args...)...); err != nil {
		// If this fails, the lease expires and the event is redelivered: at-least-once, never lost.
		d.log.Error("record delivery result", "err", err, "outbox_id", id)
	}
}

// Janitor runs periodic housekeeping: purge old idempotency keys, export the backlog gauge.
func Janitor(ctx context.Context, pool *pgxpool.Pool, idemTTL time.Duration, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		tag, err := pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE created_at < now() - make_interval(secs => $1)`, idemTTL.Seconds())
		if err == nil && tag.RowsAffected() > 0 {
			log.Info("purged idempotency keys", "count", tag.RowsAffected())
		}
		var pending int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE status = 'pending'`).Scan(&pending); err == nil {
			observability.OutboxPending.Set(float64(pending))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
