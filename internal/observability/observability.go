// Package observability groups logging and Prometheus metrics.
package observability

import (
	"log/slog"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// NewLogger returns a JSON logger: one event per line, machine parseable (Loki, ELK, Datadog...).
func NewLogger(level, service string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})).With("service", service)
}

var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests by route, method and status code.",
	}, []string{"route", "method", "code"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency by route.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"route", "method"})

	Transfers = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_transfers_total",
		Help: "Money movements by kind and result.",
	}, []string{"kind", "result"})

	IdempotentReplays = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ledger_idempotent_replays_total",
		Help: "Requests answered from a previously stored idempotency key.",
	})

	RateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ratelimit_decisions_total",
		Help: "Rate limiter decisions (allowed, rejected, error = fail-open).",
	}, []string{"decision"})

	WebhookDeliveries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_deliveries_total",
		Help: "Webhook delivery attempts by result.",
	}, []string{"result"})

	WebhookLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_delivery_duration_seconds",
		Help:    "Latency of webhook HTTP calls.",
		Buckets: prometheus.DefBuckets,
	})

	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_pending_events",
		Help: "Events waiting to be delivered (a growing value means the worker is falling behind).",
	})
)
