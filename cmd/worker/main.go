// Command worker delivers outbox events as signed webhooks. Run as many replicas as you like:
// FOR UPDATE SKIP LOCKED makes them share the work without coordination.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"ledgerflow/internal/config"
	"ledgerflow/internal/db"
	"ledgerflow/internal/observability"
	"ledgerflow/internal/webhook"
)

func main() {
	cfg, err := config.Load()
	log := observability.NewLogger(cfg.LogLevel, "worker")
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	if cfg.MigrateOnStart {
		if err := db.Migrate(ctx, pool, log); err != nil {
			log.Error("migrate", "err", err)
			os.Exit(1)
		}
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	ops := &http.Server{Addr: cfg.WorkerAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := ops.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("ops server", "err", err)
		}
	}()

	d := webhook.NewDispatcher(pool, webhook.Options{
		BatchSize:    cfg.WorkerBatchSize,
		Concurrency:  cfg.WorkerConcurrency,
		PollInterval: cfg.WorkerPollInterval,
		Lease:        cfg.WorkerLease,
		MaxAttempts:  cfg.WebhookMaxAttempts,
		Timeout:      cfg.WebhookTimeout,
	}, log)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); d.Run(ctx) }()
	go func() { defer wg.Done(); webhook.Janitor(ctx, pool, cfg.IdempotencyTTL, time.Minute, log) }()
	log.Info("worker started", "batch", cfg.WorkerBatchSize, "concurrency", cfg.WorkerConcurrency)

	<-ctx.Done()
	log.Info("shutting down: finishing in-flight deliveries")
	wg.Wait() // the current batch completes; unfinished leases simply expire and are retried
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = ops.Shutdown(shutdownCtx)
	log.Info("bye")
}
