// Command api serves the Ledgerflow HTTP API and the dashboard.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"ledgerflow/internal/config"
	"ledgerflow/internal/db"
	"ledgerflow/internal/httpapi"
	"ledgerflow/internal/ledger"
	"ledgerflow/internal/observability"
	"ledgerflow/internal/ratelimit"
	"ledgerflow/web"
)

func main() {
	cfg, err := config.Load()
	log := observability.NewLogger(cfg.LogLevel, "api")
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	// ctx is cancelled on SIGINT (Ctrl+C) or SIGTERM (what Docker/Kubernetes send to stop a container).
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

	ropt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Error("redis url", "err", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(ropt)
	defer rdb.Close()

	svc := ledger.NewService(pool)

	if cfg.BootstrapAPIKey != "" {
		if err := svc.EnsureClient(ctx, "demo", cfg.BootstrapAPIKey, cfg.BootstrapWebhookURL, cfg.BootstrapWebhookSecret); err != nil {
			log.Error("bootstrap client", "err", err)
			os.Exit(1)
		}
		log.Info("demo client ready (BOOTSTRAP_API_KEY)")
	}

	draining := &atomic.Bool{}
	handler := httpapi.New(httpapi.Deps{
		Ledger:     svc,
		Limiter:    ratelimit.NewRedis(rdb, cfg.RateLimitRPS, cfg.RateLimitBurst),
		Logger:     log,
		AdminToken: cfg.AdminToken,
		Draining:   draining,
		Static:     web.Static(),
		Ready: func(ctx context.Context) error {
			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			if err := pool.Ping(ctx); err != nil {
				return errors.New("database: " + err.Error())
			}
			// Redis being down degrades rate limiting (fail-open) but does not make us unready.
			return nil
		},
	})

	// Explicit timeouts: Go's zero-value server has none, so slow clients
	// (or a slowloris attack) could hold connections open forever.
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("server", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
	}

	// Graceful shutdown:
	// 1. report "not ready" so the load balancer stops routing new requests here,
	// 2. stop accepting connections and wait for in-flight requests (bounded by a timeout),
	// 3. close the DB pool and Redis (deferred).
	log.Info("shutting down")
	draining.Store(true)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown", "err", err)
	}
	log.Info("bye")
}
