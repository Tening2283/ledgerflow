// Package config loads runtime configuration from environment variables
// (12-factor style: the same binary runs in dev, CI and prod).
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr        string
	WorkerAddr      string // metrics/health address of the worker
	DatabaseURL     string
	RedisURL        string
	AdminToken      string
	MigrateOnStart  bool
	LogLevel        string
	ShutdownTimeout time.Duration

	RateLimitRPS   float64
	RateLimitBurst int

	WorkerBatchSize    int
	WorkerConcurrency  int
	WorkerPollInterval time.Duration
	WorkerLease        time.Duration
	WebhookTimeout     time.Duration
	WebhookMaxAttempts int
	IdempotencyTTL     time.Duration

	// Optional demo client created at startup (dev / docker-compose only).
	BootstrapAPIKey        string
	BootstrapWebhookURL    string
	BootstrapWebhookSecret string
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:               env("HTTP_ADDR", ":8080"),
		WorkerAddr:             env("WORKER_ADDR", ":9091"),
		DatabaseURL:            env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/ledgerflow?sslmode=disable"),
		RedisURL:               env("REDIS_URL", "redis://localhost:6379/0"),
		AdminToken:             os.Getenv("ADMIN_TOKEN"),
		LogLevel:               env("LOG_LEVEL", "info"),
		BootstrapAPIKey:        os.Getenv("BOOTSTRAP_API_KEY"),
		BootstrapWebhookURL:    os.Getenv("BOOTSTRAP_WEBHOOK_URL"),
		BootstrapWebhookSecret: os.Getenv("BOOTSTRAP_WEBHOOK_SECRET"),
	}
	p := parser{}
	c.MigrateOnStart = p.bool("MIGRATE_ON_START", true)
	c.ShutdownTimeout = p.duration("SHUTDOWN_TIMEOUT", 15*time.Second)
	c.RateLimitRPS = p.float("RATE_LIMIT_RPS", 50)
	c.RateLimitBurst = p.int("RATE_LIMIT_BURST", 100)
	c.WorkerBatchSize = p.int("WORKER_BATCH_SIZE", 50)
	c.WorkerConcurrency = p.int("WORKER_CONCURRENCY", 8)
	c.WorkerPollInterval = p.duration("WORKER_POLL_INTERVAL", 500*time.Millisecond)
	c.WorkerLease = p.duration("WORKER_LEASE", 30*time.Second)
	c.WebhookTimeout = p.duration("WEBHOOK_TIMEOUT", 5*time.Second)
	c.WebhookMaxAttempts = p.int("WEBHOOK_MAX_ATTEMPTS", 8)
	c.IdempotencyTTL = p.duration("IDEMPOTENCY_TTL", 24*time.Hour)
	return c, p.err
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// parser collects the first parse error so Load stays readable.
type parser struct{ err error }

func (p *parser) fail(key, v string, e error) {
	if p.err == nil {
		p.err = fmt.Errorf("config: invalid %s=%q: %w", key, v, e)
	}
}

func (p *parser) int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, e := strconv.Atoi(v)
	if e != nil {
		p.fail(key, v, e)
		return def
	}
	return n
}

func (p *parser) float(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, e := strconv.ParseFloat(v, 64)
	if e != nil {
		p.fail(key, v, e)
		return def
	}
	return n
}

func (p *parser) bool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, e := strconv.ParseBool(v)
	if e != nil {
		p.fail(key, v, e)
		return def
	}
	return b
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, e := time.ParseDuration(v)
	if e != nil {
		p.fail(key, v, e)
		return def
	}
	return d
}
