// Package testutil gives each test its own throw-away PostgreSQL schema,
// so integration tests can run in parallel against one real database without interfering.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"ledgerflow/internal/db"
)

func NewPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()

	b := make([]byte, 6)
	_, _ = rand.Read(b)
	schema := "test_" + hex.EncodeToString(b)

	admin, err := db.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 40
	pool, err := db.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect test pool: %v", err)
	}
	if err := db.Migrate(ctx, pool, Logger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
	})
	return pool
}

func Logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
