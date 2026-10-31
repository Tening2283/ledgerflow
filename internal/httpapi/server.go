// Package httpapi is the HTTP transport layer: decoding, validation, auth, error mapping.
// Business rules live in package ledger; this package only translates HTTP <-> domain.
package httpapi

import (
	"context"
	"io/fs"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"ledgerflow/internal/ledger"
	"ledgerflow/internal/ratelimit"
)

type Deps struct {
	Ledger     *ledger.Service
	Limiter    ratelimit.Limiter
	Logger     *slog.Logger
	AdminToken string
	// Ready checks dependencies (DB, Redis) for /readyz.
	Ready func(ctx context.Context) error
	// Draining is set to true when shutdown starts, so the load balancer stops sending traffic.
	Draining *atomic.Bool
	Static   fs.FS
}

type Server struct {
	svc        *ledger.Service
	limiter    ratelimit.Limiter
	adminToken string
	ready      func(ctx context.Context) error
	draining   *atomic.Bool
}

func New(d Deps) http.Handler {
	if d.Limiter == nil {
		d.Limiter = ratelimit.Noop{}
	}
	if d.Draining == nil {
		d.Draining = &atomic.Bool{}
	}
	if d.Ready == nil {
		d.Ready = func(context.Context) error { return nil }
	}
	s := &Server{svc: d.Ledger, limiter: d.Limiter, adminToken: d.AdminToken, ready: d.Ready, draining: d.Draining}

	mux := http.NewServeMux()

	// Ops
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.Handler())

	// Admin
	mux.HandleFunc("POST /admin/clients", s.admin(s.createClient))

	// Public API (v1). Versioning in the path lets us ship /v2 without breaking integrations.
	mux.HandleFunc("PUT /v1/webhook", s.authed(s.putWebhook))
	mux.HandleFunc("POST /v1/accounts", s.authed(s.createAccount))
	mux.HandleFunc("GET /v1/accounts", s.authed(s.listAccounts))
	mux.HandleFunc("GET /v1/accounts/{id}", s.authed(s.getAccount))
	mux.HandleFunc("GET /v1/accounts/{id}/entries", s.authed(s.listEntries))
	mux.HandleFunc("POST /v1/deposits", s.authed(s.deposit))
	mux.HandleFunc("POST /v1/withdrawals", s.authed(s.withdraw))
	mux.HandleFunc("POST /v1/transfers", s.authed(s.transfer))
	mux.HandleFunc("GET /v1/transfers/{id}", s.authed(s.getTransfer))
	mux.HandleFunc("GET /v1/events", s.authed(s.listEvents))

	// Dashboard
	if d.Static != nil {
		mux.Handle("GET /", http.FileServerFS(d.Static))
	}

	// Order matters: recover is outermost so it catches panics everywhere.
	return withRequestID(d.Logger, withRecover(withObservability(mux)))
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	if err := s.ready(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
