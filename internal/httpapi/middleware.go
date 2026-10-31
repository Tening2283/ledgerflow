package httpapi

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"ledgerflow/internal/ledger"
	"ledgerflow/internal/observability"
)

type ctxKey int

const (
	ctxRequestID ctxKey = iota
	ctxLogger
	ctxClient
)

func requestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxRequestID).(string)
	return id
}

func clientFrom(ctx context.Context) ledger.Client {
	c, _ := ctx.Value(ctxClient).(ledger.Client)
	return c
}

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// withRequestID reuses the caller's X-Request-Id (so one id follows a request across services)
// or generates one. Every log line of the request carries it: grep one id, see the whole story.
func withRequestID(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if !validRequestID.MatchString(id) {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-Id", id)
		ctx := context.WithValue(r.Context(), ctxRequestID, id)
		ctx = context.WithValue(ctx, ctxLogger, log.With("request_id", id))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// withObservability logs every request and records Prometheus metrics.
// The route label uses the mux pattern ("GET /v1/accounts/{id}"), NOT the raw path:
// raw paths contain ids and would create millions of metric series (cardinality explosion).
func withObservability(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		elapsed := time.Since(start)
		observability.HTTPRequests.WithLabelValues(route, r.Method, strconv.Itoa(rec.status)).Inc()
		observability.HTTPDuration.WithLabelValues(route, r.Method).Observe(elapsed.Seconds())
		if route != "GET /metrics" && route != "GET /healthz" && route != "GET /readyz" {
			logger(r.Context()).Info("http request",
				"method", r.Method, "route", route, "path", r.URL.Path,
				"status", rec.status, "duration_ms", float64(elapsed.Microseconds())/1000)
		}
	})
}

// withRecover turns a panic into a 500 instead of killing the connection (or the process).
func withRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				logger(r.Context()).Error("panic", "value", v, "stack", string(debug.Stack()))
				writeErrorCode(w, r, http.StatusInternalServerError, "internal_error", "an internal error occurred")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if v, ok := strings.CutPrefix(h, "Bearer "); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

// authed authenticates the API key, then applies the per-client rate limit.
func (s *Server) authed(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := bearer(r)
		if key == "" {
			writeErrorCode(w, r, http.StatusUnauthorized, "unauthorized", "missing Authorization: Bearer <api key>")
			return
		}
		c, err := s.svc.ClientByAPIKey(r.Context(), key)
		if errors.Is(err, ledger.ErrNotFound) {
			writeErrorCode(w, r, http.StatusUnauthorized, "unauthorized", "invalid api key")
			return
		}
		if err != nil {
			writeError(w, r, err)
			return
		}

		res, err := s.limiter.Allow(r.Context(), c.ID.String())
		switch {
		case err != nil:
			// Fail-open: if Redis is down we prefer serving traffic over a full outage.
			// (A bank-grade system might choose fail-closed; it is a business decision.)
			observability.RateLimited.WithLabelValues("error").Inc()
			logger(r.Context()).Warn("rate limiter unavailable, failing open", "err", err)
		case !res.Allowed:
			observability.RateLimited.WithLabelValues("rejected").Inc()
			secs := int(res.RetryAfter.Seconds() + 0.999)
			w.Header().Set("Retry-After", strconv.Itoa(max(1, secs)))
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(res.Limit))
			w.Header().Set("X-RateLimit-Remaining", "0")
			writeErrorCode(w, r, http.StatusTooManyRequests, "rate_limited", "too many requests")
			return
		default:
			observability.RateLimited.WithLabelValues("allowed").Inc()
			if res.Remaining >= 0 {
				w.Header().Set("X-RateLimit-Limit", strconv.Itoa(res.Limit))
				w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
			}
		}

		ctx := context.WithValue(r.Context(), ctxClient, c)
		ctx = context.WithValue(ctx, ctxLogger, logger(ctx).With("client_id", c.ID))
		h(w, r.WithContext(ctx))
	}
}

func (s *Server) admin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Constant-time comparison: avoids leaking the token through timing differences.
		if s.adminToken == "" || subtle.ConstantTimeCompare([]byte(bearer(r)), []byte(s.adminToken)) != 1 {
			writeErrorCode(w, r, http.StatusUnauthorized, "unauthorized", "admin token required")
			return
		}
		h(w, r)
	}
}
