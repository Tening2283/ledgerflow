// Command webhook-receiver is a demo "merchant" endpoint. It shows what a client must do:
// verify the signature, deduplicate by event id (delivery is at-least-once), answer 2xx fast.
//
// Set FAIL_RATE=0.5 to make it fail half the time and watch the worker's retries/backoff.
package main

import (
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"ledgerflow/internal/webhook"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "webhook-receiver")
	secret := os.Getenv("WEBHOOK_SECRET")
	failRate, _ := strconv.ParseFloat(os.Getenv("FAIL_RATE"), 64)
	addr := os.Getenv("RECEIVER_ADDR")
	if addr == "" {
		addr = ":8081"
	}

	var mu sync.Mutex
	seen := map[string]bool{} // in production: a DB table with a unique constraint on event_id

	http.HandleFunc("POST /webhooks", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if secret != "" {
			if err := webhook.Verify(secret, r.Header.Get(webhook.HeaderSignature), body, 5*time.Minute, time.Now()); err != nil {
				log.Warn("rejected webhook", "err", err)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		if rand.Float64() < failRate {
			log.Info("simulating failure")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		id := r.Header.Get(webhook.HeaderEventID)
		mu.Lock()
		dup := seen[id]
		seen[id] = true
		mu.Unlock()
		log.Info("webhook received", "event_id", id, "type", r.Header.Get(webhook.HeaderEventType),
			"duplicate", dup, "signature_checked", secret != "", "body", string(body))
		w.WriteHeader(http.StatusNoContent)
	})

	log.Info("listening", "addr", addr)
	srv := &http.Server{Addr: addr, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		log.Error("server", "err", err)
		os.Exit(1)
	}
}
