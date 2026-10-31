// Package webhook delivers outbox events to client endpoints and signs them.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
)

const (
	HeaderSignature = "Ledgerflow-Signature"
	HeaderEventID   = "Ledgerflow-Event-Id"
	HeaderEventType = "Ledgerflow-Event-Type"
)

// Sign returns "t=<unix>,v1=<hex hmac>" where the HMAC covers "<unix>.<body>".
// Including the timestamp in the signed message lets receivers reject old messages (replay attacks).
func Sign(secret string, ts time.Time, body []byte) string {
	unix := strconv.FormatInt(ts.Unix(), 10)
	return "t=" + unix + ",v1=" + mac(secret, unix, body)
}

func mac(secret, unix string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(unix))
	m.Write([]byte("."))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

var (
	ErrBadSignature = errors.New("webhook: invalid signature")
	ErrTooOld       = errors.New("webhook: timestamp outside tolerance")
)

// Verify is what a receiver runs. It uses hmac.Equal (constant time) so an attacker cannot
// guess the signature byte by byte by measuring response times.
func Verify(secret, header string, body []byte, tolerance time.Duration, now time.Time) error {
	var unix, sig string
	for _, part := range strings.Split(header, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(part), "=")
		switch k {
		case "t":
			unix = v
		case "v1":
			sig = v
		}
	}
	ts, err := strconv.ParseInt(unix, 10, 64)
	if err != nil || sig == "" {
		return ErrBadSignature
	}
	if d := now.Sub(time.Unix(ts, 0)); d > tolerance || d < -tolerance {
		return ErrTooOld
	}
	if !hmac.Equal([]byte(sig), []byte(mac(secret, unix, body))) {
		return ErrBadSignature
	}
	return nil
}

// Backoff returns the delay before attempt n+1 after n failed attempts:
// exponential (2s, 4s, 8s, ... capped at 1h) with "equal jitter" (between d/2 and d).
// Jitter spreads retries so that 10,000 events failing at once (receiver outage)
// do not all retry at the exact same second and knock the receiver down again (thundering herd).
func Backoff(attempts int) time.Duration {
	const base, maxDelay = 2 * time.Second, time.Hour
	if attempts < 1 {
		attempts = 1
	}
	d := time.Duration(float64(base) * math.Pow(2, float64(attempts-1)))
	if d > maxDelay || d <= 0 {
		d = maxDelay
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

func describeStatus(code int) string { return fmt.Sprintf("receiver answered HTTP %d", code) }
