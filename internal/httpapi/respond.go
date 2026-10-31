package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"ledgerflow/internal/ledger"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type apiError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
}

func writeErrorCode(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	writeJSON(w, status, map[string]apiError{"error": {Code: code, Message: msg, RequestID: requestID(r.Context())}})
}

// writeError maps domain errors to HTTP. Unknown errors become a generic 500: we log the
// details server-side (with the request id) but never leak internals (SQL, stack) to clients.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	code := ledger.ErrorCode(err)
	status := http.StatusInternalServerError
	switch code {
	case "not_found":
		status = http.StatusNotFound
	case "insufficient_funds", "currency_mismatch", "same_account", "idempotency_mismatch":
		status = http.StatusUnprocessableEntity
	case "invalid_amount", "invalid_currency", "invalid_input", "idempotency_key_required":
		status = http.StatusBadRequest
	}
	if status == http.StatusInternalServerError {
		logger(r.Context()).Error("internal error", "err", err)
		writeErrorCode(w, r, status, "internal_error", "an internal error occurred")
		return
	}
	writeErrorCode(w, r, status, code, err.Error())
}

func badRequest(w http.ResponseWriter, r *http.Request, msg string) {
	writeErrorCode(w, r, http.StatusBadRequest, "invalid_input", msg)
}

var errBodyTooLarge = errors.New("request body too large")

func logger(ctx interface{ Value(any) any }) *slog.Logger {
	if l, ok := ctx.Value(ctxLogger).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}
