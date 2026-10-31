package httpapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/google/uuid"

	"ledgerflow/internal/ledger"
)

const maxBody = 1 << 20 // 1 MiB: protects memory against huge payloads

// readBody reads the raw body (needed both for decoding and for the idempotency hash).
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return nil, errBodyTooLarge
	}
	return b, err
}

// decode reads strict JSON: unknown fields are rejected so typos ("ammount") fail loudly.
func decode(w http.ResponseWriter, r *http.Request, dst any) ([]byte, bool) {
	body, err := readBody(w, r)
	if err != nil {
		badRequest(w, r, err.Error())
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		badRequest(w, r, "invalid JSON body: "+err.Error())
		return nil, false
	}
	return body, true
}

// idempotency builds the key + a hash of (method, path, body).
// Same key + different hash = the client is reusing a key for another operation: refuse.
func idempotency(r *http.Request, body []byte) ledger.Idempotency {
	h := sha256.New()
	fmt.Fprintf(h, "%s %s\n", r.Method, r.URL.Path)
	h.Write(body)
	return ledger.Idempotency{Key: r.Header.Get("Idempotency-Key"), RequestHash: h.Sum(nil)}
}

func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErrorCode(w, r, http.StatusNotFound, "not_found", "not found")
		return uuid.Nil, false
	}
	return id, true
}

func queryInt(r *http.Request, name string) int64 {
	n, _ := strconv.ParseInt(r.URL.Query().Get(name), 10, 64)
	return n
}

type page[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// ---------- admin ----------

func (s *Server) createClient(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name       string `json:"name"`
		WebhookURL string `json:"webhook_url"`
	}
	if _, ok := decode(w, r, &in); !ok {
		return
	}
	if in.WebhookURL != "" && !validWebhookURL(in.WebhookURL) {
		badRequest(w, r, "webhook_url must be an absolute http(s) URL")
		return
	}
	c, key, err := s.svc.CreateClient(r.Context(), in.Name, in.WebhookURL)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"client_id":      c.ID,
		"name":           c.Name,
		"api_key":        key, // shown once, never retrievable again
		"webhook_secret": c.WebhookSecret,
	})
}

func validWebhookURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

// ---------- webhook config ----------

func (s *Server) putWebhook(w http.ResponseWriter, r *http.Request) {
	var in struct {
		URL string `json:"url"`
	}
	if _, ok := decode(w, r, &in); !ok {
		return
	}
	if in.URL != "" && !validWebhookURL(in.URL) {
		badRequest(w, r, "url must be an absolute http(s) URL")
		return
	}
	c := clientFrom(r.Context())
	if err := s.svc.SetWebhookURL(r.Context(), c.ID, in.URL); err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"url": in.URL, "secret": c.WebhookSecret})
}

// ---------- accounts ----------

func (s *Server) createAccount(w http.ResponseWriter, r *http.Request) {
	var in struct {
		OwnerName string `json:"owner_name"`
		Currency  string `json:"currency"`
	}
	if _, ok := decode(w, r, &in); !ok {
		return
	}
	a, err := s.svc.CreateAccount(r.Context(), clientFrom(r.Context()).ID, in.OwnerName, in.Currency)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, a)
}

func (s *Server) listAccounts(w http.ResponseWriter, r *http.Request) {
	var after uuid.UUID
	if c := r.URL.Query().Get("cursor"); c != "" {
		var err error
		if after, err = uuid.Parse(c); err != nil {
			badRequest(w, r, "invalid cursor")
			return
		}
	}
	limit := int(queryInt(r, "limit"))
	accts, err := s.svc.ListAccounts(r.Context(), clientFrom(r.Context()).ID, after, limit)
	if err != nil {
		writeError(w, r, err)
		return
	}
	p := page[ledger.Account]{Data: accts}
	if len(accts) > 0 && len(accts) == ledger.ClampLimit(limit) {
		p.NextCursor = accts[len(accts)-1].ID.String()
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	a, err := s.svc.GetAccount(r.Context(), clientFrom(r.Context()).ID, id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) listEntries(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	limit := int(queryInt(r, "limit"))
	es, err := s.svc.ListEntries(r.Context(), clientFrom(r.Context()).ID, id, queryInt(r, "cursor"), limit)
	if err != nil {
		writeError(w, r, err)
		return
	}
	p := page[ledger.Entry]{Data: es}
	if len(es) > 0 && len(es) == ledger.ClampLimit(limit) {
		p.NextCursor = strconv.FormatInt(es[len(es)-1].ID, 10)
	}
	writeJSON(w, http.StatusOK, p)
}

// ---------- money movements ----------

func (s *Server) respondMovement(w http.ResponseWriter, r *http.Request, t ledger.Transfer, replayed bool, err error) {
	if err != nil {
		writeError(w, r, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, http.StatusCreated, t)
}

type accountMovement struct {
	AccountID uuid.UUID `json:"account_id"`
	Amount    int64     `json:"amount"`
	Reference string    `json:"reference"`
}

func (s *Server) deposit(w http.ResponseWriter, r *http.Request) {
	var in accountMovement
	body, ok := decode(w, r, &in)
	if !ok {
		return
	}
	t, replayed, err := s.svc.Deposit(r.Context(), clientFrom(r.Context()).ID, in.AccountID, in.Amount, in.Reference, idempotency(r, body))
	s.respondMovement(w, r, t, replayed, err)
}

func (s *Server) withdraw(w http.ResponseWriter, r *http.Request) {
	var in accountMovement
	body, ok := decode(w, r, &in)
	if !ok {
		return
	}
	t, replayed, err := s.svc.Withdraw(r.Context(), clientFrom(r.Context()).ID, in.AccountID, in.Amount, in.Reference, idempotency(r, body))
	s.respondMovement(w, r, t, replayed, err)
}

func (s *Server) transfer(w http.ResponseWriter, r *http.Request) {
	var in struct {
		From      uuid.UUID `json:"from_account_id"`
		To        uuid.UUID `json:"to_account_id"`
		Amount    int64     `json:"amount"`
		Reference string    `json:"reference"`
	}
	body, ok := decode(w, r, &in)
	if !ok {
		return
	}
	t, replayed, err := s.svc.Transfer(r.Context(), clientFrom(r.Context()).ID,
		ledger.TransferInput{From: in.From, To: in.To, Amount: in.Amount, Reference: in.Reference}, idempotency(r, body))
	s.respondMovement(w, r, t, replayed, err)
}

func (s *Server) getTransfer(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	t, err := s.svc.GetTransfer(r.Context(), clientFrom(r.Context()).ID, id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// ---------- events ----------

// listEvents: ?cursor=N returns events after N in order (for consumers that poll);
// ?order=desc returns the latest events, newest first (for dashboards).
func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	limit := int(queryInt(r, "limit"))
	if r.URL.Query().Get("order") == "desc" {
		evs, err := s.svc.RecentEvents(r.Context(), clientFrom(r.Context()).ID, limit)
		if err != nil {
			writeError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, page[ledger.Event]{Data: evs})
		return
	}
	evs, err := s.svc.ListEvents(r.Context(), clientFrom(r.Context()).ID, queryInt(r, "cursor"), limit)
	if err != nil {
		writeError(w, r, err)
		return
	}
	p := page[ledger.Event]{Data: evs}
	if len(evs) > 0 {
		// For events the cursor is always returned: clients poll "give me what came after X".
		p.NextCursor = strconv.FormatInt(evs[len(evs)-1].Sequence, 10)
	} else {
		p.NextCursor = r.URL.Query().Get("cursor")
	}
	writeJSON(w, http.StatusOK, p)
}
