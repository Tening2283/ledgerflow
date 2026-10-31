package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"ledgerflow/internal/httpapi"
	"ledgerflow/internal/ledger"
	"ledgerflow/internal/ratelimit"
	"ledgerflow/internal/testutil"
)

type api struct {
	t   *testing.T
	srv *httptest.Server
	key string
}

func newAPI(t *testing.T, limiter ratelimit.Limiter) *api {
	pool := testutil.NewPool(t)
	svc := ledger.NewService(pool)
	h := httpapi.New(httpapi.Deps{Ledger: svc, Limiter: limiter, Logger: testutil.Logger(), AdminToken: "admin-secret"})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	a := &api{t: t, srv: srv}
	var out struct {
		APIKey string `json:"api_key"`
	}
	res := a.do("POST", "/admin/clients", "admin-secret", "", map[string]string{"name": "shop"}, &out)
	if res.StatusCode != 201 || !strings.HasPrefix(out.APIKey, "lf_") {
		t.Fatalf("create client: %d", res.StatusCode)
	}
	a.key = out.APIKey
	return a
}

func (a *api) do(method, path, token, idemKey string, body any, out any) *http.Response {
	a.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, a.srv.URL+path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			a.t.Fatalf("%s %s: decode %q: %v", method, path, raw, err)
		}
	}
	return res
}

type errBody struct {
	Error struct {
		Code string `json:"code"`
	} `json:"error"`
}

func TestEndToEndFlow(t *testing.T) {
	a := newAPI(t, nil)

	var alice, bob ledger.Account
	if r := a.do("POST", "/v1/accounts", a.key, "", map[string]string{"owner_name": "Awa", "currency": "xof"}, &alice); r.StatusCode != 201 {
		t.Fatalf("create account: %d", r.StatusCode)
	}
	if alice.Currency != "XOF" {
		t.Fatalf("currency not normalised: %s", alice.Currency)
	}
	a.do("POST", "/v1/accounts", a.key, "", map[string]string{"owner_name": "Moussa", "currency": "XOF"}, &bob)

	// Deposit
	r := a.do("POST", "/v1/deposits", a.key, "dep-1", map[string]any{"account_id": alice.ID, "amount": 50_000}, nil)
	if r.StatusCode != 201 {
		t.Fatalf("deposit: %d", r.StatusCode)
	}

	// Transfer, then exact retry (replay)
	body := map[string]any{"from_account_id": alice.ID, "to_account_id": bob.ID, "amount": 20_000, "reference": "loyer"}
	var t1, t2 ledger.Transfer
	r = a.do("POST", "/v1/transfers", a.key, "tr-1", body, &t1)
	if r.StatusCode != 201 || r.Header.Get("Idempotent-Replayed") != "" {
		t.Fatalf("transfer: %d", r.StatusCode)
	}
	r = a.do("POST", "/v1/transfers", a.key, "tr-1", body, &t2)
	if r.StatusCode != 201 || r.Header.Get("Idempotent-Replayed") != "true" || t1.ID != t2.ID {
		t.Fatalf("replay: %d %q", r.StatusCode, r.Header.Get("Idempotent-Replayed"))
	}

	// Same key, different body
	var e errBody
	body["amount"] = 1
	if r = a.do("POST", "/v1/transfers", a.key, "tr-1", body, &e); r.StatusCode != 422 || e.Error.Code != "idempotency_mismatch" {
		t.Fatalf("mismatch: %d %s", r.StatusCode, e.Error.Code)
	}

	// Insufficient funds
	if r = a.do("POST", "/v1/transfers", a.key, "tr-2",
		map[string]any{"from_account_id": bob.ID, "to_account_id": alice.ID, "amount": 999_999}, &e); r.StatusCode != 422 || e.Error.Code != "insufficient_funds" {
		t.Fatalf("insufficient: %d %s", r.StatusCode, e.Error.Code)
	}

	// Balances
	var got ledger.Account
	a.do("GET", "/v1/accounts/"+alice.ID.String(), a.key, "", nil, &got)
	if got.Balance != 30_000 {
		t.Fatalf("alice balance %d", got.Balance)
	}

	// Statement
	var st struct {
		Data []ledger.Entry `json:"data"`
	}
	a.do("GET", "/v1/accounts/"+alice.ID.String()+"/entries", a.key, "", nil, &st)
	if len(st.Data) != 2 || st.Data[0].Amount != -20_000 {
		t.Fatalf("statement %+v", st.Data)
	}

	// Events
	var evs struct {
		Data       []ledger.Event `json:"data"`
		NextCursor string         `json:"next_cursor"`
	}
	a.do("GET", "/v1/events", a.key, "", nil, &evs)
	if len(evs.Data) != 2 || evs.NextCursor == "" {
		t.Fatalf("events %+v", evs)
	}

	// Pagination
	var list struct {
		Data       []ledger.Account `json:"data"`
		NextCursor string           `json:"next_cursor"`
	}
	a.do("GET", "/v1/accounts?limit=1", a.key, "", nil, &list)
	if len(list.Data) != 1 || list.NextCursor != alice.ID.String() {
		t.Fatalf("page 1 %+v", list)
	}
	a.do("GET", "/v1/accounts?limit=1&cursor="+list.NextCursor, a.key, "", nil, &list)
	if len(list.Data) != 1 || list.Data[0].ID != bob.ID {
		t.Fatalf("page 2 %+v", list)
	}
}

func TestAuthAndValidation(t *testing.T) {
	a := newAPI(t, nil)
	var e errBody
	if r := a.do("GET", "/v1/accounts", "", "", nil, &e); r.StatusCode != 401 {
		t.Fatalf("no key: %d", r.StatusCode)
	}
	if r := a.do("GET", "/v1/accounts", "lf_wrong", "", nil, &e); r.StatusCode != 401 {
		t.Fatalf("bad key: %d", r.StatusCode)
	}
	if r := a.do("POST", "/admin/clients", a.key, "", map[string]string{"name": "x"}, &e); r.StatusCode != 401 {
		t.Fatalf("client key used as admin: %d", r.StatusCode)
	}
	if r := a.do("POST", "/v1/accounts", a.key, "", map[string]string{"owner_name": "x", "currency": "francs"}, &e); r.StatusCode != 400 || e.Error.Code != "invalid_currency" {
		t.Fatalf("bad currency: %d %s", r.StatusCode, e.Error.Code)
	}
	if r := a.do("POST", "/v1/accounts", a.key, "", map[string]string{"owner_name": "x", "currency": "XOF", "ammount": "1"}, &e); r.StatusCode != 400 {
		t.Fatalf("unknown field accepted: %d", r.StatusCode)
	}
	if r := a.do("POST", "/v1/deposits", a.key, "", map[string]any{"account_id": uuid.New(), "amount": 1}, &e); r.StatusCode != 400 || e.Error.Code != "idempotency_key_required" {
		t.Fatalf("missing idempotency key: %d %s", r.StatusCode, e.Error.Code)
	}
	if r := a.do("GET", "/v1/accounts/not-a-uuid", a.key, "", nil, &e); r.StatusCode != 404 {
		t.Fatalf("bad id: %d", r.StatusCode)
	}
	r := a.do("GET", "/healthz", "", "", nil, nil)
	if r.StatusCode != 200 || r.Header.Get("X-Request-Id") == "" {
		t.Fatalf("healthz / request id: %d", r.StatusCode)
	}
}

type denyAfter struct{ n int }

func (d *denyAfter) Allow(context.Context, string) (ratelimit.Result, error) {
	d.n--
	if d.n < 0 {
		return ratelimit.Result{Allowed: false, Limit: 2, RetryAfter: 1500 * time.Millisecond}, nil
	}
	return ratelimit.Result{Allowed: true, Limit: 2, Remaining: d.n}, nil
}

func TestRateLimitReturns429(t *testing.T) {
	a := newAPI(t, &denyAfter{n: 2})
	a.do("GET", "/v1/accounts", a.key, "", nil, nil)
	a.do("GET", "/v1/accounts", a.key, "", nil, nil)
	r := a.do("GET", "/v1/accounts", a.key, "", nil, nil)
	if r.StatusCode != 429 || r.Header.Get("Retry-After") != "2" {
		t.Fatalf("got %d retry-after=%q", r.StatusCode, r.Header.Get("Retry-After"))
	}
}
