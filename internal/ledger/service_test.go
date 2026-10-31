package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"ledgerflow/internal/ledger"
	"ledgerflow/internal/testutil"
)

type fixture struct {
	pool   *pgxpool.Pool
	svc    *ledger.Service
	client ledger.Client
	ctx    context.Context
}

func setup(t *testing.T) *fixture {
	t.Helper()
	pool := testutil.NewPool(t)
	svc := ledger.NewService(pool)
	c, _, err := svc.CreateClient(context.Background(), "test-client", "")
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{pool: pool, svc: svc, client: c, ctx: context.Background()}
}

func key() ledger.Idempotency {
	return ledger.Idempotency{Key: uuid.NewString(), RequestHash: []byte("h")}
}

func (f *fixture) account(t *testing.T, currency string, funded int64) ledger.Account {
	t.Helper()
	a, err := f.svc.CreateAccount(f.ctx, f.client.ID, "owner", currency)
	if err != nil {
		t.Fatal(err)
	}
	if funded > 0 {
		if _, _, err := f.svc.Deposit(f.ctx, f.client.ID, a.ID, funded, "seed", key()); err != nil {
			t.Fatal(err)
		}
	}
	return a
}

func (f *fixture) balance(t *testing.T, id uuid.UUID) int64 {
	t.Helper()
	a, err := f.svc.GetAccount(f.ctx, f.client.ID, id)
	if err != nil {
		t.Fatal(err)
	}
	return a.Balance
}

// checkInvariants verifies the ledger is consistent, straight from SQL.
func (f *fixture) checkInvariants(t *testing.T) {
	t.Helper()
	var sum int64
	var bad int
	// 1. Sum of all balances per currency is zero.
	rows, err := f.pool.Query(f.ctx, `SELECT currency, SUM(balance) FROM accounts GROUP BY currency`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var cur string
		if err := rows.Scan(&cur, &sum); err != nil {
			t.Fatal(err)
		}
		if sum != 0 {
			t.Errorf("sum of balances for %s = %d, want 0", cur, sum)
		}
	}
	rows.Close()
	// 2. Each transfer has exactly two entries summing to zero.
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM (
		SELECT transfer_id FROM entries GROUP BY transfer_id HAVING count(*) <> 2 OR SUM(amount) <> 0) x`).Scan(&bad); err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Errorf("%d transfers violate double entry", bad)
	}
	// 3. Cached balance equals the sum of entries.
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM accounts a
		WHERE a.balance <> COALESCE((SELECT SUM(amount) FROM entries e WHERE e.account_id = a.id), 0)`).Scan(&bad); err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Errorf("%d accounts have balance != sum(entries)", bad)
	}
	// 4. No negative user balance.
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM accounts WHERE kind = 'user' AND balance < 0`).Scan(&bad); err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Errorf("%d user accounts are negative", bad)
	}
}

func TestTransferMovesMoney(t *testing.T) {
	f := setup(t)
	a := f.account(t, "XOF", 10_000)
	b := f.account(t, "XOF", 0)

	tr, replayed, err := f.svc.Transfer(f.ctx, f.client.ID, ledger.TransferInput{From: a.ID, To: b.ID, Amount: 2_500, Reference: "rent"}, key())
	if err != nil || replayed {
		t.Fatalf("transfer: err=%v replayed=%v", err, replayed)
	}
	if tr.Amount != 2_500 || tr.Currency != "XOF" || tr.Kind != "transfer" {
		t.Fatalf("unexpected transfer %+v", tr)
	}
	if got := f.balance(t, a.ID); got != 7_500 {
		t.Errorf("a balance = %d, want 7500", got)
	}
	if got := f.balance(t, b.ID); got != 2_500 {
		t.Errorf("b balance = %d, want 2500", got)
	}
	entries, err := f.svc.ListEntries(f.ctx, f.client.ID, a.ID, 0, 10)
	if err != nil || len(entries) != 2 || entries[0].Amount != -2_500 || entries[0].BalanceAfter != 7_500 {
		t.Fatalf("statement: %v %+v", err, entries)
	}
	f.checkInvariants(t)
}

func TestTransferErrors(t *testing.T) {
	f := setup(t)
	a := f.account(t, "XOF", 1_000)
	b := f.account(t, "XOF", 0)
	eur := f.account(t, "EUR", 0)

	cases := []struct {
		name string
		in   ledger.TransferInput
		want error
	}{
		{"insufficient funds", ledger.TransferInput{From: a.ID, To: b.ID, Amount: 1_001}, ledger.ErrInsufficientFunds},
		{"zero amount", ledger.TransferInput{From: a.ID, To: b.ID, Amount: 0}, ledger.ErrInvalidAmount},
		{"negative amount", ledger.TransferInput{From: a.ID, To: b.ID, Amount: -5}, ledger.ErrInvalidAmount},
		{"same account", ledger.TransferInput{From: a.ID, To: a.ID, Amount: 1}, ledger.ErrSameAccount},
		{"currency mismatch", ledger.TransferInput{From: a.ID, To: eur.ID, Amount: 1}, ledger.ErrCurrencyMismatch},
		{"unknown account", ledger.TransferInput{From: a.ID, To: uuid.New(), Amount: 1}, ledger.ErrNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := f.svc.Transfer(f.ctx, f.client.ID, tc.in, key())
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
	if got := f.balance(t, a.ID); got != 1_000 {
		t.Errorf("failed transfers changed the balance: %d", got)
	}
	f.checkInvariants(t)
}

func TestTenantIsolation(t *testing.T) {
	f := setup(t)
	a := f.account(t, "XOF", 1_000)
	other, _, err := f.svc.CreateClient(f.ctx, "other", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.GetAccount(f.ctx, other.ID, a.ID); !errors.Is(err, ledger.ErrNotFound) {
		t.Fatalf("another client can read the account: %v", err)
	}
	b, _ := f.svc.CreateAccount(f.ctx, other.ID, "x", "XOF")
	if _, _, err := f.svc.Transfer(f.ctx, other.ID, ledger.TransferInput{From: a.ID, To: b.ID, Amount: 1}, key()); !errors.Is(err, ledger.ErrNotFound) {
		t.Fatalf("another client can move the money: %v", err)
	}
}

func TestWithdrawal(t *testing.T) {
	f := setup(t)
	a := f.account(t, "XOF", 5_000)
	if _, _, err := f.svc.Withdraw(f.ctx, f.client.ID, a.ID, 6_000, "", key()); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("overdraft withdrawal: %v", err)
	}
	if _, _, err := f.svc.Withdraw(f.ctx, f.client.ID, a.ID, 5_000, "", key()); err != nil {
		t.Fatal(err)
	}
	if got := f.balance(t, a.ID); got != 0 {
		t.Errorf("balance = %d, want 0", got)
	}
	f.checkInvariants(t)
}

func TestIdempotentReplay(t *testing.T) {
	f := setup(t)
	a := f.account(t, "XOF", 10_000)
	b := f.account(t, "XOF", 0)
	k := key()
	in := ledger.TransferInput{From: a.ID, To: b.ID, Amount: 1_000}

	first, replayed, err := f.svc.Transfer(f.ctx, f.client.ID, in, k)
	if err != nil || replayed {
		t.Fatal(err, replayed)
	}
	second, replayed, err := f.svc.Transfer(f.ctx, f.client.ID, in, k)
	if err != nil || !replayed {
		t.Fatalf("expected replay, got err=%v replayed=%v", err, replayed)
	}
	if first.ID != second.ID {
		t.Fatalf("replay returned another transfer")
	}
	if got := f.balance(t, a.ID); got != 9_000 {
		t.Errorf("money moved twice: balance %d", got)
	}

	// Same key, different payload -> rejected.
	_, _, err = f.svc.Transfer(f.ctx, f.client.ID, in, ledger.Idempotency{Key: k.Key, RequestHash: []byte("other")})
	if !errors.Is(err, ledger.ErrIdempotencyMismatch) {
		t.Fatalf("err = %v, want mismatch", err)
	}
	// Missing key -> rejected.
	if _, _, err := f.svc.Transfer(f.ctx, f.client.ID, in, ledger.Idempotency{}); !errors.Is(err, ledger.ErrIdempotencyRequired) {
		t.Fatalf("err = %v", err)
	}
}

func TestFailedRequestDoesNotConsumeKey(t *testing.T) {
	f := setup(t)
	a := f.account(t, "XOF", 0)
	b := f.account(t, "XOF", 0)
	k := key()
	in := ledger.TransferInput{From: a.ID, To: b.ID, Amount: 500}
	if _, _, err := f.svc.Transfer(f.ctx, f.client.ID, in, k); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatal(err)
	}
	// Fund the account, retry with the same key: it must now succeed.
	if _, _, err := f.svc.Deposit(f.ctx, f.client.ID, a.ID, 500, "", key()); err != nil {
		t.Fatal(err)
	}
	if _, replayed, err := f.svc.Transfer(f.ctx, f.client.ID, in, k); err != nil || replayed {
		t.Fatalf("retry after failure: err=%v replayed=%v", err, replayed)
	}
}

// The same request sent 20 times concurrently (e.g. a client retrying aggressively) moves money once.
func TestConcurrentSameIdempotencyKey(t *testing.T) {
	f := setup(t)
	a := f.account(t, "XOF", 10_000)
	b := f.account(t, "XOF", 0)
	k := key()
	in := ledger.TransferInput{From: a.ID, To: b.ID, Amount: 100}

	var wg sync.WaitGroup
	var created, replays atomic.Int32
	ids := sync.Map{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr, replayed, err := f.svc.Transfer(f.ctx, f.client.ID, in, k)
			if err != nil {
				t.Error(err)
				return
			}
			ids.Store(tr.ID, true)
			if replayed {
				replays.Add(1)
			} else {
				created.Add(1)
			}
		}()
	}
	wg.Wait()
	if created.Load() != 1 || replays.Load() != 19 {
		t.Fatalf("created=%d replays=%d, want 1/19", created.Load(), replays.Load())
	}
	n := 0
	ids.Range(func(_, _ any) bool { n++; return true })
	if n != 1 {
		t.Fatalf("%d distinct transfers returned", n)
	}
	if got := f.balance(t, b.ID); got != 100 {
		t.Fatalf("b balance = %d, want 100", got)
	}
}

// Stress test: many goroutines move money randomly between a few accounts, in both directions
// (A->B and B->A at the same time is exactly what causes deadlocks with naive locking).
func TestConcurrentRandomTransfersKeepInvariants(t *testing.T) {
	f := setup(t)
	const nAccounts, nWorkers, perWorker = 6, 16, 40
	accounts := make([]ledger.Account, nAccounts)
	for i := range accounts {
		accounts[i] = f.account(t, "XOF", 1_000)
	}

	var wg sync.WaitGroup
	var ok, insufficient atomic.Int32
	for w := 0; w < nWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 42))
			for i := 0; i < perWorker; i++ {
				from := accounts[r.IntN(nAccounts)]
				to := accounts[r.IntN(nAccounts)]
				if from.ID == to.ID {
					continue
				}
				_, _, err := f.svc.Transfer(f.ctx, f.client.ID, ledger.TransferInput{
					From: from.ID, To: to.ID, Amount: int64(1 + r.IntN(400)),
				}, ledger.Idempotency{Key: fmt.Sprintf("w%d-%d", w, i), RequestHash: []byte("h")})
				switch {
				case err == nil:
					ok.Add(1)
				case errors.Is(err, ledger.ErrInsufficientFunds):
					insufficient.Add(1)
				default:
					t.Errorf("unexpected error: %v", err)
				}
			}
		}(w)
	}
	wg.Wait()
	t.Logf("succeeded=%d insufficient_funds=%d", ok.Load(), insufficient.Load())

	var total int64
	for _, a := range accounts {
		total += f.balance(t, a.ID)
	}
	if total != nAccounts*1_000 {
		t.Fatalf("money was created or destroyed: total=%d", total)
	}
	f.checkInvariants(t)
}

func TestConcurrentFirstDepositsCreateOneExternalAccount(t *testing.T) {
	f := setup(t)
	a := f.account(t, "XOF", 0)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := f.svc.Deposit(f.ctx, f.client.ID, a.ID, 10, "", key()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var n int
	_ = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM accounts WHERE kind = 'external'`).Scan(&n)
	if n != 1 {
		t.Fatalf("%d external accounts, want 1", n)
	}
	if got := f.balance(t, a.ID); got != 100 {
		t.Fatalf("balance %d", got)
	}
	f.checkInvariants(t)
}

func TestEventsWrittenInOutbox(t *testing.T) {
	f := setup(t)
	a := f.account(t, "XOF", 1_000) // 1 deposit event
	b := f.account(t, "XOF", 0)
	if _, _, err := f.svc.Transfer(f.ctx, f.client.ID, ledger.TransferInput{From: a.ID, To: b.ID, Amount: 1}, key()); err != nil {
		t.Fatal(err)
	}
	evs, err := f.svc.ListEvents(f.ctx, f.client.ID, 0, 10)
	if err != nil || len(evs) != 2 {
		t.Fatalf("events: %v %d", err, len(evs))
	}
	if evs[1].Type != ledger.EventTransferCreated || evs[1].Sequence <= evs[0].Sequence {
		t.Fatalf("bad events %+v", evs)
	}
	more, _ := f.svc.ListEvents(f.ctx, f.client.ID, evs[1].Sequence, 10)
	if len(more) != 0 {
		t.Fatalf("cursor pagination broken")
	}
}
