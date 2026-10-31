package ledger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"ledgerflow/internal/observability"
)

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, so read helpers work inside or outside a transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service { return &Service{pool: pool} }

var currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)

// ---------- Accounts ----------

func (s *Service) CreateAccount(ctx context.Context, clientID uuid.UUID, owner, currency string) (Account, error) {
	owner = strings.TrimSpace(owner)
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if owner == "" || len(owner) > 200 {
		return Account{}, fmt.Errorf("%w: owner_name is required (max 200 chars)", ErrInvalidInput)
	}
	if !currencyRe.MatchString(currency) {
		return Account{}, ErrInvalidCurrency
	}
	a := Account{ID: uuid.Must(uuid.NewV7()), OwnerName: owner, Currency: currency, Kind: KindUser}
	err := s.pool.QueryRow(ctx, `
		INSERT INTO accounts (id, client_id, kind, owner_name, currency)
		VALUES ($1, $2, 'user', $3, $4) RETURNING balance, created_at`,
		a.ID, clientID, owner, currency).Scan(&a.Balance, &a.CreatedAt)
	return a, err
}

func (s *Service) GetAccount(ctx context.Context, clientID, id uuid.UUID) (Account, error) {
	var a Account
	err := s.pool.QueryRow(ctx, `
		SELECT id, kind, owner_name, currency, balance, created_at FROM accounts
		WHERE id = $1 AND client_id = $2 AND kind = 'user'`, id, clientID).
		Scan(&a.ID, &a.Kind, &a.OwnerName, &a.Currency, &a.Balance, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	return a, err
}

// ListAccounts uses keyset (cursor) pagination: "WHERE id > cursor" stays O(log n)
// with the index, whereas OFFSET 100000 must scan and discard 100000 rows.
// UUIDv7 ids are time-ordered, so this also lists accounts by creation time.
func (s *Service) ListAccounts(ctx context.Context, clientID uuid.UUID, after uuid.UUID, limit int) ([]Account, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, kind, owner_name, currency, balance, created_at FROM accounts
		WHERE client_id = $1 AND kind = 'user' AND id > $2
		ORDER BY id LIMIT $3`, clientID, after, ClampLimit(limit))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Account, error) {
		var a Account
		err := r.Scan(&a.ID, &a.Kind, &a.OwnerName, &a.Currency, &a.Balance, &a.CreatedAt)
		return a, err
	})
}

// ListEntries returns an account statement, newest first. before=0 means "from the latest".
func (s *Service) ListEntries(ctx context.Context, clientID, accountID uuid.UUID, before int64, limit int) ([]Entry, error) {
	if _, err := s.GetAccount(ctx, clientID, accountID); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, transfer_id, amount, balance_after, created_at FROM entries
		WHERE account_id = $1 AND ($2 = 0 OR id < $2)
		ORDER BY id DESC LIMIT $3`, accountID, before, ClampLimit(limit))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Entry, error) {
		var e Entry
		err := r.Scan(&e.ID, &e.TransferID, &e.Amount, &e.BalanceAfter, &e.CreatedAt)
		return e, err
	})
}

// ---------- Money movements ----------

type TransferInput struct {
	From      uuid.UUID
	To        uuid.UUID
	Amount    int64
	Reference string
}

// Transfer moves money between two user accounts of the same client.
// The bool result is true when the response is an idempotent replay.
func (s *Service) Transfer(ctx context.Context, clientID uuid.UUID, in TransferInput, idem Idempotency) (Transfer, bool, error) {
	return s.withIdempotency(ctx, clientID, idem, TransferKindTransfer, func(tx pgx.Tx) (Transfer, error) {
		return s.move(ctx, tx, clientID, TransferKindTransfer, in.From, in.To, in.Amount, in.Reference)
	})
}

// Deposit brings money from the outside world (mobile money, card, bank) into an account.
// In double entry, money is never created: it comes from the client's "external" account,
// whose balance goes negative and represents "money we owe to account holders".
func (s *Service) Deposit(ctx context.Context, clientID, accountID uuid.UUID, amount int64, ref string, idem Idempotency) (Transfer, bool, error) {
	return s.withIdempotency(ctx, clientID, idem, TransferKindDeposit, func(tx pgx.Tx) (Transfer, error) {
		ext, err := s.externalAccountFor(ctx, tx, clientID, accountID)
		if err != nil {
			return Transfer{}, err
		}
		return s.move(ctx, tx, clientID, TransferKindDeposit, ext, accountID, amount, ref)
	})
}

// Withdraw sends money from an account back to the outside world.
func (s *Service) Withdraw(ctx context.Context, clientID, accountID uuid.UUID, amount int64, ref string, idem Idempotency) (Transfer, bool, error) {
	return s.withIdempotency(ctx, clientID, idem, TransferKindWithdrawal, func(tx pgx.Tx) (Transfer, error) {
		ext, err := s.externalAccountFor(ctx, tx, clientID, accountID)
		if err != nil {
			return Transfer{}, err
		}
		return s.move(ctx, tx, clientID, TransferKindWithdrawal, accountID, ext, amount, ref)
	})
}

func (s *Service) GetTransfer(ctx context.Context, clientID, id uuid.UUID) (Transfer, error) {
	return getTransfer(ctx, s.pool, clientID, id)
}

// withIdempotency runs op in a transaction that ALSO records the idempotency key.
//
// Why in the same transaction? If we saved the key separately, a crash between "money moved"
// and "key saved" would let a retry move the money twice. Here both commit, or neither does.
//
// Concurrency: if two requests with the same key arrive at the same time, the second INSERT
// blocks on the unique index until the first transaction finishes. If the first commits,
// the second hits the conflict (DO NOTHING) and replays the stored result. If the first rolls
// back (e.g. insufficient funds), the second proceeds as if it were first.
func (s *Service) withIdempotency(ctx context.Context, clientID uuid.UUID, idem Idempotency, kind string,
	op func(tx pgx.Tx) (Transfer, error)) (t Transfer, replayed bool, err error) {

	defer func() {
		result := "ok"
		switch {
		case replayed:
			result = "replayed"
			observability.IdempotentReplays.Inc()
		case err != nil:
			result = errorCode(err)
		}
		observability.Transfers.WithLabelValues(kind, result).Inc()
	}()

	if idem.Key == "" {
		return Transfer{}, false, ErrIdempotencyRequired
	}
	if len(idem.Key) > 255 {
		return Transfer{}, false, fmt.Errorf("%w: Idempotency-Key too long (max 255)", ErrInvalidInput)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Transfer{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit

	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (client_id, key, request_hash) VALUES ($1, $2, $3)
		ON CONFLICT (client_id, key) DO NOTHING`, clientID, idem.Key, idem.RequestHash)
	if err != nil {
		return Transfer{}, false, err
	}

	if tag.RowsAffected() == 0 {
		// The key already exists and is committed: replay.
		var storedHash []byte
		var transferID uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT request_hash, transfer_id FROM idempotency_keys WHERE client_id = $1 AND key = $2`,
			clientID, idem.Key).Scan(&storedHash, &transferID); err != nil {
			return Transfer{}, false, err
		}
		if !bytes.Equal(storedHash, idem.RequestHash) {
			return Transfer{}, false, ErrIdempotencyMismatch
		}
		t, err := getTransfer(ctx, tx, clientID, transferID)
		return t, err == nil, err
	}

	t, err = op(tx)
	if err != nil {
		return Transfer{}, false, err // rollback: the key is not consumed, the client may retry later
	}
	if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET transfer_id = $3 WHERE client_id = $1 AND key = $2`,
		clientID, idem.Key, t.ID); err != nil {
		return Transfer{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Transfer{}, false, err
	}
	return t, false, nil
}

type lockedAccount struct {
	id       uuid.UUID
	kind     string
	currency string
	balance  int64
}

// move is the heart of the ledger. It must run inside a transaction.
func (s *Service) move(ctx context.Context, tx pgx.Tx, clientID uuid.UUID, kind string,
	fromID, toID uuid.UUID, amount int64, ref string) (Transfer, error) {

	if amount <= 0 {
		return Transfer{}, ErrInvalidAmount
	}
	if fromID == toID {
		return Transfer{}, ErrSameAccount
	}
	if len(ref) > 500 {
		return Transfer{}, fmt.Errorf("%w: reference too long (max 500)", ErrInvalidInput)
	}

	// Pessimistic locking with a GLOBAL ORDER (ORDER BY id) to prevent deadlocks:
	// if T1 locks A then B while T2 locks B then A, they wait for each other forever.
	// When everybody locks in id order, the cycle cannot form.
	rows, err := tx.Query(ctx, `
		SELECT id, kind, currency, balance FROM accounts
		WHERE client_id = $1 AND id IN ($2, $3)
		ORDER BY id
		FOR UPDATE`, clientID, fromID, toID)
	if err != nil {
		return Transfer{}, err
	}
	locked, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (lockedAccount, error) {
		var a lockedAccount
		err := r.Scan(&a.id, &a.kind, &a.currency, &a.balance)
		return a, err
	})
	if err != nil {
		return Transfer{}, err
	}
	if len(locked) != 2 {
		return Transfer{}, fmt.Errorf("%w: account", ErrNotFound)
	}
	from, to := locked[0], locked[1]
	if from.id != fromID {
		from, to = to, from
	}
	if kind == TransferKindTransfer && (from.kind != KindUser || to.kind != KindUser) {
		return Transfer{}, fmt.Errorf("%w: account", ErrNotFound)
	}
	if from.currency != to.currency {
		return Transfer{}, ErrCurrencyMismatch
	}
	if from.kind == KindUser && from.balance < amount {
		return Transfer{}, ErrInsufficientFunds
	}

	t := Transfer{
		ID: uuid.Must(uuid.NewV7()), Kind: kind, FromAccountID: fromID, ToAccountID: toID,
		Amount: amount, Currency: from.currency, Reference: ref,
	}
	if err := tx.QueryRow(ctx, `
		INSERT INTO transfers (id, client_id, kind, from_account_id, to_account_id, amount, currency, reference)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING created_at`,
		t.ID, clientID, kind, fromID, toID, amount, t.Currency, ref).Scan(&t.CreatedAt); err != nil {
		return Transfer{}, err
	}

	// Double entry: debit and credit, summing to zero.
	if err := postEntry(ctx, tx, t.ID, fromID, -amount); err != nil {
		return Transfer{}, err
	}
	if err := postEntry(ctx, tx, t.ID, toID, amount); err != nil {
		return Transfer{}, err
	}

	// Transactional outbox: the event is committed atomically with the money movement.
	if err := insertEvent(ctx, tx, clientID, EventTransferCreated, t); err != nil {
		return Transfer{}, err
	}
	return t, nil
}

func postEntry(ctx context.Context, tx pgx.Tx, transferID, accountID uuid.UUID, amount int64) error {
	var balanceAfter int64
	if err := tx.QueryRow(ctx, `UPDATE accounts SET balance = balance + $2 WHERE id = $1 RETURNING balance`,
		accountID, amount).Scan(&balanceAfter); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO entries (transfer_id, account_id, amount, balance_after) VALUES ($1, $2, $3, $4)`,
		transferID, accountID, amount, balanceAfter)
	return err
}

func insertEvent(ctx context.Context, tx pgx.Tx, clientID uuid.UUID, typ string, data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events (event_id, client_id, type, payload) VALUES ($1, $2, $3, $4)`,
		uuid.Must(uuid.NewV7()), clientID, typ, payload)
	return err
}

// externalAccountFor returns (creating it if needed) the external account matching the user account's currency.
func (s *Service) externalAccountFor(ctx context.Context, tx pgx.Tx, clientID, accountID uuid.UUID) (uuid.UUID, error) {
	var currency string
	err := tx.QueryRow(ctx, `SELECT currency FROM accounts WHERE id = $1 AND client_id = $2 AND kind = 'user'`,
		accountID, clientID).Scan(&currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, fmt.Errorf("%w: account", ErrNotFound)
	}
	if err != nil {
		return uuid.Nil, err
	}
	// Race-free "get or create": two concurrent first deposits both try to INSERT,
	// the unique partial index lets only one win, and both then read the same row.
	if _, err := tx.Exec(ctx, `
		INSERT INTO accounts (id, client_id, kind, owner_name, currency)
		VALUES ($1, $2, 'external', 'External funding', $3)
		ON CONFLICT (client_id, currency) WHERE kind = 'external' DO NOTHING`,
		uuid.Must(uuid.NewV7()), clientID, currency); err != nil {
		return uuid.Nil, err
	}
	var id uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM accounts WHERE client_id = $1 AND currency = $2 AND kind = 'external'`,
		clientID, currency).Scan(&id)
	return id, err
}

func getTransfer(ctx context.Context, q querier, clientID, id uuid.UUID) (Transfer, error) {
	var t Transfer
	err := q.QueryRow(ctx, `
		SELECT id, kind, from_account_id, to_account_id, amount, currency, reference, created_at
		FROM transfers WHERE id = $1 AND client_id = $2`, id, clientID).
		Scan(&t.ID, &t.Kind, &t.FromAccountID, &t.ToAccountID, &t.Amount, &t.Currency, &t.Reference, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, ErrNotFound
	}
	return t, err
}

// ---------- Events ----------

// ListEvents lets clients poll events (alternative / complement to webhooks).
func (s *Service) ListEvents(ctx context.Context, clientID uuid.UUID, after int64, limit int) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, event_id, type, payload, status, attempts, created_at FROM outbox_events
		WHERE client_id = $1 AND id > $2 ORDER BY id LIMIT $3`, clientID, after, ClampLimit(limit))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanEvent)
}

// ClampLimit bounds page sizes to [1,100], default 20.
// RecentEvents returns the latest events, newest first (used by the dashboard).
func (s *Service) RecentEvents(ctx context.Context, clientID uuid.UUID, limit int) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, event_id, type, payload, status, attempts, created_at FROM outbox_events
		WHERE client_id = $1 ORDER BY id DESC LIMIT $2`, clientID, ClampLimit(limit))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanEvent)
}

func scanEvent(r pgx.CollectableRow) (Event, error) {
	var e Event
	err := r.Scan(&e.Sequence, &e.ID, &e.Type, &e.Data, &e.Status, &e.Attempts, &e.CreatedAt)
	return e, err
}

func ClampLimit(n int) int {
	switch {
	case n <= 0:
		return 20
	case n > 100:
		return 100
	}
	return n
}

// errorCode gives a stable machine-readable code for an error (used by HTTP and metrics).
func errorCode(err error) string {
	switch {
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrInsufficientFunds):
		return "insufficient_funds"
	case errors.Is(err, ErrCurrencyMismatch):
		return "currency_mismatch"
	case errors.Is(err, ErrSameAccount):
		return "same_account"
	case errors.Is(err, ErrInvalidAmount):
		return "invalid_amount"
	case errors.Is(err, ErrInvalidCurrency):
		return "invalid_currency"
	case errors.Is(err, ErrInvalidInput):
		return "invalid_input"
	case errors.Is(err, ErrIdempotencyMismatch):
		return "idempotency_mismatch"
	case errors.Is(err, ErrIdempotencyRequired):
		return "idempotency_key_required"
	default:
		return "internal_error"
	}
}

// ErrorCode is exported for the HTTP layer.
func ErrorCode(err error) string { return errorCode(err) }
