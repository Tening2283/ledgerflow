// Package ledger is the domain core: accounts, money movements and their invariants.
//
// Invariants guaranteed by this package (and checked by the tests):
//  1. Every transfer writes exactly two entries whose amounts sum to zero (double entry).
//  2. For each client and currency, the sum of all account balances is always zero.
//  3. A user account balance never goes below zero.
//  4. accounts.balance == SUM(entries.amount) for that account.
//  5. A request repeated with the same Idempotency-Key moves money at most once.
package ledger

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

const (
	KindUser     = "user"
	KindExternal = "external"

	TransferKindTransfer   = "transfer"
	TransferKindDeposit    = "deposit"
	TransferKindWithdrawal = "withdrawal"

	EventTransferCreated = "transfer.created"
)

type Client struct {
	ID            uuid.UUID `json:"id"`
	Name          string    `json:"name"`
	WebhookURL    string    `json:"webhook_url,omitempty"`
	WebhookSecret string    `json:"-"`
	CreatedAt     time.Time `json:"created_at"`
}

type Account struct {
	ID        uuid.UUID `json:"id"`
	OwnerName string    `json:"owner_name"`
	Currency  string    `json:"currency"`
	Balance   int64     `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
	Kind      string    `json:"-"`
}

type Transfer struct {
	ID            uuid.UUID `json:"id"`
	Kind          string    `json:"kind"`
	FromAccountID uuid.UUID `json:"from_account_id"`
	ToAccountID   uuid.UUID `json:"to_account_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	Reference     string    `json:"reference"`
	CreatedAt     time.Time `json:"created_at"`
}

type Entry struct {
	ID           int64     `json:"id"`
	TransferID   uuid.UUID `json:"transfer_id"`
	Amount       int64     `json:"amount"`
	BalanceAfter int64     `json:"balance_after"`
	CreatedAt    time.Time `json:"created_at"`
}

type Event struct {
	Sequence  int64           `json:"sequence"`
	ID        uuid.UUID       `json:"id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Status    string          `json:"delivery_status"`
	Attempts  int             `json:"delivery_attempts"`
	CreatedAt time.Time       `json:"created_at"`
}

// Idempotency carries the client-supplied key and a hash of the request,
// so that reusing a key with a different payload is detected.
type Idempotency struct {
	Key         string
	RequestHash []byte
}

// Domain errors. The HTTP layer maps them to status codes; the domain knows nothing about HTTP.
var (
	ErrNotFound            = errors.New("not found")
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrCurrencyMismatch    = errors.New("accounts have different currencies")
	ErrSameAccount         = errors.New("source and destination must differ")
	ErrInvalidAmount       = errors.New("amount must be a positive integer (minor units)")
	ErrInvalidCurrency     = errors.New("currency must be a 3-letter ISO 4217 code")
	ErrInvalidInput        = errors.New("invalid input")
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
	ErrIdempotencyRequired = errors.New("Idempotency-Key header is required")
)
