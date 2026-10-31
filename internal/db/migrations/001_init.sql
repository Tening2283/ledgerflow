-- Ledgerflow initial schema.
-- Money is stored as BIGINT in minor units (XOF has no decimals; for EUR, 1 = 1 cent).
-- Never use FLOAT for money.

CREATE TABLE api_clients (
    id             UUID PRIMARY KEY,
    name           TEXT        NOT NULL,
    api_key_hash   BYTEA       NOT NULL UNIQUE,   -- sha256(api_key); the raw key is never stored
    webhook_url    TEXT,
    webhook_secret TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE accounts (
    id         UUID PRIMARY KEY,                  -- UUIDv7: time-ordered, index friendly, cursor friendly
    client_id  UUID        NOT NULL REFERENCES api_clients(id),
    kind       TEXT        NOT NULL CHECK (kind IN ('user', 'external')),
    owner_name TEXT        NOT NULL,
    currency   CHAR(3)     NOT NULL,
    balance    BIGINT      NOT NULL DEFAULT 0,    -- cached projection of SUM(entries.amount)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Defense in depth: even if application code has a bug, a user account can never go negative.
    CONSTRAINT balance_non_negative CHECK (kind = 'external' OR balance >= 0)
);
CREATE INDEX accounts_client_idx ON accounts (client_id, id);
-- Exactly one "external world" account per client and currency (source of deposits, sink of withdrawals).
CREATE UNIQUE INDEX accounts_one_external_per_currency ON accounts (client_id, currency) WHERE kind = 'external';

CREATE TABLE transfers (
    id              UUID PRIMARY KEY,
    client_id       UUID        NOT NULL REFERENCES api_clients(id),
    kind            TEXT        NOT NULL CHECK (kind IN ('transfer', 'deposit', 'withdrawal')),
    from_account_id UUID        NOT NULL REFERENCES accounts(id),
    to_account_id   UUID        NOT NULL REFERENCES accounts(id),
    amount          BIGINT      NOT NULL CHECK (amount > 0),
    currency        CHAR(3)     NOT NULL,
    reference       TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (from_account_id <> to_account_id)
);
CREATE INDEX transfers_client_idx ON transfers (client_id, id);

-- Double-entry ledger: every transfer writes exactly two rows whose amounts sum to zero.
-- Entries are append-only: never UPDATE or DELETE them.
CREATE TABLE entries (
    id            BIGSERIAL PRIMARY KEY,
    transfer_id   UUID        NOT NULL REFERENCES transfers(id),
    account_id    UUID        NOT NULL REFERENCES accounts(id),
    amount        BIGINT      NOT NULL CHECK (amount <> 0),  -- negative = debit, positive = credit
    balance_after BIGINT      NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX entries_account_idx ON entries (account_id, id);
CREATE INDEX entries_transfer_idx ON entries (transfer_id);

CREATE TABLE idempotency_keys (
    client_id    UUID        NOT NULL REFERENCES api_clients(id),
    key          TEXT        NOT NULL,
    request_hash BYTEA       NOT NULL,
    transfer_id  UUID        REFERENCES transfers(id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (client_id, key)
);
CREATE INDEX idempotency_keys_created_idx ON idempotency_keys (created_at);

-- Transactional outbox: events are written in the SAME transaction as the business change,
-- then delivered asynchronously by the worker (at-least-once).
CREATE TABLE outbox_events (
    id              BIGSERIAL PRIMARY KEY,         -- global order, used as a cursor by GET /v1/events
    event_id        UUID        NOT NULL UNIQUE,   -- sent to receivers so they can deduplicate
    client_id       UUID        NOT NULL REFERENCES api_clients(id),
    type            TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'delivered', 'failed', 'no_endpoint')),
    attempts        INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ
);
-- Partial index: the worker only ever scans pending rows, so the index stays small.
CREATE INDEX outbox_due_idx ON outbox_events (next_attempt_at) WHERE status = 'pending';
CREATE INDEX outbox_client_idx ON outbox_events (client_id, id);
