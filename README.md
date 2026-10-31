# Ledgerflow

A **double-entry wallet & payments API** (think a mobile-money backend), written in Go on PostgreSQL and Redis.
The focus is on what makes money-moving backends hard: correctness under concurrency, safe retries,
reliable event delivery and operability.

> 🇫🇷 A detailed learning guide (concepts, design decisions, interview Q&A, rebuild roadmap) is in
> [`docs/GUIDE_FR.md`](docs/GUIDE_FR.md).

## Highlights

| Problem | Solution in Ledgerflow |
|---|---|
| Money must never be created or lost | **Double-entry ledger**: every transfer = 2 immutable entries summing to 0; deposits come from an `external` account |
| Concurrent transfers on the same accounts | Row locks (`SELECT … FOR UPDATE`) taken in a **global order** → no lost updates, no deadlocks. DB `CHECK` constraint as a second line of defense |
| Client retries after a timeout | **Idempotency keys** stored in the *same transaction* as the transfer; concurrent duplicates block on the unique index then replay |
| "Money moved but the event was lost" | **Transactional outbox**: event row committed atomically with the transfer |
| Delivering events to merchants | Workers claim batches with **`FOR UPDATE SKIP LOCKED`** + lease; **HMAC-signed** webhooks, **exponential backoff with jitter**, dead letter |
| Abusive clients across many API replicas | **Distributed token bucket** in Redis (atomic Lua script, Redis clock), fail-open |
| Operating it | JSON logs with request IDs, Prometheus metrics, `/healthz` + `/readyz`, graceful shutdown, migrations guarded by an advisory lock |

## Architecture

```
            ┌──────────────┐  HTTPS + API key   ┌───────────────────────────┐
 merchant → │  Dashboard / │ ─────────────────→ │ api (N replicas)          │
            │  any client  │                    │  auth → rate limit → handler
            └──────────────┘                    └──────┬─────────────┬──────┘
                                                       │ 1 transaction│ token bucket
                                                       ▼             ▼
                                   ┌──────────────────────────┐  ┌───────┐
                                   │ PostgreSQL               │  │ Redis │
                                   │ accounts, transfers,     │  └───────┘
                                   │ entries, idempotency_keys│
                                   │ outbox_events            │
                                   └────────────┬─────────────┘
                                                │ SKIP LOCKED batches
                                                ▼
                                   ┌──────────────────────────┐   signed POST    ┌──────────────────┐
                                   │ worker (N replicas)      │ ───────────────→ │ merchant webhook │
                                   │ retries, backoff, DLQ    │   at-least-once  └──────────────────┘
                                   └──────────────────────────┘
```

A transfer is **one** PostgreSQL transaction:

```
BEGIN
  INSERT idempotency_keys (client, key, hash) ON CONFLICT DO NOTHING   -- replay if it already exists
  SELECT … FROM accounts WHERE id IN (a, b) ORDER BY id FOR UPDATE     -- lock in a global order
  check currency + balance
  INSERT transfers;  INSERT entries (-amount, +amount);  UPDATE balances
  INSERT outbox_events ('transfer.created')
  UPDATE idempotency_keys SET transfer_id = …
COMMIT
```

## Quick start

```bash
docker compose up --build
```

Open http://localhost:8080 and use the API key `lf_demo_key`. The stack runs Postgres, Redis, the API, **2 workers**
and a demo webhook receiver that fails 20% of the time, so you can watch retries happen.

Without Docker (Go 1.24, a local Postgres and Redis):

```bash
export DATABASE_URL=postgres://postgres@localhost:5432/ledgerflow?sslmode=disable ADMIN_TOKEN=admin BOOTSTRAP_API_KEY=lf_demo_key
go run ./cmd/api & go run ./cmd/worker
```

## API

All `/v1` routes require `Authorization: Bearer <api_key>`. Amounts are integers in minor units.

| Method & path | Description |
|---|---|
| `POST /admin/clients` | (admin token) create a client → `api_key` (shown once) + `webhook_secret` |
| `PUT /v1/webhook` | set the webhook URL |
| `POST /v1/accounts` · `GET /v1/accounts?cursor=&limit=` · `GET /v1/accounts/{id}` | accounts (cursor pagination) |
| `GET /v1/accounts/{id}/entries?cursor=` | account statement |
| `POST /v1/deposits` · `POST /v1/withdrawals` · `POST /v1/transfers` | money movements, **`Idempotency-Key` header required** |
| `GET /v1/transfers/{id}` | a transfer |
| `GET /v1/events?cursor=` | event feed (polling alternative to webhooks) |
| `GET /healthz` · `GET /readyz` · `GET /metrics` | ops |

```bash
curl -X POST localhost:8080/v1/transfers \
  -H 'Authorization: Bearer lf_demo_key' -H 'Idempotency-Key: 7f1c…' -H 'Content-Type: application/json' \
  -d '{"from_account_id":"…","to_account_id":"…","amount":15000,"reference":"rent"}'
```

Errors use the format `{"error":{"code":"insufficient_funds","message":"…","request_id":"…"}}`.
A replayed request returns the original transfer with the header `Idempotent-Replayed: true`.

Webhooks carry `Ledgerflow-Event-Id`, `Ledgerflow-Event-Type` and `Ledgerflow-Signature: t=<unix>,v1=<hex HMAC-SHA256 of "t.body">`.
Delivery is at-least-once, so receivers must deduplicate by event id (see `cmd/webhook-receiver`).

## Tests

```bash
make test   # needs Postgres + Redis (or run: docker compose up postgres redis)
```

The integration tests run against a real PostgreSQL. Each test gets its own schema, so tests run in parallel. They include:
- 640 random concurrent transfers in both directions, checking that the sum of balances is still 0, no balance is
  negative, `balance == SUM(entries)` and there are no deadlocks;
- 20 concurrent requests with the same idempotency key, checking that exactly 1 transfer is created and 19 are replayed;
- 4 concurrent workers delivering 30 events, checking that each event is delivered exactly once in that round;
- a token bucket hammered by 100 goroutines, checking that exactly `burst` requests are allowed.

## Load test

```bash
make loadtest
```

On a small cloud sandbox (API, worker, Postgres and the load generator all on the same machine, 32 concurrent clients
and 20 accounts, so lock contention is high), I measured about **980 transfers/s, p50 26 ms, p99 125 ms**.
All 9,857 events were delivered and the invariant was verified after the run.

## Project layout

```
cmd/api               HTTP server + dashboard
cmd/worker            outbox dispatcher + janitor
cmd/webhook-receiver  demo merchant endpoint (signature check, dedup)
cmd/loadtest          load generator + invariant check
internal/ledger       domain: accounts, transfers, idempotency (the core)
internal/httpapi      routing, middleware, handlers, error mapping
internal/webhook      signing, dispatcher, backoff
internal/ratelimit    Redis token bucket
internal/db           pool + embedded migrations
web/static            dashboard (vanilla JS)
```

## Known limitations / next steps

- Webhook order is not guaranteed (parallel workers + retries). Consumers should rely on `created_at` or fetch the current state.
- A single PostgreSQL primary is both the source of truth and the write bottleneck. Next steps would be read replicas
  for statements, and sharding by client.
- "Hot" accounts (one merchant receiving everything) serialize on one row lock. This could be mitigated with
  sub-accounts or batched settlement.
- Webhook URLs should be checked against SSRF (block private IP ranges) in production.
- Swap polling for `LISTEN/NOTIFY` (lower latency) or a broker (Kafka) when several consumers need the events.
