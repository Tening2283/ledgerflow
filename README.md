# Ledgerflow

Une **API de portefeuille électronique et de paiements en partie double**, dans l'esprit d'un backend de Mobile Money
(Wave, Orange Money), écrite en Go avec PostgreSQL et Redis.
Le projet se concentre sur ce qui rend difficiles les backends qui manipulent de l'argent : rester juste sous forte
concurrence, rendre les retries sans danger, livrer les événements de façon fiable, et pouvoir exploiter le service en
production.

> 📘 Un guide d'apprentissage détaillé (concepts, choix de conception, questions d'entretien, feuille de route pour
> reconstruire le projet) se trouve dans [`docs/GUIDE_FR.md`](docs/GUIDE_FR.md).

## Points forts

| Problème | Solution dans Ledgerflow |
|---|---|
| L'argent ne doit jamais être créé ni perdu | **Registre en partie double** : chaque virement écrit 2 lignes immuables dont la somme fait 0. Les dépôts proviennent d'un compte `external` |
| Virements simultanés sur les mêmes comptes | Verrous de ligne (`SELECT … FOR UPDATE`) pris dans un **ordre global** : aucune mise à jour perdue, aucun deadlock. Une contrainte `CHECK` en base sert de seconde barrière |
| Le client réessaie après un timeout | **Clés d'idempotence** enregistrées *dans la même transaction* que le virement. Les doublons simultanés attendent sur l'index unique, puis rejouent le résultat |
| « L'argent a bougé mais l'événement est perdu » | **Transactional outbox** : l'événement est commité de façon atomique avec le virement |
| Notifier les marchands | Des workers réclament les événements par lots avec **`FOR UPDATE SKIP LOCKED`** et un bail. Webhooks **signés HMAC**, **backoff exponentiel avec jitter**, dead letter |
| Clients abusifs répartis sur plusieurs instances de l'API | **Token bucket distribué** dans Redis (script Lua atomique, horloge de Redis), en mode fail-open |
| Exploitation | Logs JSON avec request ID, métriques Prometheus, `/healthz` et `/readyz`, arrêt propre (graceful shutdown), migrations protégées par un advisory lock |

## Architecture

```
            ┌──────────────┐  HTTPS + clé API   ┌────────────────────────────┐
 marchand → │  Dashboard / │ ─────────────────→ │ api (N instances)          │
            │  tout client │                    │  auth → rate limit → handler
            └──────────────┘                    └──────┬──────────────┬──────┘
                                                       │ 1 transaction │ token bucket
                                                       ▼              ▼
                                   ┌──────────────────────────┐  ┌───────┐
                                   │ PostgreSQL               │  │ Redis │
                                   │ accounts, transfers,     │  └───────┘
                                   │ entries, idempotency_keys│
                                   │ outbox_events            │
                                   └────────────┬─────────────┘
                                                │ lots SKIP LOCKED
                                                ▼
                                   ┌──────────────────────────┐   POST signé     ┌──────────────────┐
                                   │ worker (N instances)     │ ───────────────→ │ webhook marchand │
                                   │ retries, backoff, DLQ    │  at-least-once   └──────────────────┘
                                   └──────────────────────────┘
```

Un virement correspond à **une seule** transaction PostgreSQL :

```
BEGIN
  INSERT idempotency_keys (client, key, hash) ON CONFLICT DO NOTHING   -- si la clé existe déjà : on rejoue
  SELECT … FROM accounts WHERE id IN (a, b) ORDER BY id FOR UPDATE     -- verrouillage dans un ordre global
  vérification de la devise et du solde
  INSERT transfers;  INSERT entries (-montant, +montant);  UPDATE des soldes
  INSERT outbox_events ('transfer.created')
  UPDATE idempotency_keys SET transfer_id = …
COMMIT
```

## Démarrage rapide

```bash
docker compose up --build
```

Ouvrez http://localhost:8080 et utilisez la clé API `lf_demo_key`. La stack lance Postgres, Redis, l'API, **2 workers**
et un receveur de webhooks de démonstration qui échoue 20 % du temps, pour qu'on voie les nouvelles tentatives se
produire.

Sans Docker (Go 1.24, avec un Postgres et un Redis en local) :

```bash
export DATABASE_URL=postgres://postgres@localhost:5432/ledgerflow?sslmode=disable ADMIN_TOKEN=admin BOOTSTRAP_API_KEY=lf_demo_key
go run ./cmd/api & go run ./cmd/worker
```

## API

Toutes les routes `/v1` exigent `Authorization: Bearer <clé_api>`. Les montants sont des entiers, exprimés en unités
mineures.

| Méthode et chemin | Description |
|---|---|
| `POST /admin/clients` | (token admin) crée un client → `api_key` (affichée une seule fois) et `webhook_secret` |
| `PUT /v1/webhook` | définit l'URL du webhook |
| `POST /v1/accounts` · `GET /v1/accounts?cursor=&limit=` · `GET /v1/accounts/{id}` | comptes (pagination par curseur) |
| `GET /v1/accounts/{id}/entries?cursor=` | relevé de compte |
| `POST /v1/deposits` · `POST /v1/withdrawals` · `POST /v1/transfers` | mouvements d'argent, **en-tête `Idempotency-Key` obligatoire** |
| `GET /v1/transfers/{id}` | un virement |
| `GET /v1/events?cursor=` | flux d'événements (alternative aux webhooks, par polling) |
| `GET /healthz` · `GET /readyz` · `GET /metrics` | exploitation |

```bash
curl -X POST localhost:8080/v1/transfers \
  -H 'Authorization: Bearer lf_demo_key' -H 'Idempotency-Key: 7f1c…' -H 'Content-Type: application/json' \
  -d '{"from_account_id":"…","to_account_id":"…","amount":15000,"reference":"loyer"}'
```

Les erreurs ont ce format : `{"error":{"code":"insufficient_funds","message":"…","request_id":"…"}}`.
Une requête rejouée renvoie le virement d'origine avec l'en-tête `Idempotent-Replayed: true`.

Les webhooks portent les en-têtes `Ledgerflow-Event-Id`, `Ledgerflow-Event-Type` et
`Ledgerflow-Signature: t=<unix>,v1=<HMAC-SHA256 en hexadécimal de "t.body">`.
La livraison est *at-least-once* : un même événement peut arriver plusieurs fois, donc les receveurs doivent
dédoublonner grâce à l'identifiant de l'événement (voir `cmd/webhook-receiver`).

## Tests

```bash
make test   # nécessite Postgres et Redis (ou lancez : docker compose up postgres redis)
```

Les tests d'intégration tournent sur un vrai PostgreSQL. Chaque test a son propre schéma, ce qui permet de les lancer
en parallèle. Ils comprennent notamment :
- 640 virements aléatoires simultanés, dans les deux sens. Le test vérifie que la somme des soldes reste à 0, qu'aucun
  solde n'est négatif, que `balance == SUM(entries)` et qu'il n'y a aucun deadlock ;
- 20 requêtes simultanées avec la même clé d'idempotence : exactement 1 virement créé et 19 rejoués ;
- 4 workers simultanés qui livrent 30 événements : chaque événement est livré exactement une fois dans ce tour ;
- un token bucket sollicité par 100 goroutines : exactement `burst` requêtes sont acceptées.

## Test de charge

```bash
make loadtest
```

Mesures sur une petite machine cloud, avec l'API, le worker, Postgres et le générateur de charge sur la même machine,
32 clients simultanés et 20 comptes (donc une forte contention sur les verrous) :
**environ 980 virements/s, p50 26 ms, p99 125 ms**. Les 9 857 événements ont tous été livrés, et l'invariant a été
vérifié après le test.

## Organisation du code

```
cmd/api               serveur HTTP et dashboard
cmd/worker            dispatcher de l'outbox et janitor (nettoyage périodique)
cmd/webhook-receiver  endpoint marchand de démo (vérification de signature, dédoublonnage)
cmd/loadtest          générateur de charge et vérification de l'invariant
internal/ledger       domaine : comptes, virements, idempotence (le cœur du projet)
internal/httpapi      routage, middlewares, handlers, conversion des erreurs en codes HTTP
internal/webhook      signature, dispatcher, backoff
internal/ratelimit    token bucket Redis
internal/db           pool de connexions et migrations embarquées
web/static            dashboard (JavaScript sans framework)
```

## Limites connues et pistes d'amélioration

- L'ordre des webhooks n'est pas garanti (workers en parallèle et retries). Les consommateurs doivent s'appuyer sur
  `created_at` ou relire l'état actuel.
- Un seul PostgreSQL primaire est à la fois la source de vérité et le goulot d'étranglement en écriture. Les étapes
  suivantes seraient des réplicas en lecture pour les relevés, puis un sharding par client.
- Les comptes très sollicités (un marchand qui reçoit tout) sont sérialisés sur un seul verrou de ligne. On pourrait
  atténuer ce problème avec des sous-comptes ou des règlements par lots.
- En production, les URLs de webhook devraient être protégées contre la SSRF (blocage des plages d'IP privées).
- Remplacer le polling par `LISTEN/NOTIFY` (latence plus faible), ou par un broker comme Kafka si plusieurs
  consommateurs ont besoin des événements.
