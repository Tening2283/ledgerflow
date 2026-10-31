# Guide Ledgerflow : comprendre, expliquer, reconstruire

Ce guide sert à **comprendre chaque décision** du projet, à pouvoir **la défendre en entretien** et à **refaire le
projet vous-même sans regarder le code**.

Méthode conseillée :
1. Lisez les parties 1 à 4 en entier.
2. Fermez le code et suivez la **feuille de route** (partie 7), étape par étape. Quand vous êtes bloqué, relisez la
   section du concept concerné, pas le code.
3. Entraînez-vous à répondre à voix haute aux **questions d'entretien** (partie 5).

---

## 1. Le pitch (2 minutes, à savoir par cœur)

> « J'ai construit Ledgerflow, le backend d'un portefeuille électronique type Wave ou Orange Money : des comptes,
> des dépôts, des retraits, des virements, et des webhooks pour notifier les marchands.
>
> Le sujet n'est pas le CRUD, c'est la **justesse sous concurrence et face aux pannes**. L'argent est stocké dans un
> **registre en partie double** : chaque mouvement écrit deux lignes dont la somme fait zéro, donc l'argent ne peut être
> ni créé ni perdu. Les virements concurrents sont protégés par des **verrous de ligne pris dans un ordre global**,
> ce qui empêche à la fois les mises à jour perdues et les deadlocks. Les retries des clients sont sûrs grâce à des
> **clés d'idempotence** enregistrées dans la même transaction que le virement.
>
> Pour les événements, j'utilise le **transactional outbox** : l'événement est écrit dans la même transaction que le
> virement, puis des workers le livrent en webhook **signé HMAC**, avec **backoff exponentiel** et dead letter. Les
> workers se partagent le travail grâce à `FOR UPDATE SKIP LOCKED`, donc on peut en lancer autant qu'on veut.
> Il y a aussi un **rate limiter distribué** dans Redis, des métriques Prometheus et un arrêt propre.
>
> Je l'ai validé avec des tests d'intégration sur un vrai Postgres, dont un test de 640 virements simultanés qui
> vérifie que la somme reste à zéro, et un test de charge à environ 1 000 virements par seconde. »

---

## 2. Le domaine : pourquoi ce projet est un projet « systèmes distribués »

Dès qu'il y a **plusieurs processus** (3 instances de l'API, 2 workers, une base, un Redis, un serveur marchand) qui
communiquent **par le réseau**, vous êtes dans un système distribué. Le réseau peut :
- **perdre** un message (la requête n'arrive jamais) ;
- perdre **la réponse** (le virement est fait, mais le client reçoit un timeout et croit que ça a échoué) ;
- **dupliquer** (le client réessaie) ;
- **retarder** et **réordonner**.

Et n'importe quel processus peut **planter à n'importe quelle ligne**.

Un résultat théorique important : sur un réseau non fiable, la livraison **exactly-once** est impossible. On a seulement le
choix entre *at-most-once* (on risque de perdre) et *at-least-once* (on risque de dupliquer). La solution
industrielle est la suivante :

> **at-least-once + idempotence = effet exactly-once.**

Tout le projet applique cette idée :
- côté API, le client réessaie (at-least-once) et la clé d'idempotence garantit un seul virement ;
- côté webhooks, le worker réessaie (at-least-once) et le marchand dédoublonne grâce à `event_id`.

---

## 3. Les concepts, un par un

Chaque concept suit le même plan : **Problème → Solution → Pourquoi ce choix → Alternatives → Pièges**.

### 3.1 Représenter l'argent

- **Problème** : `0.1 + 0.2 = 0.30000000000000004` en virgule flottante.
- **Solution** : un entier `BIGINT` en *unités mineures* (centimes pour l'EUR ; le XOF n'a pas de décimales). La devise
  est stockée à part (`CHAR(3)`, code ISO 4217).
- **Piège** : ne jamais additionner deux devises différentes. Le code refuse un virement entre un compte XOF et un
  compte EUR (`currency_mismatch`).

### 3.2 Registre en partie double (double-entry)

- **Problème** : avec un simple `UPDATE accounts SET balance = balance - 100`, si un bug fait l'un des deux
  `UPDATE` sans l'autre, de l'argent apparaît ou disparaît, et **on ne peut pas le prouver ni le retrouver**.
- **Solution** : chaque mouvement crée un `transfer` et **exactement deux `entries`** (lignes d'écriture) :
  `-100` sur le compte débité et `+100` sur le compte crédité. Les entries sont **immuables** : on ne les modifie ni ne
  les supprime jamais. Pour annuler, on fait un mouvement inverse.
- **Le compte `external`** : un dépôt ne « crée » pas d'argent, il le fait venir de l'extérieur (Mobile Money, carte…).
  Chaque client a un compte `external` par devise, dont le solde devient négatif. Ce solde négatif représente
  « l'argent que nous devons aux détenteurs de comptes ».
  Exemple : Awa dépose 50 000. On écrit `external −50 000` et `Awa +50 000`, et la somme fait 0.
- **Invariants vérifiables par SQL** (les tests les vérifient) :
  1. `SUM(balance)` par devise = 0 ;
  2. chaque transfer a 2 entries de somme nulle ;
  3. `accounts.balance = SUM(entries.amount)`. Le champ `balance` est un **cache** qui évite de sommer des millions
     de lignes à chaque lecture, et on peut le vérifier à tout moment ;
  4. aucun compte `user` n'est négatif.
- **`balance_after`** dans chaque entry : il permet d'afficher un relevé bancaire sans recalcul.
- **Défense en profondeur** : la contrainte `CHECK (kind = 'external' OR balance >= 0)` en base. Même si le code Go
  avait un bug, PostgreSQL refuserait un solde négatif.

### 3.3 Concurrence : verrous et deadlocks (le point le plus important)

**Problème 1, la mise à jour perdue (lost update).** Awa a 1 000. Deux virements de 800 arrivent en même temps :
```
T1: lit solde = 1000 → OK, 1000 ≥ 800      T2: lit solde = 1000 → OK, 1000 ≥ 800
T1: écrit 200                               T2: écrit 200
```
Résultat : 1 600 envoyés, le solde affiche 200. **De l'argent a été créé.**

**Solution : le verrou pessimiste.** `SELECT … FOR UPDATE` verrouille les lignes jusqu'à la fin de la transaction. T2
attend que T1 ait fini, puis relit le solde (200) et refuse le virement.

**Problème 2, le deadlock.** T1 fait A→B et verrouille A puis B. Au même moment, T2 fait B→A et verrouille B puis A.
T1 attend B (tenu par T2) et T2 attend A (tenu par T1) : blocage. PostgreSQL le détecte et tue l'une des deux
transactions, ce qui donne une erreur 500.

**Solution : l'ordre global des verrous.** On verrouille **toujours dans le même ordre** (par `id`), quel que soit le
sens du virement :
```sql
SELECT … FROM accounts WHERE id IN ($a, $b) ORDER BY id FOR UPDATE
```
Si tout le monde prend les verrous dans le même ordre, un cycle d'attente est impossible. Le test
`TestConcurrentRandomTransfersKeepInvariants` fait exactement des virements A→B et B→A en parallèle.

**Alternatives à connaître** :
- *Verrou optimiste* : une colonne `version`, puis `UPDATE … WHERE version = $v`. Si 0 ligne est modifiée, quelqu'un
  est passé avant et on réessaie. C'est bien quand les conflits sont rares, mauvais sur un compte très sollicité
  (beaucoup de retries).
- *Isolation `SERIALIZABLE`* : PostgreSQL détecte les conflits et annule une transaction, qu'il faut alors réessayer.
  C'est plus simple à raisonner, mais il faut une boucle de retry partout.
- *UPDATE atomique conditionnel* : `UPDATE accounts SET balance = balance - $1 WHERE id = $2 AND balance >= $1`.
  C'est très efficace pour un seul compte, mais moins lisible quand il faut aussi lire la devise, le type, etc.

**Le niveau d'isolation utilisé** : *READ COMMITTED*, le défaut de Postgres, suffit ici parce que les verrous de ligne
protègent les données critiques.

### 3.4 Idempotence

- **Problème** : le client envoie un virement et le réseau coupe **après** le commit, mais **avant** qu'il reçoive la
  réponse. Il réessaie. Sans protection, Awa est débitée deux fois.
- **Solution** : le client génère une clé unique par *intention* (un UUID) et l'envoie dans l'en-tête
  `Idempotency-Key`. Le serveur mémorise `(client_id, key) → transfer_id`. Si la même clé revient, il renvoie le
  **même** résultat sans rien refaire, avec l'en-tête `Idempotent-Replayed: true`.
- **Le détail qui fait la différence** : la clé est enregistrée **dans la même transaction** que le virement.
  - Si on enregistrait la clé *après* le commit, un crash entre les deux laisserait un virement fait sans clé
    enregistrée, et le retry referait le virement.
  - Ici, les deux sont commités ensemble, ou rien du tout.
- **Deux requêtes identiques en même temps** : la première fait `INSERT … ON CONFLICT DO NOTHING` et prend la ligne.
  La seconde **bloque** sur l'index unique jusqu'à ce que la première se termine.
  - Si la première commite, la seconde voit le conflit et rejoue le résultat.
  - Si la première échoue (rollback), la seconde continue comme si elle était la première.

  Postgres fait donc la coordination gratuitement. C'est ce que vérifie le test `TestConcurrentSameIdempotencyKey`
  (20 requêtes → 1 virement et 19 replays).
- **Le hash de requête** : on stocke `sha256(méthode + chemin + body)`. Une même clé avec un autre contenu donne une
  erreur 422 `idempotency_mismatch`. C'est un bug côté client, et on ne doit surtout pas renvoyer silencieusement un
  autre virement.
- **Choix assumé** : si l'opération échoue (fonds insuffisants), la transaction est annulée et **la clé n'est pas
  consommée**. Le client peut approvisionner le compte et réessayer avec la même clé. Stripe, lui, mémorise aussi
  les erreurs. Les deux choix se défendent.
- **Durée de vie** : le *janitor* du worker purge les clés de plus de 24 h, pour que la table ne grossisse pas
  indéfiniment. Après 24 h, la même clé crée un nouveau virement : c'est documenté, comme chez Stripe.

### 3.5 Transactional outbox

- **Problème** : après un virement, il faut notifier le marchand. Si on appelle son webhook **dans** la transaction,
  on la garde ouverte (avec les verrous) pendant un appel réseau lent, et si le commit échoue après l'appel, on a
  annoncé un virement qui n'existe pas. Si on l'appelle **après** le commit et que le processus plante juste avant,
  l'événement est **perdu**. C'est le problème du *dual write* : on ne peut pas écrire de façon atomique dans deux
  systèmes différents.
- **Solution** : on écrit l'événement dans une table `outbox_events` **dans la même transaction** que le virement.
  Un processus séparé (le worker) lit cette table et livre les événements. Le virement et son événement existent tous
  les deux, ou aucun.
- **Bonus** : la table sert aussi de journal d'événements, lisible avec `GET /v1/events?cursor=N` pour les clients
  qui préfèrent interroger l'API (polling) plutôt que recevoir des webhooks.

### 3.6 Le worker : `FOR UPDATE SKIP LOCKED` et bail

- **Problème** : on veut plusieurs workers (pour la tolérance aux pannes et le débit) sans qu'ils livrent deux fois le
  même événement en même temps, et sans coordinateur central.
- **Solution** :
  ```sql
  WITH due AS (SELECT id FROM outbox_events WHERE status='pending' AND next_attempt_at <= now()
               ORDER BY next_attempt_at LIMIT 50 FOR UPDATE SKIP LOCKED)
  UPDATE outbox_events SET attempts = attempts + 1, next_attempt_at = now() + 30s FROM due …
  ```
  `SKIP LOCKED` signifie : « les lignes déjà verrouillées par un autre worker, saute-les ». Chaque worker obtient donc
  un lot **différent**, sans attendre les autres. Postgres devient une file de messages.
- **Le bail (lease, ou visibility timeout comme dans Amazon SQS)** : en réclamant un événement, on repousse
  `next_attempt_at` de 30 s. Si le worker plante en pleine livraison, l'événement redevient visible 30 s plus tard
  et un autre worker le reprend. **Rien n'est perdu**, mais il peut être livré deux fois : c'est at-least-once.
- **Concurrence bornée** : chaque lot est livré par au plus `WORKER_CONCURRENCY` goroutines, grâce à un sémaphore
  (un canal bufferisé).
- **Index partiel** `WHERE status = 'pending'` : l'index ne contient que les événements à livrer, il reste petit même
  avec des millions d'événements déjà livrés.

### 3.7 Webhooks : signature, retries, dead letter

- **Signature HMAC** : le marchand doit être sûr que le message vient de nous et n'a pas été modifié. On envoie
  `Ledgerflow-Signature: t=<timestamp>,v1=<HMAC-SHA256(secret, "t.body")>`.
  - Le **timestamp est signé** : le marchand rejette les messages trop vieux (plus de 5 min). Un attaquant qui a
    intercepté un message ne peut pas le rejouer plus tard (*replay attack*).
  - On compare avec `hmac.Equal`, une **comparaison à temps constant**. Avec `==`, la comparaison s'arrête au premier
    octet différent, et un attaquant pourrait deviner la signature octet par octet en mesurant le temps de réponse
    (*timing attack*).
- **Retries et backoff exponentiel** : en cas d'échec (timeout ou code non-2xx), on réessaie après 2 s, 4 s, 8 s…
  avec un plafond à 1 h.
- **Jitter** (aléa) : le délai est tiré au hasard entre d/2 et d. Sans jitter, si le serveur du marchand tombe,
  10 000 événements échouent en même temps, réessaient **à la même seconde** et le refont tomber : c'est le
  *thundering herd* (troupeau qui charge).
- **Dead letter** : après `WEBHOOK_MAX_ATTEMPTS` tentatives, le statut passe à `failed`. On ne réessaie plus, mais la
  ligne reste pour analyse ou relance manuelle.
- **Timeout HTTP obligatoire** : `http.DefaultClient` n'a **aucun** timeout. Un marchand lent bloquerait un worker
  pour toujours.
- **Côté marchand** (voir `cmd/webhook-receiver`) : vérifier la signature, **dédoublonner par `event_id`** (la livraison
  est at-least-once) et répondre 2xx vite (faire le traitement lourd après).
- **Ordre non garanti** : avec plusieurs workers et des retries, l'événement #12 peut arriver avant le #11. C'est une
  limite à connaître et à assumer.

### 3.8 Rate limiting distribué (token bucket)

- **Problème** : un client buggé envoie 10 000 requêtes par seconde et dégrade le service pour tout le monde.
- **Token bucket** : un seau de capacité `burst` (100) se remplit à `rate` jetons par seconde (50). Chaque requête
  consomme un jeton, et si le seau est vide, on répond 429 avec l'en-tête `Retry-After`. Cela autorise de courtes
  rafales tout en limitant le débit moyen.
- **Pourquoi Redis** : avec 3 instances de l'API, un compteur en mémoire laisserait passer 3 fois la limite. Redis est
  l'état partagé.
- **Pourquoi un script Lua** : faire « lire, calculer, écrire » en 3 commandes séparées crée une *race condition*
  (deux instances lisent « 1 jeton restant » et le consomment toutes les deux). Un script Lua s'exécute **de façon
  atomique** dans Redis. Le test `TestConcurrentCallersShareOneBucket` lance 100 goroutines et vérifie qu'exactement
  20 requêtes passent.
- **L'horloge de Redis (`TIME`)** : les serveurs de l'API peuvent avoir des horloges décalées (*clock skew*). En
  utilisant l'horloge de Redis, tout le monde partage la même référence.
- **Fail-open** : si Redis tombe, on **laisse passer** les requêtes (avec un log et une métrique) plutôt que de couper
  tout le service. C'est un choix métier : un système bancaire très sensible pourrait choisir *fail-closed*.
- **Alternatives** : fenêtre fixe (simple, mais autorise un double pic à la frontière de deux fenêtres), fenêtre
  glissante, leaky bucket.

### 3.9 Pagination par curseur et UUIDv7

- `OFFSET 100000` oblige Postgres à lire puis jeter 100 000 lignes : c'est de plus en plus lent. Et si une ligne est
  insérée entre deux pages, on voit des doublons ou des trous.
- **Pagination par curseur (keyset)** : `WHERE id > $cursor ORDER BY id LIMIT 20`. L'index va directement au bon
  endroit, en O(log n), et c'est stable.
- **UUIDv7** : les 48 premiers bits sont un horodatage, donc les identifiants sont **triables par date de création**.
  Avantages : le curseur suit l'ordre chronologique, et les insertions vont à la fin de l'index B-tree (bonne
  localité). Avec des UUIDv4 aléatoires, chaque insertion tombe à un endroit au hasard dans l'index, ce qui le
  fragmente. Contrairement à un identifiant auto-incrémenté, un UUIDv7 ne révèle pas le volume d'activité et peut être
  généré sans passer par la base.

### 3.10 Multi-tenant et sécurité

- **Isolation** : **toutes** les requêtes SQL filtrent par `client_id`. Un client qui devine l'UUID d'un compte d'un
  autre client reçoit une 404, pas une 403, pour ne même pas révéler que le compte existe. C'est testé dans
  `TestTenantIsolation`.
- **Clés API** : on ne stocke que `sha256(clé)`. Si la base fuit, les clés ne sont pas utilisables. Pas besoin de
  bcrypt : bcrypt sert à ralentir le cassage de mots de passe *humains* (faibles), alors que nos clés sont 192 bits
  aléatoires, impossibles à deviner par force brute. La clé n'est affichée **qu'une fois**, à sa création.
- **Token admin** comparé à temps constant (`subtle.ConstantTimeCompare`).
- **Limite de taille du body** (1 Mo) et **JSON strict** (`DisallowUnknownFields`) : une faute de frappe comme
  `"ammount"` donne une erreur 400 au lieu d'être silencieusement ignorée.
- **Erreurs 500** : on logue le détail avec le `request_id`, mais on ne renvoie jamais le SQL ou la stack au client.

### 3.11 Observabilité

- **Logs JSON structurés** (`log/slog`) : une ligne JSON par événement, exploitable par Loki, ELK ou Datadog.
- **Request ID** : généré, ou repris de l'en-tête `X-Request-Id` du client. Il est présent dans chaque log et dans
  les réponses d'erreur. Un client signale un problème en donnant son `request_id`, et vous retrouvez toute
  l'histoire de la requête.
- **Métriques Prometheus** (`/metrics`) :
  - `http_requests_total{route,method,code}` et l'histogramme `http_request_duration_seconds` : trafic, erreurs,
    latence (les « golden signals ») ;
  - `ledger_transfers_total{kind,result}` : métriques métier ;
  - `outbox_pending_events` : **la** métrique d'alerte du worker. Si elle monte, les workers ne suivent plus.
- **Piège de cardinalité** : le label `route` utilise le *pattern* (`GET /v1/accounts/{id}`), jamais le chemin brut.
  Avec le chemin brut, chaque UUID créerait une série temporelle, soit des millions de séries, et Prometheus explose.

### 3.12 Arrêt propre, health checks, timeouts

- **`/healthz`** (liveness) : « le processus est vivant ». Kubernetes le redémarre si ça échoue.
- **`/readyz`** (readiness) : « je peux recevoir du trafic » (la base répond). Le load balancer retire l'instance du
  pool si ça échoue.
- **Graceful shutdown** : à la réception de SIGTERM (envoyé par Docker ou Kubernetes) :
  1. `/readyz` passe à 503 (le load balancer arrête d'envoyer du trafic) ;
  2. `srv.Shutdown()` refuse les nouvelles connexions et **attend la fin des requêtes en cours**, avec un timeout ;
  3. fermeture de la base et de Redis.

  Résultat : un déploiement ne coupe aucun virement en plein milieu. Le worker, lui, finit son lot en cours. Le
  résultat d'une livraison est enregistré avec un contexte qui n'est pas annulé
  (`context.WithoutCancel`), pour ne pas perdre l'information.
- **Timeouts du serveur HTTP** : le `http.Server` de Go n'a aucun timeout par défaut. Des clients lents (attaque
  *slowloris*) pourraient occuper toutes les connexions.

### 3.13 Migrations

- Les fichiers SQL sont **embarqués dans le binaire** (`go:embed`) et appliqués dans l'ordre, chacun dans une
  transaction, avec un suivi dans la table `schema_migrations`.
- **`pg_advisory_lock`** : si 3 instances démarrent en même temps, une seule applique les migrations et les autres
  attendent. C'est un verrou distribué gratuit fourni par Postgres.

### 3.14 Tests

- Des **tests d'intégration sur un vrai PostgreSQL**, sans mock de la base. Les bugs de concurrence n'existent
  *que* dans la vraie base : un mock ne verrouille rien.
- **Un schéma Postgres par test** (`search_path`) : les tests sont isolés et peuvent tourner en parallèle.
- Le détecteur de *data races* de Go (`go test -race`) est activé.
- Les tests vérifient les **invariants** directement en SQL, pas seulement les réponses de l'API.
- La CI GitHub Actions lance Postgres et Redis comme services, puis exécute `gofmt`, `vet`, les tests et le build Docker.

---

## 4. Le trajet d'un virement, de A à Z

1. Le client envoie `POST /v1/transfers` avec `Authorization: Bearer lf_…` et `Idempotency-Key: 7f1c…`.
2. Le middleware `withRequestID` attribue un ID, puis `withRecover` intercepte une éventuelle panique, puis
   `withObservability` démarre le chronomètre.
3. Le mux route la requête vers `POST /v1/transfers`, puis `authed` hashe la clé, retrouve le client et consulte le
   rate limiter Redis.
4. Le handler lit le body (1 Mo max), décode le JSON en mode strict et calcule le hash de la requête.
5. `ledger.Transfer` exécute `withIdempotency` :
   `BEGIN` → `INSERT idempotency_keys ON CONFLICT DO NOTHING` (s'il y a conflit, on rejoue) → `move()` :
   verrouillage ordonné → vérifications → `INSERT transfers` → 2 × (`UPDATE balance`, `INSERT entry`) →
   `INSERT outbox_events` → `UPDATE idempotency_keys SET transfer_id` → `COMMIT`.
6. Réponse 201, puis le middleware logue la requête et met à jour les métriques.
7. Moins d'une demi-seconde plus tard, un worker réclame l'événement (`SKIP LOCKED`), le signe, le POSTe chez le
   marchand et le marque `delivered`. En cas d'échec, il programme un nouvel essai avec backoff.

---

## 5. Questions d'entretien (avec réponses)

**1. Pourquoi PostgreSQL plutôt que MongoDB ?**
Il faut des transactions ACID sur plusieurs lignes et plusieurs tables, des contraintes (CHECK, UNIQUE, FK) et des
verrous de ligne. C'est le cœur du problème, et Postgres le fait parfaitement.

**2. Comment garantir qu'un compte ne passe jamais en négatif ?**
Avec trois niveaux : la vérification dans le code sous verrou `FOR UPDATE`, la contrainte `CHECK` en base, et un test
de concurrence qui vérifie l'invariant.

**3. Qu'est-ce qu'un deadlock et comment l'évitez-vous ?**
C'est une attente circulaire entre transactions. Je l'évite en verrouillant toujours dans un ordre global (`ORDER BY id`).

**4. Pourquoi pas de verrou optimiste ?**
Sur un compte très sollicité, il provoquerait beaucoup de conflits et de retries. Le verrou pessimiste fait la queue
proprement. Le verrou optimiste est meilleur quand les conflits sont rares.

**5. Que se passe-t-il si le client réessaie un virement ?**
Grâce à la même clé d'idempotence, la même transaction est renvoyée avec `Idempotent-Replayed: true`, et l'argent
n'est déplacé qu'une fois.

**6. Et si les deux requêtes arrivent exactement en même temps ?**
La seconde bloque sur l'index unique de `idempotency_keys` jusqu'au commit de la première, puis rejoue le résultat.

**7. Pourquoi enregistrer la clé d'idempotence dans la même transaction ?**
Sinon, un crash entre le virement et l'enregistrement de la clé permettrait un double virement au retry.

**8. Qu'est-ce que le transactional outbox ? Quel problème résout-il ?**
Le *dual write* : on ne peut pas écrire de façon atomique dans la base **et** envoyer un message. On écrit donc le
message dans la base, dans la même transaction, et un processus séparé le publie.

**9. Exactly-once, c'est possible ?**
Pas pour la livraison sur un réseau non fiable. On fait at-least-once plus de l'idempotence côté consommateur, ce qui
donne un effet exactly-once.

**10. Comment plusieurs workers se partagent-ils la file sans doublon ?**
Avec `FOR UPDATE SKIP LOCKED`, plus un bail (`next_attempt_at` repoussé) pour reprendre les événements d'un worker
qui a planté.

**11. Pourquoi le jitter dans le backoff ?**
Pour éviter que tous les retries tombent en même temps et refassent tomber le serveur (*thundering herd*).

**12. Comment le marchand sait-il que le webhook vient de vous ?**
Par une signature HMAC-SHA256 avec un secret partagé. Le timestamp signé empêche le rejeu, et la comparaison se fait
à temps constant.

**13. Pourquoi Redis pour le rate limit ? Et si Redis tombe ?**
Redis fournit un état partagé entre les instances. Le script Lua rend l'opération atomique. Si Redis tombe, on passe
en fail-open, ce qui est un choix métier.

**14. Pourquoi des UUIDv7 ?**
Ils sont triables par le temps, ce qui donne une bonne localité d'index et permet la pagination par curseur. Ils ne
révèlent pas le volume d'activité et peuvent être générés côté application.

**15. Pourquoi ne pas stocker seulement les entries et calculer le solde à la volée ?**
Ce serait trop lent avec des millions de lignes. Le solde est un cache mis à jour dans la même transaction et
vérifiable à tout moment (`balance = SUM(entries)`).

**16. Comment l'API monte-t-elle en charge ?**
L'API est *stateless* : on ajoute des instances derrière un load balancer, et l'état est dans Postgres et Redis. Les
workers montent en charge de la même manière. La limite, c'est Postgres en écriture.

**17. Et quand Postgres devient le goulot ?**
D'abord : index, pool de connexions (PgBouncer), réplicas en lecture pour les relevés. Ensuite : sharding par client
(les virements restent à l'intérieur d'un client, donc à l'intérieur d'un shard). Les virements entre shards
demanderaient une *saga* ou un *2PC*.

**18. Qu'est-ce qu'un « hot account » ?**
Un compte qui reçoit énormément de virements (le compte d'un gros marchand) : tous les virements s'alignent sur un
seul verrou de ligne. Solutions : des sous-comptes, ou des écritures en lot.

**19. Différence entre liveness et readiness ?**
Liveness : « suis-je vivant ? » (sinon on redémarre le processus). Readiness : « puis-je servir ? » (sinon on ne
m'envoie plus de trafic).

**20. Que fait votre API pendant un déploiement ?**
Elle passe en readiness 503, termine les requêtes en cours, puis s'arrête. Aucune requête n'est coupée.

**21. Pourquoi des tests sur une vraie base et pas des mocks ?**
Les bugs de concurrence, les contraintes et les verrous n'existent que dans la vraie base.

**22. Comment déboguer une erreur signalée par un client ?**
Il donne le `request_id` présent dans l'erreur, et je filtre les logs JSON sur cet ID.

**23. Que surveillez-vous en production ?**
Le taux d'erreurs 5xx, la latence p99, `outbox_pending_events` (le retard des workers), les décisions du rate
limiter, et le nombre de webhooks en dead letter.

**24. Pourquoi l'ordre des webhooks n'est-il pas garanti ? Comment le garantir ?**
À cause du parallélisme et des retries. Pour le garantir : un seul consommateur par partition (par compte), comme une
partition Kafka, au prix d'un débit plus faible.

**25. Quelle faille de sécurité reste-t-il sur les webhooks ?**
La SSRF : un client pourrait configurer une URL interne (`http://169.254.169.254/…`). En production, il faut résoudre
le DNS et bloquer les plages d'IP privées.

**26. Pourquoi Go ?**
Concurrence native (goroutines, canaux), binaire statique facile à déployer, bibliothèque standard très complète
(HTTP, crypto, slog), et c'est le langage de Docker, Kubernetes et de beaucoup d'infrastructures.

---

## 6. Limites et améliorations possibles (montrez que vous les connaissez)

- **LISTEN/NOTIFY** au lieu du polling toutes les 500 ms, pour réduire la latence des webhooks.
- **Kafka ou NATS** comme bus d'événements si plusieurs services doivent consommer les événements (le worker publie
  l'outbox vers Kafka : c'est le pattern classique).
- **Partitionnement** de `entries` et `outbox_events` par mois, avec archivage des vieux événements livrés.
- **Cache** du client authentifié (quelques secondes) pour éviter une requête SQL par appel.
- **Protection SSRF** des URLs de webhook.
- **Tracing distribué** (OpenTelemetry) pour suivre une requête à travers l'API, la base et le worker.
- **Rotation** des clés API et des secrets de webhook.
- **Réconciliation** : un job nocturne qui revérifie tous les invariants et alerte en cas d'écart.

---

## 7. Feuille de route pour reconstruire le projet (sans regarder le code)

Chaque étape a un **critère de réussite**. Ne passez à la suivante que quand il est atteint. Commitez à chaque étape :
un historique Git propre et progressif est très bien vu.

| # | Étape | Critère de réussite |
|---|---|---|
| 1 | **Squelette** : `go mod init`, `cmd/api/main.go` avec `GET /healthz`, configuration par variables d'environnement, logger `slog` JSON | `curl /healthz` renvoie `{"status":"ok"}` |
| 2 | **Base de données** : connexion pgx, fichier `001_init.sql` embarqué, migrateur avec table `schema_migrations` et `pg_advisory_lock` | lancer l'API 2 fois crée les tables une seule fois |
| 3 | **Clients et auth** : `POST /admin/clients` (token admin), clé API aléatoire hashée en SHA-256, middleware Bearer | une mauvaise clé donne 401 |
| 4 | **Comptes** : création (devise validée), lecture, liste paginée par curseur (UUIDv7), filtrée par `client_id` | un autre client reçoit 404 |
| 5 | **Virements naïfs** : transaction, `transfers` + 2 `entries` + mise à jour des soldes ; dépôts et retraits via le compte `external` | écrivez le test de concurrence : il doit **échouer** ou faire des deadlocks avec une version naïve. C'est formateur ! |
| 6 | **Verrous** : `FOR UPDATE` avec `ORDER BY id`, contrainte `CHECK` | le test de 640 virements concurrents passe, et la somme vaut 0 |
| 7 | **Idempotence** : table, `ON CONFLICT DO NOTHING` dans la même transaction, hash de requête | test : 20 requêtes simultanées avec la même clé donnent 1 virement |
| 8 | **Outbox et worker** : insertion de l'événement dans la transaction ; worker avec `SKIP LOCKED`, bail, signature HMAC, backoff avec jitter, dead letter ; receveur de démo | coupez le receveur, faites des virements, relancez-le : tous les événements arrivent |
| 9 | **Rate limit et ops** : token bucket Redis en Lua, request ID, métriques Prometheus, `/readyz`, graceful shutdown, timeouts | 429 au-delà du burst ; `kill -TERM` pendant le test de charge ne donne aucune erreur 5xx |
| 10 | **Livraison** : Dockerfile multi-stage, docker-compose, CI GitHub Actions, dashboard HTML/JS, test de charge, README | `docker compose up` suffit pour tout lancer ; la CI est verte |

Conseils :
- À l'étape 5, **faites volontairement la version naïve** et regardez le test de concurrence casser. Vous comprendrez
  vraiment pourquoi on verrouille, et vous pourrez raconter cette expérience en entretien.
- Écrivez le README **vous-même**, avec vos mots et vos propres mesures de performance.
- Vous pouvez changer le domaine (tontine, cagnotte, points de fidélité…) : les mêmes mécanismes s'appliquent, et le
  projet sera encore plus le vôtre.

---

## 8. Glossaire

| Terme | Définition simple |
|---|---|
| ACID | Atomicité, Cohérence, Isolation, Durabilité : les garanties d'une transaction |
| At-least-once | Le message arrive au moins une fois, parfois plusieurs |
| Backoff | Attendre de plus en plus longtemps entre deux essais |
| Dead letter (DLQ) | L'endroit où l'on range ce qui a échoué trop de fois |
| Deadlock | Deux transactions qui s'attendent mutuellement pour toujours |
| Double-entry | Chaque mouvement = un débit + un crédit de même montant |
| Dual write | Écrire dans deux systèmes sans pouvoir le faire de façon atomique |
| Fail-open / fail-closed | Quand une dépendance tombe : tout laisser passer, ou tout bloquer |
| HMAC | Signature d'un message avec une clé secrète partagée |
| Idempotent | Faire l'opération 1 fois ou N fois donne le même résultat |
| Invariant | Une propriété qui doit **toujours** être vraie (somme = 0) |
| Jitter | Un aléa ajouté aux délais pour désynchroniser les clients |
| Lease (bail) | Une réservation temporaire qui expire si son détenteur disparaît |
| Lost update | Une écriture concurrente qui en écrase une autre |
| Outbox | Une table d'événements écrite dans la même transaction que les données |
| p99 | La latence sous laquelle se trouvent 99 % des requêtes |
| SKIP LOCKED | « Ignore les lignes déjà verrouillées par quelqu'un d'autre » |
| Stateless | Un service qui ne garde aucun état en mémoire entre deux requêtes |
| Thundering herd | Tous les clients qui réessaient en même temps |
| Token bucket | Un seau de jetons qui se remplit à vitesse fixe ; une requête consomme un jeton |
