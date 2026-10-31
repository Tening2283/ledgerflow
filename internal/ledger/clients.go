package ledger

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// HashAPIKey: we only store sha256(key). A database leak does not leak usable keys.
// (A slow hash like bcrypt is unnecessary here: keys are 192-bit random, not human passwords.)
func HashAPIKey(key string) []byte {
	h := sha256.Sum256([]byte(key))
	return h[:]
}

func randomToken(prefix string) string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	return prefix + hex.EncodeToString(b)
}

// CreateClient registers a new API client. The raw API key is returned once and never stored.
func (s *Service) CreateClient(ctx context.Context, name, webhookURL string) (Client, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Client{}, "", ErrInvalidInput
	}
	apiKey := randomToken("lf_")
	c := Client{ID: uuid.Must(uuid.NewV7()), Name: name, WebhookURL: webhookURL, WebhookSecret: randomToken("whsec_")}
	err := s.pool.QueryRow(ctx, `
		INSERT INTO api_clients (id, name, api_key_hash, webhook_url, webhook_secret)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5) RETURNING created_at`,
		c.ID, c.Name, HashAPIKey(apiKey), webhookURL, c.WebhookSecret).Scan(&c.CreatedAt)
	return c, apiKey, err
}

// EnsureClient creates a client with a known key if it does not exist (demo bootstrap).
func (s *Service) EnsureClient(ctx context.Context, name, apiKey, webhookURL, webhookSecret string) error {
	if webhookSecret == "" {
		webhookSecret = randomToken("whsec_")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO api_clients (id, name, api_key_hash, webhook_url, webhook_secret)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5)
		ON CONFLICT (api_key_hash) DO UPDATE SET webhook_url = EXCLUDED.webhook_url, webhook_secret = EXCLUDED.webhook_secret`,
		uuid.Must(uuid.NewV7()), name, HashAPIKey(apiKey), webhookURL, webhookSecret)
	return err
}

func (s *Service) ClientByAPIKey(ctx context.Context, apiKey string) (Client, error) {
	var c Client
	var url *string
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, webhook_url, webhook_secret, created_at FROM api_clients WHERE api_key_hash = $1`,
		HashAPIKey(apiKey)).Scan(&c.ID, &c.Name, &url, &c.WebhookSecret, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Client{}, ErrNotFound
	}
	if url != nil {
		c.WebhookURL = *url
	}
	return c, err
}

func (s *Service) SetWebhookURL(ctx context.Context, clientID uuid.UUID, url string) error {
	_, err := s.pool.Exec(ctx, `UPDATE api_clients SET webhook_url = NULLIF($2, '') WHERE id = $1`, clientID, url)
	return err
}
