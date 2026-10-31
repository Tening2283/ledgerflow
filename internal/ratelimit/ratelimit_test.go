package ratelimit

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func newRedis(t *testing.T) *redis.Client {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL not set")
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func TestBurstThenReject(t *testing.T) {
	l := NewRedis(newRedis(t), 1, 5) // 1 req/s sustained, burst of 5
	key := uuid.NewString()
	for i := 0; i < 5; i++ {
		r, err := l.Allow(context.Background(), key)
		if err != nil || !r.Allowed {
			t.Fatalf("request %d rejected: %v", i, err)
		}
	}
	r, err := l.Allow(context.Background(), key)
	if err != nil || r.Allowed {
		t.Fatalf("6th request should be rejected")
	}
	if r.RetryAfter <= 0 {
		t.Fatalf("missing retry-after")
	}
}

// Simulates many API replicas hitting the same bucket concurrently:
// exactly `burst` requests must pass, never more.
func TestConcurrentCallersShareOneBucket(t *testing.T) {
	l := NewRedis(newRedis(t), 0.001, 20)
	key := uuid.NewString()
	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r, err := l.Allow(context.Background(), key); err == nil && r.Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 20 {
		t.Fatalf("allowed=%d, want exactly 20", allowed.Load())
	}
}
