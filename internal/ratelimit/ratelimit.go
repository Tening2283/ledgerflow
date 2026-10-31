// Package ratelimit implements a distributed token bucket backed by Redis.
//
// Why Redis and not an in-memory map? With 3 API replicas behind a load balancer, an in-memory
// limiter would let each client do 3x the limit. Redis is the shared state all replicas agree on.
package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// The whole read-modify-write runs inside Redis as one Lua script, so it is atomic:
// two replicas cannot both read "1 token left" and both consume it.
// We use Redis' own clock (TIME) so clock skew between API servers does not matter.
var tokenBucket = redis.NewScript(`
local key   = KEYS[1]
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local t     = redis.call('TIME')
local now   = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local data   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now
end

tokens = math.min(burst, tokens + (math.max(0, now - ts) / 1000) * rate)

local allowed = 0
local retry_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_ms = math.ceil((1 - tokens) / rate * 1000)
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', key, math.ceil(burst / rate * 1000) + 1000)
return {allowed, math.floor(tokens), retry_ms}
`)

type Result struct {
	Allowed    bool
	Remaining  int
	Limit      int
	RetryAfter time.Duration
}

type Limiter interface {
	Allow(ctx context.Context, key string) (Result, error)
}

type RedisLimiter struct {
	rdb   *redis.Client
	rate  float64 // tokens added per second (sustained rate)
	burst int     // bucket capacity (allowed burst)
}

func NewRedis(rdb *redis.Client, ratePerSecond float64, burst int) *RedisLimiter {
	return &RedisLimiter{rdb: rdb, rate: ratePerSecond, burst: burst}
}

func (l *RedisLimiter) Allow(ctx context.Context, key string) (Result, error) {
	res, err := tokenBucket.Run(ctx, l.rdb, []string{"ratelimit:" + key}, l.rate, l.burst).Int64Slice()
	if err != nil {
		return Result{}, err
	}
	return Result{
		Allowed:    res[0] == 1,
		Remaining:  int(res[1]),
		Limit:      l.burst,
		RetryAfter: time.Duration(res[2]) * time.Millisecond,
	}, nil
}

// Noop allows everything (used when no Redis is configured, e.g. some tests).
type Noop struct{}

func (Noop) Allow(context.Context, string) (Result, error) {
	return Result{Allowed: true, Remaining: -1}, nil
}
