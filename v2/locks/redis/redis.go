package redis

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrRedisLockFailed = errors.New("redis lock: failed to acquire lock")
)

const (
	// DefaultLockRetryInterval is the default interval between lock retries
	DefaultLockRetryInterval = 100 * time.Millisecond
)

// Lock implements distributed lock using Redis
type Lock struct {
	// rclient is the Redis universal client
	rclient redis.UniversalClient

	// retries is the maximum number of retry attempts
	retries int

	// interval is the wait duration between retries
	interval time.Duration
}

// New creates Lock instance with an existing redis client
// Returns empty Lock if retries <= 0
func New(client redis.UniversalClient, retries int, interval time.Duration) Lock {
	if retries <= 0 {
		return Lock{}
	}
	if interval <= 0 {
		interval = DefaultLockRetryInterval
	}
	return Lock{
		rclient:  client,
		retries:  retries,
		interval: interval,
	}
}

func (r Lock) LockWithRetries(key string, unixTsToExpireNs int64) error {
	for i := 0; i <= r.retries; i++ {
		err := r.Lock(key, unixTsToExpireNs)
		if err == nil {
			// Lock acquired successfully
			return nil
		}

		time.Sleep(r.interval)
	}
	return ErrRedisLockFailed
}

func (r Lock) Lock(key string, unixTsToExpireNs int64) error {
	now := time.Now().UnixNano()
	expiration := time.Duration(unixTsToExpireNs + 1 - now)
	ctx := context.Background()

	success, err := r.rclient.SetNX(ctx, key, unixTsToExpireNs, expiration).Result()
	if err != nil {
		return err
	}

	if !success {
		v, err := r.rclient.Get(ctx, key).Result()
		if err != nil {
			return err
		}
		timeout, err := strconv.Atoi(v)
		if err != nil {
			return err
		}

		if timeout != 0 && now > int64(timeout) {
			newTimeout, err := r.rclient.GetSet(ctx, key, unixTsToExpireNs).Result()
			if err != nil {
				return err
			}

			curTimeout, err := strconv.Atoi(newTimeout)
			if err != nil {
				return err
			}

			if now > int64(curTimeout) {
				// success to acquire lock with get set
				// set the expiration of redis key
				if err := r.rclient.Expire(ctx, key, expiration).Err(); err != nil {
					return err
				}
				return nil
			}

			return ErrRedisLockFailed
		}

		return ErrRedisLockFailed
	}

	return nil
}
