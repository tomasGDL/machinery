package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	persistentiface "github.com/RichardKnop/machinery/v2/persistent/iface"
	storageiface "github.com/RichardKnop/machinery/v2/persistent/storage/iface"
)

const (
	KeyPrefix     = "machinery:persistent:"
	EntriesKey    = "entries"
	PendingKey    = "pending"
	ScheduledKey  = "scheduled"
	QueueIndexKey = "queue_index"
)

// Store Redis 存储实现
type Store struct {
	client    redis.UniversalClient
	keyPrefix string
}

// New 创建 Redis 存储
func New(client redis.UniversalClient) storageiface.PersistentStorage {
	return &Store{
		client:    client,
		keyPrefix: KeyPrefix,
	}
}

// Store 存储条目
func (s *Store) Store(ctx context.Context, entry *persistentiface.PersistentEntry) error {
	key := s.keyPrefix + EntriesKey + ":" + entry.ID

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal error for %s: %w", entry.ID, err)
	}

	pipe := s.client.Pipeline()

	pipe.HSet(ctx, s.keyPrefix+EntriesKey, key, data)

	score := float64(entry.Priority)*1e12 + float64(^uint64(0)-uint64(entry.CreatedAt.UnixNano()))/1e6
	pipe.ZAdd(ctx, s.keyPrefix+PendingKey, redis.Z{
		Score:  score,
		Member: entry.ID,
	})

	if entry.Queue != "" {
		pipe.SAdd(ctx, s.keyPrefix+QueueIndexKey+":"+entry.Queue, entry.ID)
	}

	_, err = pipe.Exec(ctx)
	return err
}

// GetForSchedule 获取待调度条目
func (s *Store) GetForSchedule(ctx context.Context, queue string, batchSize int) ([]*persistentiface.PersistentEntry, error) {
	var ids []string
	var err error

	if queue != "" {
		queueIds, err := s.client.SMembers(ctx, s.keyPrefix+QueueIndexKey+":"+queue).Result()
		if err != nil {
			return nil, err
		}

		for _, id := range queueIds {
			score, err := s.client.ZScore(ctx, s.keyPrefix+PendingKey, id).Result()
			if err == nil && score > 0 {
				ids = append(ids, id)
			}
		}

		if len(ids) > batchSize {
			ids = ids[:batchSize]
		}
	} else {
		ids, err = s.client.ZRevRange(ctx, s.keyPrefix+PendingKey, 0, int64(batchSize-1)).Result()
		if err != nil {
			return nil, err
		}
	}

	if len(ids) == 0 {
		return nil, nil
	}

	return s.getBatch(ctx, ids)
}

// getBatch 批量获取条目（内部方法）
func (s *Store) getBatch(ctx context.Context, ids []string) ([]*persistentiface.PersistentEntry, error) {
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.keyPrefix + EntriesKey + ":" + id
	}

	results, err := s.client.HMGet(ctx, s.keyPrefix+EntriesKey, keys...).Result()
	if err != nil {
		return nil, err
	}

	entries := make([]*persistentiface.PersistentEntry, 0, len(results))
	for _, result := range results {
		if result == nil {
			continue
		}

		var entry persistentiface.PersistentEntry
		if err := json.Unmarshal([]byte(result.(string)), &entry); err != nil {
			continue
		}
		entries = append(entries, &entry)
	}

	return entries, nil
}

// MarkScheduled 标记为已调度
func (s *Store) MarkScheduled(ctx context.Context, id string) error {
	pipe := s.client.Pipeline()

	pipe.ZRem(ctx, s.keyPrefix+PendingKey, id)
	pipe.ZAdd(ctx, s.keyPrefix+ScheduledKey, redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: id,
	})

	_, err := pipe.Exec(ctx)
	return err
}

// MarkDone 标记为已完成并删除
func (s *Store) MarkDone(ctx context.Context, id string) error {
	pipe := s.client.Pipeline()

	key := s.keyPrefix + EntriesKey + ":" + id

	pipe.HDel(ctx, s.keyPrefix+EntriesKey, key)
	pipe.ZRem(ctx, s.keyPrefix+PendingKey, id)
	pipe.ZRem(ctx, s.keyPrefix+ScheduledKey, id)

	_, err := pipe.Exec(ctx)
	return err
}

// MarkFailed 标记为失败（增加 scheduled_count）
func (s *Store) MarkFailed(ctx context.Context, id string, errMsg string) error {
	key := s.keyPrefix + EntriesKey + ":" + id

	result, err := s.client.HGet(ctx, s.keyPrefix+EntriesKey, key).Result()
	if err != nil {
		return err
	}

	var entry persistentiface.PersistentEntry
	if err := json.Unmarshal([]byte(result), &entry); err != nil {
		return err
	}

	entry.ScheduledCount++
	entry.LastError = errMsg
	entry.LastAttempt = func(t time.Time) *time.Time { return &t }(time.Now())
	entry.UpdatedAt = time.Now()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	pipe := s.client.Pipeline()

	pipe.HSet(ctx, s.keyPrefix+EntriesKey, key, data)
	pipe.ZRem(ctx, s.keyPrefix+ScheduledKey, id)
	pipe.ZAdd(ctx, s.keyPrefix+PendingKey, redis.Z{
		Score:  float64(entry.Priority)*1e12 + float64(^uint64(0)-uint64(entry.CreatedAt.UnixNano()))/1e6,
		Member: id,
	})

	_, err = pipe.Exec(ctx)
	return err
}

// CountPending 获取待处理数量
func (s *Store) CountPending(ctx context.Context, queue string) (int64, error) {
	if queue != "" {
		return s.client.SCard(ctx, s.keyPrefix+QueueIndexKey+":"+queue).Result()
	}
	return s.client.ZCard(ctx, s.keyPrefix+PendingKey).Result()
}

// Close 关闭连接
func (s *Store) Close() error {
	return nil
}
