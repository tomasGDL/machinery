package machinery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/RichardKnop/machinery/v2/config"
	"github.com/redis/go-redis/v9"
)

const DLQKeyPrefix = "mq:deadletter"

type DLQManager struct {
	client redis.UniversalClient
	dlqKey string
	onPush func(entry config.DLQEntry)
}

func NewDLQManager(client redis.UniversalClient, queueName string, onPush func(entry config.DLQEntry)) *DLQManager {
	return &DLQManager{
		client: client,
		dlqKey: fmt.Sprintf("%s:%s", DLQKeyPrefix, queueName),
		onPush: onPush,
	}
}

func (m *DLQManager) Push(entry config.DLQEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("DLQ entry marshal error: %w", err)
	}

	err = m.client.RPush(context.Background(), m.dlqKey, data).Err()
	if err != nil {
		return fmt.Errorf("DLQ push error: %w", err)
	}

	if m.onPush != nil {
		m.onPush(entry)
	}

	return nil
}

func (m *DLQManager) List(start, stop int64) ([]config.DLQEntry, error) {
	items, err := m.client.LRange(context.Background(), m.dlqKey, start, stop).Result()
	if err != nil {
		return nil, err
	}

	entries := make([]config.DLQEntry, 0, len(items))
	for _, item := range items {
		var entry config.DLQEntry
		if err := json.Unmarshal([]byte(item), &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (m *DLQManager) Size() (int64, error) {
	return m.client.LLen(context.Background(), m.dlqKey).Result()
}

func (m *DLQManager) Clear() error {
	return m.client.Del(context.Background(), m.dlqKey).Err()
}
