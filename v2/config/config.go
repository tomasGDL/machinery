package config

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds all configuration for Machinery
type Config struct {
	// UniversalOptions contains redis connection options
	// Supports single node, sentinel and cluster modes
	redis.UniversalOptions

	// DefaultQueue is the default queue name for tasks
	// Default: "default"
	DefaultQueue string

	// ResultsExpireIn is the time in seconds for task states and group metadata to expire from the backend
	// Default: 3600 (1 hour)
	ResultsExpireIn int

	// TaskPrefix is the prefix for auto-generated task UUIDs
	// Default: "task_"
	TaskPrefix string

	// DefaultMaxRetry is the default maximum number of retries for failed tasks
	// Default: 3
	DefaultMaxRetry int

	// NoUnixSignals when set disables signal handling in machinery
	// Default: false
	NoUnixSignals bool

	// NormalTasksPollPeriod specifies the period for polling redis for normal tasks
	// Default: 1s
	NormalTasksPollPeriod time.Duration

	// DelayedTasksPollPeriod specifies the period for polling redis for delayed tasks
	// Default: 500ms
	DelayedTasksPollPeriod time.Duration

	// DelayedTasksKey is the redis key used to store delayed tasks
	// Default: "delayed_tasks"
	DelayedTasksKey string

	// DLQEnabled enables the dead letter queue for failed tasks
	// Default: false
	DLQEnabled bool

	// DLQKey is the redis key used to store dead letter queue entries
	// Default: "mq:deadletter:{queue_name}"
	DLQKey string

	// DLQOnPush is a callback function that is called when a task is pushed to the DLQ
	// Can be used for alerting or logging
	DLQOnPush func(entry DLQEntry)

	// Persistent 持久化配置
	Persistent *PersistentConfig `json:"persistent" yaml:"persistent"`
}

// DLQEntry represents a dead letter queue entry
type DLQEntry struct {
	TaskUUID   string    `json:"uuid"`
	TaskName   string    `json:"name"`
	Reason     string    `json:"reason"`
	RetryCount int       `json:"retry_count"`
	Timestamp  time.Time `json:"timestamp"`
}

// DefaultConfig returns a Config instance with default values
func DefaultConfig() *Config {
	return &Config{
		DefaultQueue:           "default",
		ResultsExpireIn:        3600,
		TaskPrefix:             "task_",
		DefaultMaxRetry:        3,
		NoUnixSignals:          false,
		NormalTasksPollPeriod:  1 * time.Second,
		DelayedTasksPollPeriod: 500 * time.Millisecond,
		DelayedTasksKey:        "delayed_tasks",
		UniversalOptions: redis.UniversalOptions{
			Addrs: []string{"localhost:6379"},
			DB:    0,
		},
	}
}
