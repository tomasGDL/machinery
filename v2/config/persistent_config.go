package config

// QueueConfig 队列配置
type QueueConfig struct {
	QueueName        string `json:"queue_name" yaml:"queue_name"`
	MaxQueueSize     int64  `json:"max_queue_size" yaml:"max_queue_size"`
	QueueThreshold   int64  `json:"queue_threshold" yaml:"queue_threshold"`
	BatchSize        int    `json:"batch_size" yaml:"batch_size"`
	ScheduleInterval int    `json:"schedule_interval" yaml:"schedule_interval"`
	Enabled          bool   `json:"enabled" yaml:"enabled"`
}

// GetBatchSize 获取批量大小（自动计算）
func (c *QueueConfig) GetBatchSize() int {
	if c.BatchSize > 0 {
		return c.BatchSize
	}
	if c.MaxQueueSize > 0 {
		batchSize := int(c.MaxQueueSize / 10)
		if batchSize < 10 {
			return 10
		}
		if batchSize > 1000 {
			return 1000
		}
		return batchSize
	}
	return 100
}

// RetryPolicy 重试策略
type RetryPolicy struct {
	MaxRetries    int  `json:"max_retries" yaml:"max_retries"`
	RetryInterval int  `json:"retry_interval" yaml:"retry_interval"`
	RetryBackoff  bool `json:"retry_backoff" yaml:"retry_backoff"`
	MaxInterval   int  `json:"max_interval" yaml:"max_interval"`
}

// ConstraintConfig 调度约束配置
type ConstraintConfig struct {
	MaxScheduledPerSubject int `json:"max_scheduled_per_subject" yaml:"max_scheduled_per_subject"`
	MaxScheduledPerAccount int `json:"max_scheduled_per_account" yaml:"max_scheduled_per_account"`
	MaxScheduledPerGroup   int `json:"max_scheduled_per_group" yaml:"max_scheduled_per_group"`
	ScheduledTimeout       int `json:"scheduled_timeout" yaml:"scheduled_timeout"`
}

// PersistentConfig 持久化配置
type PersistentConfig struct {
	Enabled            bool                    `json:"enabled" yaml:"enabled"`
	StoreType          string                  `json:"store_type" yaml:"store_type"`
	QueuePrefix        string                  `json:"queue_prefix" yaml:"queue_prefix"`
	DefaultQueueConfig QueueConfig             `json:"default_queue_config" yaml:"default_queue_config"`
	QueueConfigs       map[string]*QueueConfig `json:"queue_configs" yaml:"queue_configs"`
	DefaultRetryPolicy *RetryPolicy            `json:"default_retry_policy" yaml:"default_retry_policy"`
	StoreConfig        map[string]interface{}  `json:"store_config" yaml:"store_config"`
	ConstraintConfig   *ConstraintConfig       `json:"constraint_config" yaml:"constraint_config"`
}

// GetQueueConfig 获取指定队列的配置
func (c *PersistentConfig) GetQueueConfig(queueName string) *QueueConfig {
	if queueConfig, ok := c.QueueConfigs[queueName]; ok {
		return queueConfig
	}
	defaultConfig := c.DefaultQueueConfig
	defaultConfig.QueueName = queueName
	return &defaultConfig
}
