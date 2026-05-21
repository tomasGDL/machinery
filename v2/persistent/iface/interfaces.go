package iface

import (
	"context"
	"time"

	"github.com/RichardKnop/machinery/v2/tasks"
)

// TaskType 任务类型
type TaskType string

const (
	TaskTypeInspection TaskType = "inspection"
	TaskTypePassword   TaskType = "password"
	TaskTypeEmail      TaskType = "email"
	TaskTypeSMS        TaskType = "sms"
	TaskTypeCustom     TaskType = "custom"
)

// TaskStatus 任务状态
type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusScheduled TaskStatus = "scheduled"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
	StatusCancelled TaskStatus = "cancelled"
)

// PersistentEntry 持久化条目
type PersistentEntry struct {
	ID             string            `json:"id" bson:"_id"`
	Signature      *tasks.Signature  `json:"signature" bson:"signature"`
	TaskType       TaskType          `json:"task_type" bson:"task_type"`
	PlanID         string            `json:"plan_id" bson:"plan_id"`
	SubjectID      string            `json:"subject_id" bson:"subject_id"`
	SubjectType    string            `json:"subject_type" bson:"subject_type"`
	Priority       int               `json:"priority" bson:"priority"`
	Queue          string            `json:"queue" bson:"queue"`
	GroupID        string            `json:"group_id" bson:"group_id"`
	ScheduledAt    *time.Time        `json:"scheduled_at" bson:"scheduled_at"`
	ExpiredAt      *time.Time        `json:"expired_at" bson:"expired_at"`
	Status         TaskStatus        `json:"status" bson:"status"`
	ScheduledCount int               `json:"scheduled_count" bson:"scheduled_count"`
	LastError      string            `json:"last_error" bson:"last_error"`
	LastAttempt    *time.Time        `json:"last_attempt" bson:"last_attempt"`
	CreatedAt      time.Time         `json:"created_at" bson:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at" bson:"updated_at"`
	Metadata       map[string]string `json:"metadata" bson:"metadata"`
}

// CanSchedule 检查是否可以被调度
func (e *PersistentEntry) CanSchedule() bool {
	if e.Status != StatusPending && e.Status != StatusFailed {
		return false
	}
	if e.IsExpired() {
		return false
	}
	if e.ScheduledAt != nil && time.Now().Before(*e.ScheduledAt) {
		return false
	}
	return true
}

// IsExpired 检查是否已过期
func (e *PersistentEntry) IsExpired() bool {
	if e.ExpiredAt == nil {
		return false
	}
	return time.Now().After(*e.ExpiredAt)
}

// Persistent 持久化层接口
type Persistent interface {
	Receive(signature *tasks.Signature) error
	ReceiveBatch(signatures []*tasks.Signature) error
	Flush(ctx context.Context) error
	Start(ctx context.Context) error
	Stop() error
	TriggerQueue(ctx context.Context, queue string) error
	TriggerAll(ctx context.Context) error
	RegisterConstraint(constraint ScheduleConstraint)
	UnregisterConstraint(name string)
	GetStats() *PersistentStats
	GetScheduledSignatures() []*ScheduledSignature
	Complete(id string)
	Fail(id string, err string)
}

// PersistentStats 持久化层统计信息
type PersistentStats struct {
	TotalReceived    int64            `json:"total_received"`
	TotalScheduled   int64            `json:"total_scheduled"`
	TotalCompleted   int64            `json:"total_completed"`
	TotalFailed      int64            `json:"total_failed"`
	BufferSize       int              `json:"buffer_size"`
	ScheduledSize    int              `json:"scheduled_size"`
	LastScheduleTime *time.Time       `json:"last_schedule_time"`
	QueueStats       map[string]int64 `json:"queue_stats"`
}

// ScheduledSignature 已调度但未完成的签名
type ScheduledSignature struct {
	ID          string    `json:"id"`
	Queue       string    `json:"queue"`
	SubjectID   string    `json:"subject_id"`
	SubjectType string    `json:"subject_type"`
	GroupID     string    `json:"group_id"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

// ScheduleConstraint 调度约束接口
type ScheduleConstraint interface {
	Name() string
	Check(entry *PersistentEntry, scheduled []*ScheduledSignature) bool
}
