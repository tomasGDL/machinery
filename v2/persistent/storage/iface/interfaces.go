package iface

import (
	"context"

	persistentiface "github.com/RichardKnop/machinery/v2/persistent/iface"
)

// PersistentStorage 持久化存储接口（Storage 层）
type PersistentStorage interface {
	// Store 存储条目
	Store(ctx context.Context, entry *persistentiface.PersistentEntry) error

	// GetForSchedule 获取待调度条目（调度器专用）
	GetForSchedule(ctx context.Context, queue string, batchSize int) ([]*persistentiface.PersistentEntry, error)

	// MarkScheduled 标记为已调度
	MarkScheduled(ctx context.Context, id string) error

	// MarkDone 标记为已完成并删除
	MarkDone(ctx context.Context, id string) error

	// MarkFailed 标记为失败（增加 scheduled_count）
	MarkFailed(ctx context.Context, id string, err string) error

	// CountPending 获取待处理数量
	CountPending(ctx context.Context, queue string) (int64, error)

	// Close 关闭连接
	Close() error
}
