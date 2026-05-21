package persistent

import (
	"context"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/log"
	piface "github.com/RichardKnop/machinery/v2/persistent/iface"
	siface "github.com/RichardKnop/machinery/v2/persistent/storage/iface"
	"github.com/RichardKnop/machinery/v2/tasks"
)

// Persistent 持久化层实现
type Persistent struct {
	storage       siface.PersistentStorage
	broker        iface.Broker
	cnf           *config.PersistentConfig
	buffer        []*piface.PersistentEntry
	bufferMu      sync.Mutex
	bufferSize    int
	flushInterval time.Duration
	scheduled     map[string]*piface.ScheduledSignature
	scheduledMu   sync.RWMutex
	constraints   []piface.ScheduleConstraint
	constraintsMu sync.RWMutex
	stopChan      chan struct{}
	wg            sync.WaitGroup
	stats         *piface.PersistentStats
	statsMu       sync.RWMutex
}

// NewPersistent 创建 Persistent 实例
func NewPersistent(storage siface.PersistentStorage, broker iface.Broker, cnf *config.PersistentConfig) *Persistent {
	return &Persistent{
		storage:       storage,
		broker:        broker,
		cnf:           cnf,
		buffer:        make([]*piface.PersistentEntry, 0, 1000),
		bufferSize:    1000,
		flushInterval: 5 * time.Second,
		scheduled:     make(map[string]*piface.ScheduledSignature),
		stopChan:      make(chan struct{}),
		stats: &piface.PersistentStats{
			QueueStats: make(map[string]int64),
		},
	}
}

// Receive 接收单个 signature
func (p *Persistent) Receive(signature *tasks.Signature) error {
	entry := p.signatureToEntry(signature)

	p.bufferMu.Lock()
	p.buffer = append(p.buffer, entry)
	shouldFlush := len(p.buffer) >= p.bufferSize
	p.bufferMu.Unlock()

	p.recordReceived(entry.Queue)

	if shouldFlush {
		return p.Flush(context.Background())
	}
	return nil
}

// ReceiveBatch 批量接收 signatures
func (p *Persistent) ReceiveBatch(signatures []*tasks.Signature) error {
	entries := make([]*piface.PersistentEntry, len(signatures))
	for i, sig := range signatures {
		entries[i] = p.signatureToEntry(sig)
	}

	p.bufferMu.Lock()
	p.buffer = append(p.buffer, entries...)
	shouldFlush := len(p.buffer) >= p.bufferSize
	p.bufferMu.Unlock()

	for _, entry := range entries {
		p.recordReceived(entry.Queue)
	}

	if shouldFlush {
		return p.Flush(context.Background())
	}
	return nil
}

// Flush 立即将缓冲区写入 storage
func (p *Persistent) Flush(ctx context.Context) error {
	p.bufferMu.Lock()
	if len(p.buffer) == 0 {
		p.bufferMu.Unlock()
		return nil
	}

	entries := make([]*piface.PersistentEntry, len(p.buffer))
	copy(entries, p.buffer)
	p.buffer = p.buffer[:0]
	p.bufferMu.Unlock()

	for _, entry := range entries {
		if err := p.storage.Store(ctx, entry); err != nil {
			return err
		}
	}
	return nil
}

// Start 启动调度器
func (p *Persistent) Start(ctx context.Context) error {
	p.wg.Add(1)
	go p.flushLoop(ctx)

	p.wg.Add(1)
	go p.scheduleLoop(ctx)

	p.wg.Add(1)
	go p.cleanupLoop(ctx)

	return nil
}

// Stop 停止调度器
func (p *Persistent) Stop() error {
	close(p.stopChan)
	p.Flush(context.Background())
	p.wg.Wait()
	return p.storage.Close()
}

// TriggerQueue 手动触发指定队列的调度
func (p *Persistent) TriggerQueue(ctx context.Context, queue string) error {
	queueConfig := p.cnf.GetQueueConfig(queue)
	return p.scheduleQueue(ctx, queue, queueConfig)
}

// TriggerAll 手动触发所有队列的调度
func (p *Persistent) TriggerAll(ctx context.Context) error {
	queues := p.getQueueList()
	for _, queue := range queues {
		queueConfig := p.cnf.GetQueueConfig(queue)
		if !queueConfig.Enabled {
			continue
		}
		if err := p.scheduleQueue(ctx, queue, queueConfig); err != nil {
			log.GetLogger().Errorf("[Persistent] Trigger queue %s error: %v", queue, err)
		}
	}
	return nil
}

// RegisterConstraint 注册调度约束
func (p *Persistent) RegisterConstraint(constraint piface.ScheduleConstraint) {
	p.constraintsMu.Lock()
	defer p.constraintsMu.Unlock()

	for i, c := range p.constraints {
		if c.Name() == constraint.Name() {
			p.constraints[i] = constraint
			return
		}
	}
	p.constraints = append(p.constraints, constraint)
}

// UnregisterConstraint 注销调度约束
func (p *Persistent) UnregisterConstraint(name string) {
	p.constraintsMu.Lock()
	defer p.constraintsMu.Unlock()

	for i, c := range p.constraints {
		if c.Name() == name {
			p.constraints = append(p.constraints[:i], p.constraints[i+1:]...)
			return
		}
	}
}

// GetStats 获取持久化层统计信息
func (p *Persistent) GetStats() *piface.PersistentStats {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()

	result := *p.stats
	result.QueueStats = make(map[string]int64)
	for k, v := range p.stats.QueueStats {
		result.QueueStats[k] = v
	}

	p.bufferMu.Lock()
	result.BufferSize = len(p.buffer)
	p.bufferMu.Unlock()

	p.scheduledMu.RLock()
	result.ScheduledSize = len(p.scheduled)
	p.scheduledMu.RUnlock()

	return &result
}

// GetScheduledSignatures 获取已调度但未完成的 signatures
func (p *Persistent) GetScheduledSignatures() []*piface.ScheduledSignature {
	p.scheduledMu.RLock()
	defer p.scheduledMu.RUnlock()

	result := make([]*piface.ScheduledSignature, 0, len(p.scheduled))
	for _, s := range p.scheduled {
		result = append(result, s)
	}
	return result
}

// Complete 标记任务已完成
func (p *Persistent) Complete(id string) {
	p.removeScheduled(id)

	p.statsMu.Lock()
	p.stats.TotalCompleted++
	p.statsMu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := p.storage.MarkDone(ctx, id); err != nil {
			log.GetLogger().Warnf("[Persistent] MarkDone error: %v", err)
		}
	}()
}

// Fail 标记任务已失败
func (p *Persistent) Fail(id string, err string) {
	p.removeScheduled(id)

	p.statsMu.Lock()
	p.stats.TotalFailed++
	p.statsMu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if storageErr := p.storage.MarkFailed(ctx, id, err); storageErr != nil {
			log.GetLogger().Warnf("[Persistent] MarkFailed error: %v", storageErr)
		}
	}()
}

// flushLoop 定时 flush 缓冲区
func (p *Persistent) flushLoop(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopChan:
			return
		case <-ticker.C:
			if err := p.Flush(ctx); err != nil {
				log.GetLogger().Errorf("[Persistent] Flush error: %v", err)
			}
		}
	}
}

// scheduleLoop 调度循环
func (p *Persistent) scheduleLoop(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopChan:
			return
		case <-ticker.C:
			if err := p.scheduleAllQueues(ctx); err != nil {
				log.GetLogger().Errorf("[Persistent] Schedule error: %v", err)
			}
		}
	}
}

// scheduleAllQueues 调度所有队列
func (p *Persistent) scheduleAllQueues(ctx context.Context) error {
	queues := p.getQueueList()
	queueSizes := p.getQueueSizes(ctx, queues)

	for _, queue := range queues {
		queueConfig := p.cnf.GetQueueConfig(queue)
		if !queueConfig.Enabled {
			continue
		}
		if !p.shouldSchedule(queue, queueSizes[queue], queueConfig) {
			continue
		}
		if err := p.scheduleQueue(ctx, queue, queueConfig); err != nil {
			log.GetLogger().Errorf("[Persistent] Schedule queue %s error: %v", queue, err)
		}
	}
	return nil
}

// getQueueSizes 统一获取所有队列长度
func (p *Persistent) getQueueSizes(ctx context.Context, queues []string) map[string]int64 {
	sizes := make(map[string]int64)

	if p.cnf.QueuePrefix != "" {
		// TODO: 通过 QueuePrefix 批量获取队列长度
	}

	for _, queue := range queues {
		count, _ := p.storage.CountPending(ctx, queue)
		sizes[queue] = count
	}
	return sizes
}

// shouldSchedule 检查是否应该调度
func (p *Persistent) shouldSchedule(queue string, currentSize int64, queueConfig *config.QueueConfig) bool {
	if queueConfig.QueueThreshold > 0 && currentSize < queueConfig.QueueThreshold {
		return true
	}
	return false
}

// scheduleQueue 调度指定队列
func (p *Persistent) scheduleQueue(ctx context.Context, queue string, queueConfig *config.QueueConfig) error {
	batchSize := queueConfig.GetBatchSize()

	entries, err := p.storage.GetForSchedule(ctx, queue, batchSize)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	entries = p.applyConstraints(entries)
	if len(entries) == 0 {
		return nil
	}

	log.GetLogger().Infof("[Persistent] Scheduling %d tasks for queue %s", len(entries), queue)

	var scheduledCount int64
	for _, entry := range entries {
		if err := p.storage.MarkScheduled(ctx, entry.ID); err != nil {
			log.GetLogger().Warnf("[Persistent] MarkScheduled error: %v", err)
			continue
		}

		if err := p.broker.Publish(ctx, entry.Signature); err != nil {
			log.GetLogger().Errorf("[Persistent] Publish error: %v", err)
			p.storage.MarkFailed(ctx, entry.ID, err.Error())
			continue
		}

		p.addScheduled(entry)
		scheduledCount++
	}

	p.recordSchedule(queue, scheduledCount)
	return nil
}

// applyConstraints 应用调度约束
func (p *Persistent) applyConstraints(entries []*piface.PersistentEntry) []*piface.PersistentEntry {
	p.constraintsMu.RLock()
	constraints := make([]piface.ScheduleConstraint, len(p.constraints))
	copy(constraints, p.constraints)
	p.constraintsMu.RUnlock()

	p.scheduledMu.RLock()
	scheduled := make([]*piface.ScheduledSignature, 0, len(p.scheduled))
	for _, s := range p.scheduled {
		scheduled = append(scheduled, s)
	}
	p.scheduledMu.RUnlock()

	result := make([]*piface.PersistentEntry, 0, len(entries))
	for _, entry := range entries {
		pass := true
		for _, constraint := range constraints {
			if !constraint.Check(entry, scheduled) {
				pass = false
				break
			}
		}
		if pass {
			result = append(result, entry)
			scheduled = append(scheduled, &piface.ScheduledSignature{
				ID:          entry.ID,
				Queue:       entry.Queue,
				SubjectID:   entry.SubjectID,
				SubjectType: entry.SubjectType,
				GroupID:     entry.GroupID,
				ScheduledAt: time.Now(),
			})
		}
	}
	return result
}

// addScheduled 添加到已调度缓存
func (p *Persistent) addScheduled(entry *piface.PersistentEntry) {
	p.scheduledMu.Lock()
	defer p.scheduledMu.Unlock()

	p.scheduled[entry.ID] = &piface.ScheduledSignature{
		ID:          entry.ID,
		Queue:       entry.Queue,
		SubjectID:   entry.SubjectID,
		SubjectType: entry.SubjectType,
		GroupID:     entry.GroupID,
		ScheduledAt: time.Now(),
	}
}

// removeScheduled 从已调度缓存移除
func (p *Persistent) removeScheduled(id string) {
	p.scheduledMu.Lock()
	defer p.scheduledMu.Unlock()

	delete(p.scheduled, id)
}

// cleanupLoop 清理超时的已调度任务
func (p *Persistent) cleanupLoop(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopChan:
			return
		case <-ticker.C:
			p.cleanupScheduled(ctx)
		}
	}
}

// cleanupScheduled 清理超时的已调度任务
func (p *Persistent) cleanupScheduled(ctx context.Context) {
	p.scheduledMu.Lock()
	defer p.scheduledMu.Unlock()

	timeout := time.Duration(p.cnf.ConstraintConfig.ScheduledTimeout) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}

	now := time.Now()
	for id, sig := range p.scheduled {
		if now.Sub(sig.ScheduledAt) > timeout {
			delete(p.scheduled, id)
		}
	}
}

// signatureToEntry 将 signature 转换为 PersistentEntry
func (p *Persistent) signatureToEntry(signature *tasks.Signature) *piface.PersistentEntry {
	now := time.Now()

	entry := &piface.PersistentEntry{
		ID:        signature.UUID,
		Signature: signature,
		Status:    piface.StatusPending,
		Queue:     signature.RoutingKey,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if v, ok := signature.Headers["task_type"]; ok {
		entry.TaskType = piface.TaskType(v.(string))
	}
	if v, ok := signature.Headers["plan_id"]; ok {
		entry.PlanID = v.(string)
	}
	if v, ok := signature.Headers["subject_id"]; ok {
		entry.SubjectID = v.(string)
	}
	if v, ok := signature.Headers["subject_type"]; ok {
		entry.SubjectType = v.(string)
	}
	if v, ok := signature.Headers["group_id"]; ok {
		entry.GroupID = v.(string)
	}
	if v, ok := signature.Headers["priority"]; ok {
		// TODO: 类型转换
		_ = v
	}

	return entry
}

// recordReceived 记录接收统计
func (p *Persistent) recordReceived(queue string) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()

	p.stats.TotalReceived++
	if queue == "" {
		queue = "default"
	}
	p.stats.QueueStats[queue]++
}

// recordSchedule 记录调度统计
func (p *Persistent) recordSchedule(queue string, count int64) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()

	p.stats.TotalScheduled++
	now := time.Now()
	p.stats.LastScheduleTime = &now

	if queue == "" {
		queue = "default"
	}
	p.stats.QueueStats[queue] += count
}

// getQueueList 获取所有队列列表
func (p *Persistent) getQueueList() []string {
	queues := []string{""}
	for queueName := range p.cnf.QueueConfigs {
		queues = append(queues, queueName)
	}
	return queues
}
