# Machinery v2 消息可靠性优化方案

> 本文档提供基于代码层面的消息可靠性优化方案，不涉及 Redis 服务器配置。

## 一、方案概览

### 1.1 优化目标

通过代码层面的改进，解决以下核心问题：
- 任务从队列取出后 Worker 崩溃导致消息丢失
- 任务处理中 Worker 崩溃后无法恢复
- 任务无限期处于处理中状态无超时控制
- 最终失败的任务无处可去

### 1.2 实施计划

| 阶段 | 优化项 | 优先级 | 影响范围 | 向后兼容 |
|------|--------|--------|----------|----------|
| 1 | 消息确认机制 | 高 | Broker/Worker | 是 |
| 2 | 未完成任务恢复 | 高 | Worker | 是 |
| 3 | 任务超时控制 | 高 | Worker | 是 |
| 4 | 死信队列 | 中 | Broker/Worker | 是 |
| 5 | 延迟任务事务优化 | 中 | Broker | 是 |

---

## 二、方案详细设计

### 2.1 消息确认机制（核心方案）

**问题**: 当前使用 `BLPop` 弹出消息，一旦 Worker 崩溃，消息丢失。

**解决方案**: 使用 `BRPopLPush` 实现"处理中队列"，配合**批量 ACK 窗口**机制，定期批量确认已完成的任务。

#### 设计要点

**为什么需要批量 ACK 窗口？**

Worker 并发处理多个任务时，如果每个任务完成都立即调用 `Ack`：
- 多个 goroutine 同时对处理中队列执行 `LRem`，可能产生竞态条件
- 高频 Redis 调用增加延迟和压力
- ACK 失败时重试逻辑复杂

**批量窗口机制**：
- Worker 本地维护一个已完成任务的缓冲区（窗口）
- 定期（如每 1 秒）或在窗口达到一定大小（如 100 个任务）时，批量确认
- 使用 Redis Pipeline 一次性删除多个任务
- Worker 关闭时确保窗口中的任务都被确认

#### 2.1.1 Broker 接口扩展

在 `brokers/iface/interfaces.go` 中新增接口方法：

```go
// Broker - a common interface for all brokers
type Broker interface {
    // 现有方法保持不变...
    
    // 新增：可靠性相关方法
    
    // Ack acknowledges a single task after successful processing
    // This is kept for backward compatibility and simple use cases
    Ack(ctx context.Context, task *tasks.Signature) error
    
    // AckBatch acknowledges multiple tasks at once using Redis pipeline
    // Returns the number of successfully acknowledged tasks
    AckBatch(ctx context.Context, tasks []*tasks.Signature) (int, error)
    
    // Reject rejects a task and optionally requeues it
    Reject(ctx context.Context, task *tasks.Signature, requeue bool) error
}
```

#### 2.1.2 AckWindow - 批量确认窗口实现

新增文件 `brokers/redis/ack_window.go`：

```go
package redis

import (
    "context"
    "sync"
    "time"

    "github.com/RichardKnop/machinery/v2/log"
    "github.com/RichardKnop/machinery/v2/tasks"
    "github.com/redis/go-redis/v9"
)

// AckWindow implements batch acknowledgment with a sliding window
type AckWindow struct {
    broker    *BrokerGR
    rclient   redis.UniversalClient
    
    // buffer stores tasks waiting to be acknowledged
    buffer []*tasks.Signature
    
    // mu protects buffer access
    mu sync.Mutex
    
    // maxSize is the maximum number of tasks before triggering a flush
    maxSize int
    
    // flushInterval is the maximum time to wait before flushing
    flushInterval time.Duration
    
    // ticker for periodic flush
    ticker *time.Ticker
    
    // stopChan signals the flush goroutine to stop
    stopChan chan struct{}
    
    // wg waits for flush goroutine to complete
    wg sync.WaitGroup
}

// NewAckWindow creates a new acknowledgment window
func NewAckWindow(broker *BrokerGR, maxSize int, flushInterval time.Duration) *AckWindow {
    w := &AckWindow{
        broker:        broker,
        rclient:       broker.rclient,
        buffer:        make([]*tasks.Signature, 0, maxSize),
        maxSize:       maxSize,
        flushInterval: flushInterval,
        ticker:        time.NewTicker(flushInterval),
        stopChan:      make(chan struct{}),
    }
    
    // Start periodic flush goroutine
    w.wg.Add(1)
    go w.flushLoop()
    
    return w
}

// Add adds a task to the acknowledgment window
// Triggers flush if buffer is full
func (w *AckWindow) Add(task *tasks.Signature) {
    w.mu.Lock()
    defer w.mu.Unlock()
    
    w.buffer = append(w.buffer, task)
    
    // Trigger flush if buffer is full
    if len(w.buffer) >= w.maxSize {
        w.flushLocked()
    }
}

// Flush forces immediate acknowledgment of all buffered tasks
func (w *AckWindow) Flush() error {
    w.mu.Lock()
    defer w.mu.Unlock()
    return w.flushLocked()
}

// flushLocked flushes buffered tasks (must be called with lock held)
func (w *AckWindow) flushLocked() error {
    if len(w.buffer) == 0 {
        return nil
    }
    
    // Get tasks to flush and clear buffer
    tasksToFlush := w.buffer
    w.buffer = make([]*tasks.Signature, 0, w.maxSize)
    
    // Use Redis pipeline for batch operations
    pipe := w.rclient.Pipeline()
    
    for _, task := range tasksToFlush {
        processingKey := w.broker.getProcessingKey(task.RoutingKey)
        msg, err := json.Marshal(task)
        if err != nil {
            log.GetLogger().Errorf("Failed to marshal task %s for ack: %v", task.UUID, err)
            continue
        }
        
        // Remove from processing queue
        pipe.LRem(context.Background(), processingKey, 1, msg)
    }
    
    // Execute pipeline
    _, err := pipe.Exec(context.Background())
    if err != nil {
        // Log error but don't fail - tasks are already marked successful in backend
        log.GetLogger().Errorf("Failed to ack %d tasks: %v", len(tasksToFlush), err)
        
        // Put tasks back to buffer for retry
        w.buffer = append(w.buffer, tasksToFlush...)
        return err
    }
    
    log.GetLogger().Debugf("Acknowledged %d tasks", len(tasksToFlush))
    return nil
}

// flushLoop periodically flushes buffered tasks
func (w *AckWindow) flushLoop() {
    defer w.wg.Done()
    
    for {
        select {
        case <-w.ticker.C:
            w.Flush()
        case <-w.stopChan:
            w.ticker.Stop()
            // Final flush before stopping
            w.Flush()
            return
        }
    }
}

// Close closes the acknowledgment window and flushes remaining tasks
func (w *AckWindow) Close() {
    close(w.stopChan)
    w.wg.Wait()
}
```

#### 2.1.3 Redis Broker 实现

修改 `brokers/redis/goredis.go`：

```go
type BrokerGR struct {
    common.Broker
    rclient redis.UniversalClient
    consumingWG sync.WaitGroup
    processingWG sync.WaitGroup
    delayedWG sync.WaitGroup
    redisDelayedTasksKey string
    
    // 新增：处理中队列前缀
    processingKeyPrefix string
    
    // 新增：批量确认窗口（每个 Broker 实例一个）
    ackWindow *AckWindow
}

// New 创建新的 Broker 实例
func New(cnf *config.Config, client redis.UniversalClient) iface.Broker {
    b := &BrokerGR{
        Broker:               common.NewBroker(cnf),
        rclient:              client,
        redisDelayedTasksKey: cnf.DelayedTasksKey,
        processingKeyPrefix:  "machinery:processing",
        // 默认窗口：100 个任务或 1 秒，以先到者为准
        ackWindow:            NewAckWindow(nil, 100, 1*time.Second),
    }
    // Set broker reference in ack window
    b.ackWindow.broker = b
    return b
}

// NewWithAckWindow 创建带自定义 ACK 窗口配置的 Broker
func NewWithAckWindow(cnf *config.Config, client redis.UniversalClient, 
                       ackWindowSize int, ackWindowInterval time.Duration) iface.Broker {
    b := &BrokerGR{
        Broker:               common.NewBroker(cnf),
        rclient:              client,
        redisDelayedTasksKey: cnf.DelayedTasksKey,
        processingKeyPrefix:  "machinery:processing",
        ackWindow:            nil, // Will be initialized below
    }
    b.ackWindow = NewAckWindow(b, ackWindowSize, ackWindowInterval)
    return b
}

// GetAckWindow returns the acknowledgment window for direct use
func (b *BrokerGR) GetAckWindow() *AckWindow {
    return b.ackWindow
}

// getProcessingKey 获取处理中队列的 key
func (b *BrokerGR) getProcessingKey(queue string) string {
    if queue == "" {
        queue = b.GetConfig().DefaultQueue
    }
    return fmt.Sprintf("%s:%s", b.processingKeyPrefix, queue)
}
```

#### 2.1.4 nextTask 修改为使用 BRPopLPush

```go
// nextTask 修改为使用 BRPopLPush
func (b *BrokerGR) nextTask(queue string) (result []byte, err error) {
    processingKey := b.getProcessingKey(queue)
    pollPeriod := b.GetConfig().NormalTasksPollPeriod
    if pollPeriod <= 0 {
        pollPeriod = DefaultNormalTasksPollPeriod
    }
    
    // BRPopLPush: 原子地将元素从源列表弹出并推入目标列表
    result, err = b.rclient.BRPopLPush(
        context.Background(),
        queue,          // source queue
        processingKey,  // processing queue
        pollPeriod,
    ).Bytes()
    
    if err != nil {
        if err == redis.Nil {
            return []byte{}, nil
        }
        return []byte{}, err
    }
    
    return result, nil
}
```

#### 2.1.5 批量 Ack 实现

```go
// Ack 确认单个任务（向后兼容，内部使用窗口）
func (b *BrokerGR) Ack(ctx context.Context, signature *tasks.Signature) error {
    // 直接添加到窗口，由窗口批量处理
    b.ackWindow.Add(signature)
    return nil
}

// AckBatch 批量确认多个任务
func (b *BrokerGR) AckBatch(ctx context.Context, signatures []*tasks.Signature) (int, error) {
    if len(signatures) == 0 {
        return 0, nil
    }
    
    // 按队列分组
    tasksByQueue := make(map[string][]*tasks.Signature)
    for _, sig := range signatures {
        queue := sig.RoutingKey
        if queue == "" {
            queue = b.GetConfig().DefaultQueue
        }
        tasksByQueue[queue] = append(tasksByQueue[queue], sig)
    }
    
    // 使用 Pipeline 批量删除
    pipe := b.rclient.Pipeline()
    totalAcked := 0
    
    for queue, tasks := range tasksByQueue {
        processingKey := b.getProcessingKey(queue)
        for _, task := range tasks {
            msg, err := json.Marshal(task)
            if err != nil {
                log.GetLogger().Errorf("Failed to marshal task %s: %v", task.UUID, err)
                continue
            }
            pipe.LRem(ctx, processingKey, 1, msg)
            totalAcked++
        }
    }
    
    _, err := pipe.Exec(ctx)
    if err != nil {
        log.GetLogger().Errorf("Failed to ack batch tasks: %v", err)
    }
    
    return totalAcked, err
}

// Reject 拒绝任务（立即处理，不使用窗口）
func (b *BrokerGR) Reject(ctx context.Context, signature *tasks.Signature, requeue bool) error {
    processingKey := b.getProcessingKey(signature.RoutingKey)
    msg, err := json.Marshal(signature)
    if err != nil {
        return fmt.Errorf("JSON marshal error: %s", err)
    }
    
    if !requeue {
        // 不重新入队，仅从处理中队列删除
        return b.rclient.LRem(ctx, processingKey, 1, msg).Err()
    }
    
    // 从处理中队列删除并重新放入原队列
    pipe := b.rclient.TxPipeline()
    pipe.LRem(ctx, processingKey, 1, msg)
    pipe.LPush(ctx, signature.RoutingKey, msg)
    _, err = pipe.Exec(ctx)
    return err
}

// StopConsuming 修改：确保在停止前刷新 ACK 窗口
func (b *BrokerGR) StopConsuming() {
    // 先刷新 ACK 窗口
    if b.ackWindow != nil {
        b.ackWindow.Close()
    }
    
    // 再停止消费
    b.Broker.StopConsuming()
    b.delayedWG.Wait()
    b.consumingWG.Wait()
}
```

#### 2.1.3 Worker Process 修改

修改 `worker.go` 中的 `Process` 方法：

```go
// Process handles received tasks and triggers success/error callbacks
func (worker *Worker) Process(signature *tasks.Signature) error {
    // 任务处理完成后自动 ACK
    defer func() {
        // 注意：这里需要在 taskSucceeded 或 taskFailed 后调用
        // 实际实现中应该在具体方法末尾调用
    }()
    
    // 现有逻辑保持不变...
    if !worker.server.IsTaskRegistered(signature.Name) {
        return nil
    }
    
    // ... 其余逻辑保持不变
}

// taskSucceeded 修改：增加 Ack 调用
func (worker *Worker) taskSucceeded(signature *tasks.Signature, taskResults []*tasks.TaskResult) error {
    // 更新任务状态
    if err := worker.server.GetBackend().SetStateSuccess(signature, taskResults); err != nil {
        return fmt.Errorf("Set state to 'success' for task %s returned error: %s", signature.UUID, err)
    }
    
    // 确认消息
    if err := worker.server.GetBroker().Ack(context.Background(), signature); err != nil {
        log.GetLogger().Warnf("Failed to ack task %s: %v", signature.UUID, err)
        // 不返回错误，因为任务已成功
    }
    
    // ... 其余逻辑保持不变
}

// taskFailed 修改：增加 Reject 调用
func (worker *Worker) taskFailed(signature *tasks.Signature, taskErr error) error {
    // 更新任务状态
    if err := worker.server.GetBackend().SetStateFailure(signature, taskErr.Error()); err != nil {
        return fmt.Errorf("Set state to 'failure' for task %s returned error: %s", signature.UUID, err)
    }
    
    // 拒绝消息（不重新入队，因为已经标记为失败）
    if err := worker.server.GetBroker().Reject(context.Background(), signature, false); err != nil {
        log.GetLogger().Warnf("Failed to reject task %s: %v", signature.UUID, err)
    }
    
    // ... 其余逻辑保持不变
}
```

#### 2.1.4 向后兼容

- 如果 Broker 实现没有 `Ack`/`Reject` 方法，可以通过接口默认实现返回 `nil`（无操作）
- 现有的 `BLPop` 方式可以通过配置开关切换

---

### 2.2 未完成的任务恢复机制

**问题**: Worker 崩溃重启后，处于 `STARTED` 或 `RECEIVED` 状态的任务丢失。

**解决方案**: Worker 启动时扫描处理中队列和未完成状态，恢复任务。

#### 2.2.1 Worker 恢复方法

在 `worker.go` 中添加：

```go
// RecoverUnfinishedTasks scans and recovers unfinished tasks from previous run
func (worker *Worker) RecoverUnfinishedTasks() error {
    cnf := worker.server.GetConfig()
    broker := worker.server.GetBroker()
    backend := worker.server.GetBackend()
    
    queue := worker.Queue
    if queue == "" {
        queue = cnf.DefaultQueue
    }
    
    // 1. 从处理中队列恢复
    processingKey := fmt.Sprintf("machinery:processing:%s", queue)
    pendingTasks, err := broker.GetPendingTasks(processingKey)
    if err != nil {
        return fmt.Errorf("failed to get processing tasks: %w", err)
    }
    
    for _, signature := range pendingTasks {
        if err := worker.recoverTask(signature, backend); err != nil {
            log.GetLogger().Errorf("Failed to recover task %s: %v", signature.UUID, err)
        }
    }
    
    // 2. 从 Backend 状态恢复（可选，处理更广范围的未完成任务）
    // 这需要 Backend 支持查询特定状态的任务
    
    log.GetLogger().Infof("Recovered %d unfinished tasks from processing queue", len(pendingTasks))
    return nil
}

// recoverTask recovers a single unfinished task
func (worker *Worker) recoverTask(signature *tasks.Signature, backend backendsiface.Backend) error {
    // 检查任务当前状态
    state, err := backend.GetState(signature.UUID)
    if err != nil {
        // 如果无法获取状态，直接重新发送
        log.GetLogger().Warnf("Task %s state not found, requeuing", signature.UUID)
        return worker.requeueTask(signature)
    }
    
    // 根据状态决定恢复策略
    switch state.State {
    case tasks.StateReceived, tasks.StateStarted:
        // 任务被中断，重新发送
        log.GetLogger().Infof("Recovering interrupted task %s (state: %s)", 
            signature.UUID, state.State)
        return worker.requeueTask(signature)
        
    case tasks.StateRetry:
        // 重试任务，检查 ETA
        if signature.ETA != nil && signature.ETA.After(time.Now().UTC()) {
            // 还未到重试时间，不处理
            return nil
        }
        return worker.requeueTask(signature)
        
    case tasks.StateSuccess, tasks.StateFailure:
        // 任务已完成，从处理中队列移除
        return worker.server.GetBroker().Ack(context.Background(), signature)
        
    default:
        log.GetLogger().Warnf("Unknown task state %s for task %s", state.State, signature.UUID)
        return worker.requeueTask(signature)
    }
}

// requeueTask requeues a task for processing
func (worker *Worker) requeueTask(signature *tasks.Signature) error {
    // 使用 SendTask 重新发送
    // 注意：这里需要复制签名避免副作用
    _, err := worker.server.SendTask(tasks.CopySignature(signature))
    return err
}
```

#### 2.2.2 启动时集成

修改 `worker.go` 的 `LaunchAsync` 方法：

```go
func (worker *Worker) LaunchAsync(errorsChan chan<- error) {
    cnf := worker.server.GetConfig()
    broker := worker.server.GetBroker()
    
    // 启动时恢复未完成的任务
    if err := worker.RecoverUnfinishedTasks(); err != nil {
        log.GetLogger().Warnf("Failed to recover unfinished tasks: %v", err)
        // 不阻断启动，继续正常运行
    }
    
    // 原有的启动逻辑...
    // ...
}
```

---

### 2.3 任务超时控制机制

**问题**: 任务处理中如果卡住，会永远处于 `STARTED` 状态。

**解决方案**: 为任务执行添加超时控制。

#### 2.3.1 Worker 配置扩展

```go
// Worker 结构新增字段
type Worker struct {
    server            *Server
    ConsumerTag       string
    Concurrency       int
    Queue             string
    errorHandler      func(err error)
    preTaskHandler    func(*tasks.Signature)
    postTaskHandler   func(*tasks.Signature)
    preConsumeHandler func(*Worker) bool
    
    // 新增：任务超时配置
    // TaskTimeout specifies the maximum duration for task execution
    // Zero means no timeout
    TaskTimeout time.Duration
}
```

#### 2.3.2 带超时的 Process 方法

```go
// ProcessWithTimeout handles tasks with timeout control
func (worker *Worker) Process(signature *tasks.Signature) error {
    if !worker.server.IsTaskRegistered(signature.Name) {
        return nil
    }
    
    taskFunc, err := worker.server.GetRegisteredTask(signature.Name)
    if err != nil {
        return nil
    }
    
    // 更新状态到 RECEIVED
    if err = worker.server.GetBackend().SetStateReceived(signature); err != nil {
        return fmt.Errorf("Set state to 'received' for task %s returned error: %s", 
            signature.UUID, err)
    }
    
    task, err := tasks.NewWithSignature(taskFunc, signature)
    if err != nil {
        worker.taskFailed(signature, err)
        return err
    }
    
    // 设置超时 context
    ctx := context.Background()
    if worker.TaskTimeout > 0 {
        var cancel context.CancelFunc
        ctx, cancel = context.WithTimeout(ctx, worker.TaskTimeout)
        defer cancel()
    }
    
    taskSpan := tracing.StartSpanFromHeaders(signature.Headers, signature.Name)
    tracing.AnnotateSpanWithSignatureInfo(taskSpan, signature)
    task.Context = opentracing.ContextWithSpan(ctx, taskSpan)
    
    // 更新状态到 STARTED
    if err = worker.server.GetBackend().SetStateStarted(signature); err != nil {
        return fmt.Errorf("Set state to 'started' for task %s returned error: %s", 
            signature.UUID, err)
    }
    
    if worker.preTaskHandler != nil {
        worker.preTaskHandler(signature)
    }
    if worker.postTaskHandler != nil {
        defer worker.postTaskHandler(signature)
    }
    
    // 使用 goroutine + channel 实现超时
    resultCh := make(chan struct {
        results []*tasks.TaskResult
        err     error
    }, 1)
    
    go func() {
        results, err := task.Call()
        resultCh <- struct {
            results []*tasks.TaskResult
            err     error
        }{results, err}
    }()
    
    // 等待结果或超时
    select {
    case <-ctx.Done():
        // 超时
        if ctx.Err() == context.DeadlineExceeded {
            log.GetLogger().Errorf("Task %s timed out after %v", 
                signature.UUID, worker.TaskTimeout)
            timeoutErr := fmt.Errorf("task timeout after %v", worker.TaskTimeout)
            return worker.taskFailed(signature, timeoutErr)
        }
        return worker.taskFailed(signature, ctx.Err())
        
    case result := <-resultCh:
        if result.err != nil {
            // 处理重试逻辑
            retriableErr, ok := interface{}(result.err).(tasks.ErrRetryTaskLater)
            if ok {
                return worker.retryTaskIn(signature, retriableErr.RetryIn())
            }
            if signature.RetryCount > 0 {
                return worker.taskRetry(signature)
            }
            return worker.taskFailed(signature, result.err)
        }
        return worker.taskSucceeded(signature, result.results)
    }
}
```

#### 2.3.3 使用示例

```go
worker := server.NewWorker("my_worker", 10)
worker.TaskTimeout = 5 * time.Minute  // 5分钟超时
```

---

### 2.4 死信队列机制

**问题**: 任务重试耗尽后，失败任务无处可去。

**解决方案**: 实现死信队列，收集最终失败的任务。

#### 2.4.1 配置扩展

```go
// config.Config 新增字段
type Config struct {
    // 现有字段...
    
    // DeadLetterQueue enables dead letter queue for failed tasks
    // Default: false
    DeadLetterQueue bool
    
    // DeadLetterQueueName is the name of the dead letter queue
    // Default: "dead_letter_queue"
    DeadLetterQueueName string
}
```

#### 2.4.2 Broker 实现

```go
// brokers/redis/goredis.go 新增方法

// PublishToDeadLetterQueue publishes a failed task to the dead letter queue
func (b *BrokerGR) PublishToDeadLetterQueue(ctx context.Context, signature *tasks.Signature, reason string) error {
    // 包装原始签名，添加失败信息
    type DeadLetterMessage struct {
        *tasks.Signature
        FailureReason string    `json:"failure_reason"`
        FailedAt      time.Time `json:"failed_at"`
        RetryCount    int       `json:"retry_count"`
    }
    
    msg := DeadLetterMessage{
        Signature:     signature,
        FailureReason: reason,
        FailedAt:      time.Now().UTC(),
        RetryCount:    signature.RetryCount,
    }
    
    data, err := json.Marshal(msg)
    if err != nil {
        return fmt.Errorf("JSON marshal error: %s", err)
    }
    
    queueName := b.GetConfig().DeadLetterQueueName
    if queueName == "" {
        queueName = "dead_letter_queue"
    }
    
    return b.rclient.RPush(ctx, queueName, data).Err()
}

// GetDeadLetterTasks returns tasks from the dead letter queue
func (b *BrokerGR) GetDeadLetterTasks(queue string) ([]*tasks.Signature, error) {
    queueName := b.GetConfig().DeadLetterQueueName
    if queueName == "" {
        queueName = "dead_letter_queue"
    }
    
    results, err := b.rclient.LRange(context.Background(), queueName, 0, -1).Result()
    if err != nil {
        return nil, err
    }
    
    var signatures []*tasks.Signature
    for _, result := range results {
        var msg struct {
            *tasks.Signature
            FailureReason string    `json:"failure_reason"`
            FailedAt      time.Time `json:"failed_at"`
        }
        if err := json.Unmarshal([]byte(result), &msg); err != nil {
            log.GetLogger().Errorf("Failed to unmarshal dead letter message: %v", err)
            continue
        }
        signatures = append(signatures, msg.Signature)
    }
    
    return signatures, nil
}
```

#### 2.4.3 Worker 集成

修改 `worker.go` 的 `taskFailed` 方法：

```go
func (worker *Worker) taskFailed(signature *tasks.Signature, taskErr error) error {
    // 更新任务状态
    if err := worker.server.GetBackend().SetStateFailure(signature, taskErr.Error()); err != nil {
        return fmt.Errorf("Set state to 'failure' for task %s returned error: %s", 
            signature.UUID, err)
    }
    
    // 检查是否启用死信队列
    cnf := worker.server.GetConfig()
    if cnf.DeadLetterQueue {
        // 将失败任务发送到死信队列
        if err := worker.server.GetBroker().PublishToDeadLetterQueue(
            context.Background(), signature, taskErr.Error()); err != nil {
            log.GetLogger().Errorf("Failed to publish to dead letter queue: %v", err)
        } else {
            log.GetLogger().Infof("Task %s published to dead letter queue", signature.UUID)
        }
    }
    
    // ... 其余逻辑保持不变
}
```

---

### 2.5 延迟任务事务优化

**问题**: 延迟任务从 ZSET 取出后，如果发布到正常队列失败，任务丢失。

**解决方案**: 使用 Redis 事务确保原子性。

#### 2.5.1 优化后的延迟任务处理

```go
// brokers/redis/goredis.go

// nextDelayedTask 优化版本
func (b *BrokerGR) nextDelayedTaskReliable(key string) (result []byte, err error) {
    pollPeriod := b.GetConfig().DelayedTasksPollPeriod
    if pollPeriod <= 0 {
        pollPeriod = DefaultDelayedTasksPollPeriod
    }
    
    for {
        time.Sleep(pollPeriod)
        
        // 使用 WATCH 确保原子性
        err = b.rclient.Watch(context.Background(), func(tx *redis.Tx) error {
            now := time.Now().UTC().UnixNano()
            ctx := context.Background()
            
            // 获取到期任务
            items, err := tx.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
                Min: "0", Max: strconv.FormatInt(now, 10), Offset: 0, Count: 1,
            }).Result()
            if err != nil {
                return err
            }
            if len(items) == 0 {
                return redis.Nil
            }
            
            // 解析任务
            signature := new(tasks.Signature)
            if err := json.Unmarshal([]byte(items[0]), signature); err != nil {
                return err
            }
            
            // 在事务中：从 ZSET 删除 + 推入正常队列
            _, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
                // 1. 从延迟队列删除
                pipe.ZRem(ctx, key, items[0])
                
                // 2. 直接推入正常队列
                pipe.RPush(ctx, signature.RoutingKey, items[0])
                
                return nil
            })
            
            if err != nil {
                return err
            }
            
            result = []byte(items[0])
            return nil
        }, key)
        
        if err == nil {
            return result, nil
        }
        
        if err == redis.TxFailedErr {
            // 事务冲突，重试
            continue
        }
        
        if err == redis.Nil {
            // 没有到期任务
            return nil, nil
        }
        
        return nil, err
    }
}
```

---

## 三、使用指南

### 3.1 启用可靠性特性

```go
import (
    "time"
    "github.com/redis/go-redis/v9"
    machinery "github.com/RichardKnop/machinery/v2"
    "github.com/RichardKnop/machinery/v2/config"
    machinery_redis "github.com/RichardKnop/machinery/v2/brokers/redis"
)

func main() {
    // 1. 配置
    cnf := &config.Config{
        DefaultQueue:    "machinery_tasks",
        ResultsExpireIn: 3600,
        DefaultMaxRetry: 3,
        
        // 启用死信队列
        DeadLetterQueue:     true,
        DeadLetterQueueName: "my_dead_letter_queue",
        
        UniversalOptions: redis.UniversalOptions{
            Addrs: []string{"localhost:6379"},
        },
    }
    
    // 2. 创建 Redis 客户端（配置重试）
    rdb := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "",
        DB:       0,
        
        // 连接重试配置
        MaxRetries:      3,
        MinRetryBackoff: 8 * time.Millisecond,
        MaxRetryBackoff: 512 * time.Millisecond,
        DialTimeout:     5 * time.Second,
        ReadTimeout:     3 * time.Second,
        WriteTimeout:    3 * time.Second,
    })
    
    // 3. 创建 Server
    broker := machinery_redis.New(cnf, rdb)
    backend := machinery_redis.New(cnf, rdb)
    lock := locks_redis.New(rdb, 3, 100*time.Millisecond)
    
    server := machinery.NewServer(cnf, broker, backend, lock)
    
    // 4. 创建 Worker（带超时和恢复）
    worker := server.NewWorker("reliable_worker", 10)
    worker.TaskTimeout = 5 * time.Minute
    
    // 5. 启动 Worker（自动恢复未完成的任务）
    err := worker.Launch()
    if err != nil {
        log.Fatalf("Worker failed: %v", err)
    }
}
```

### 3.2 任务设计建议

```go
// 任务函数应该：
// 1. 支持幂等执行
// 2. 接受 context.Context 作为第一个参数以支持超时
// 3. 返回适当的错误类型以触发重试

func MyTask(ctx context.Context, arg1 string) (string, error) {
    // 检查 context 是否已取消
    select {
    case <-ctx.Done():
        return "", ctx.Err()
    default:
    }
    
    // 执行业务逻辑...
    return "result", nil
}

// 或者返回重试错误
func MyTaskWithRetry(ctx context.Context, arg1 string) (string, error) {
    err := doSomething()
    if err != nil {
        // 5分钟后重试
        return "", tasks.NewErrRetryTaskLater(err.Error(), 5*time.Minute)
    }
    return "result", nil
}
```

---

## 四、监控指标

### 4.1 关键指标

```go
// 监控函数示例
func MonitorMachinery(broker iface.Broker) {
    // 1. 处理中队列长度
    processingKey := "machinery:processing:default"
    processingTasks, _ := broker.GetPendingTasks(processingKey)
    metrics.GaugeSet("machinery.processing_tasks", len(processingTasks))
    
    // 2. 正常队列长度
    normalTasks, _ := broker.GetPendingTasks("default")
    metrics.GaugeSet("machinery.pending_tasks", len(normalTasks))
    
    // 3. 死信队列长度
    deadLetterTasks, _ := broker.GetDeadLetterTasks("")
    metrics.GaugeSet("machinery.dead_letter_tasks", len(deadLetterTasks))
}
```

### 4.2 告警规则建议

| 指标 | 阈值 | 级别 |
|------|------|------|
| 处理中任务数 | > 1000 | Warning |
| 处理中任务数 | > 5000 | Critical |
| 正常队列长度 | > 10000 | Warning |
| 死信队列增长 | > 10/min | Warning |
| 任务平均处理时间 | > 配置超时 80% | Warning |

---

## 五、实施检查清单

- [ ] 修改 Broker 接口，添加 `Ack` 和 `Reject` 方法
- [ ] 实现 Redis Broker 的 `BRPopLPush` 逻辑
- [ ] 修改 Worker `Process` 方法，调用 `Ack`/`Reject`
- [ ] 实现 Worker 启动时的任务恢复逻辑
- [ ] 添加 Worker `TaskTimeout` 字段
- [ ] 修改 `Process` 方法支持超时控制
- [ ] 配置 `DeadLetterQueue` 支持
- [ ] 实现 Broker 的死信队列发布方法
- [ ] 修改 `taskFailed` 发送失败任务到死信队列
- [ ] 优化延迟任务处理的事务保障
- [ ] 编写相关单元测试
- [ ] 编写集成测试验证可靠性场景

---

## 六、风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| BRPopLPush 性能略低于 BLPop | 低 | 差异很小，可接受 |
| 处理中队列积压 | 中 | 监控 + 告警 |
| 任务恢复导致重复执行 | 中 | 任务设计支持幂等 |
| 向后兼容性 | 低 | 通过接口默认实现保证 |
| 死信队列无限增长 | 中 | 定期清理 + 监控 |

---

## 七、后续优化方向

1. **任务优先级**: 支持任务优先级队列
2. **速率限制**: 控制任务消费速率
3. **任务依赖**: 支持更复杂的工作流
4. **批量处理**: 支持批量任务处理
5. **可观测性**: 增强 Tracing 和 Metrics
