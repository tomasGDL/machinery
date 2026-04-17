# Machinery v2 Redis 消息可靠性保障方案

> 本文档深入分析了 Machinery v2 使用 Redis 作为 Broker 和 Backend 时可能存在的消息丢失风险，并提供完整的可靠性保障方案。

## 一、消息丢失风险分析

### 1.1 Broker 层的消息丢失场景

#### 场景 1：任务发布时 Redis 故障

**问题代码位置**: `brokers/redis/goredis.go:174-197`

```go
func (b *BrokerGR) Publish(ctx context.Context, signature *tasks.Signature) error {
    msg, err := json.Marshal(signature)
    if err != nil {
        return fmt.Errorf("JSON marshal error: %s", err)
    }
    
    if signature.ETA != nil && signature.ETA.After(time.Now().UTC()) {
        score := signature.ETA.UnixNano()
        err = b.rclient.ZAdd(context.Background(), b.redisDelayedTasksKey, 
            redis.Z{Score: float64(score), Member: msg}).Err()
        return err
    }
    
    err = b.rclient.RPush(context.Background(), signature.RoutingKey, msg).Err()
    return err
}
```

**风险点**:
- `RPush` 操作如果 Redis 在写入后立即崩溃，数据可能未持久化到磁盘
- Redis 默认使用异步持久化（AOF 每秒刷盘或 RDB 快照），存在数据丢失窗口
- 没有确认机制确保消息真正持久化

**丢失时机**:
- 调用方已收到 "发送成功" 响应
- Redis 在数据刷盘前崩溃
- 任务消息永久丢失

---

#### 场景 2：消费者获取消息后 Worker 崩溃

**问题代码位置**: `brokers/redis/goredis.go:315-335`

```go
func (b *BrokerGR) nextTask(queue string) (result []byte, err error) {
    items, err := b.rclient.BLPop(context.Background(), pollPeriod, queue).Result()
    if err != nil {
        return []byte{}, err
    }
    result = []byte(items[1])
    return result, nil
}
```

**风险点**:
- `BLPop` 是破坏性操作，消息一旦从队列弹出就消失
- 消息传递给 Worker 后，如果 Worker 在处理前崩溃，消息丢失
- 没有类似 RabbitMQ 的 Ack 机制

**丢失时机**:
```
时间线:
T1: Worker BLPop 获取消息 ✓
T2: 消息从 Redis 队列删除 ✓
T3: Worker 进程崩溃/被 Kill ✗
T4: 消息永久丢失
```

---

#### 场景 3：延迟任务处理中的竞态条件

**问题代码位置**: `brokers/redis/goredis.go:338-383`

```go
func (b *BrokerGR) nextDelayedTask(key string) (result []byte, err error) {
    watchFunc := func(tx *redis.Tx) error {
        items, err = tx.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
            Min: "0", Max: strconv.FormatInt(now, 10), Offset: 0, Count: 1,
        }).Result()
        // ...
        _, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
            pipe.ZRem(ctx, key, items[0])  // 删除延迟任务
            result = []byte(items[0])
            return nil
        })
        return err
    }
    
    if err = b.rclient.Watch(context.Background(), watchFunc, key); err != nil {
        return
    }
}
```

**风险点**:
- 虽然使用了 WATCH/MULTI/EXEC 事务，但如果从 ZSET 删除后、发布到正常队列前崩溃，任务丢失
- 事务失败重试机制不完善，可能丢失任务

---

#### 场景 4：未注册任务的重新入队

**问题代码位置**: `brokers/redis/goredis.go:289-312`

```go
func (b *BrokerGR) consumeOne(delivery []byte, taskProcessor iface.TaskProcessor) error {
    if !b.IsTaskRegistered(signature.Name) {
        if signature.IgnoreWhenTaskNotRegistered {
            return nil  // 直接忽略，消息丢失
        }
        b.rclient.RPush(context.Background(), getQueueGR(b.GetConfig(), taskProcessor), delivery)
        return nil  // 重新入队，但如果是多个 Worker 竞争，可能无限循环
    }
    return taskProcessor.Process(signature)
}
```

**风险点**:
- `IgnoreWhenTaskNotRegistered = true` 时，消息被直接丢弃
- 重新入队的消息可能被同一个 Worker 再次消费，造成无限循环

---

### 1.2 Backend 层的消息丢失场景

#### 场景 5：状态更新丢失

**问题代码位置**: `backends/redis/goredis.go:259-268`

```go
func (b *BackendGR) updateState(taskState *tasks.TaskState) error {
    encoded, err := json.Marshal(taskState)
    if err != nil {
        return err
    }
    
    expiration := b.getExpiration()
    _, err = b.rclient.Set(context.Background(), taskState.TaskUUID, encoded, expiration).Result()
    return err
}
```

**风险点**:
- 状态更新如果 Redis 崩溃，最新状态可能丢失
- 调用方查询结果时可能得到过期状态

---

#### 场景 6：Chord 触发竞态条件

**问题代码位置**: `backends/redis/goredis.go:99-132`

```go
func (b *BackendGR) TriggerChord(groupUUID string) (bool, error) {
    m := b.redsync.NewMutex("TriggerChordMutex")
    if err := m.Lock(); err != nil {
        return false, err
    }
    defer m.Unlock()
    
    groupMeta, err := b.getGroupMeta(groupUUID)
    // ...
    groupMeta.ChordTriggered = true
    encoded, err := json.Marshal(&groupMeta)
    err = b.rclient.Set(context.Background(), groupUUID, encoded, expiration).Err()
    return true, nil
}
```

**风险点**:
- 分布式锁如果获取失败，可能导致 Chord 不被触发
- 锁持有期间 Redis 故障，可能导致 Chord 重复触发或不触发

---

### 1.3 Worker 层的消息丢失场景

#### 场景 7：Worker 处理中崩溃

**问题代码位置**: `worker.go:125-193`

```go
func (worker *Worker) Process(signature *tasks.Signature) error {
    // 状态更新: RECEIVED
    worker.server.GetBackend().SetStateReceived(signature)
    
    // 状态更新: STARTED
    worker.server.GetBackend().SetStateStarted(signature)
    
    // 执行任务
    results, err := task.Call()
    if err != nil {
        // 失败处理
        return worker.taskFailed(signature, err)
    }
    
    return worker.taskSucceeded(signature, results)
}
```

**风险点**:
- 任务执行中 Worker 崩溃，任务状态停留在 STARTED
- 没有超时机制，任务可能永远处于 STARTED 状态
- 没有恢复机制，重启后无法继续处理

---

## 二、Redis 持久化配置优化

### 2.1 Redis 服务器配置

在 `redis.conf` 中配置以下参数以增强持久化：

```ini
# ========== RDB 持久化配置 ==========
# 更频繁的快照
save 900 1      # 900秒内至少有1个key变化
save 300 10     # 300秒内至少有10个key变化
save 60 10000   # 60秒内至少有10000个key变化

# 快照失败时停止写入（重要！）
stop-writes-on-bgsave-error yes

# RDB 文件压缩
rdbcompression yes
rdbchecksum yes

# RDB 文件名
dbdump.rdb

# ========== AOF 持久化配置 ==========
# 启用 AOF
appendonly yes
appendfilename "appendonly.aof"

# 每秒刷盘（推荐平衡方案）
appendfsync everysec

# 重写时停止刷盘可能导致的数据丢失
no-appendfsync-on-rewrite no

# AOF 文件最小大小（触发自动重写）
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb

# ========== 内存策略 ==========
# 不要使用 volatile-lru 或 allkeys-lru，这会导致数据被驱逐
# 如果内存不足，应该扩展而不是删除数据
maxmemory-policy noeviction

# 设置合理的最大内存限制
# maxmemory <your-memory-limit>

# ========== 其他重要配置 ==========
# 启用 protected-mode 保护数据安全
protected-mode yes

# 绑定到特定 IP
bind 127.0.0.1

# 设置密码
requirepass your-strong-password
```

### 2.2 Redis Sentinel / Cluster 高可用配置

#### Sentinel 配置示例

```ini
# sentinel.conf
sentinel monitor mymaster 127.0.0.1 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
sentinel parallel-syncs mymaster 1

# 保护模式
sentinel auth-pass mymaster your-strong-password
```

#### Go 代码中使用 Sentinel

```go
import (
    "github.com/redis/go-redis/v9"
    "github.com/RichardKnop/machinery/v2/config"
    machinery_redis "github.com/RichardKnop/machinery/v2/brokers/redis"
)

func createRedisClient() redis.UniversalClient {
    // 使用 Sentinel 模式
    rdb := redis.NewFailoverClient(&redis.FailoverOptions{
        MasterName:       "mymaster",
        SentinelAddrs:    []string{":26379", ":26380", ":26381"},
        Password:         "your-strong-password",
        DB:               0,
        MaxRetries:       3,                    // 命令重试次数
        MinRetryBackoff:  8 * time.Millisecond, // 最小重试间隔
        MaxRetryBackoff:  512 * time.Millisecond, // 最大重试间隔
        DialTimeout:      5 * time.Second,      // 连接超时
        ReadTimeout:      3 * time.Second,      // 读取超时
        WriteTimeout:     3 * time.Second,      // 写入超时
        PoolSize:         10,                   // 连接池大小
        PoolTimeout:      4 * time.Second,      // 连接池超时
    })
    
    return rdb
}

func createMachineryServer() *machinery.Server {
    cnf := &config.Config{
        DefaultQueue:    "machinery_tasks",
        ResultsExpireIn: 3600,
        UniversalOptions: redis.UniversalOptions{
            Addrs: []string{":26379", ":26380", ":26381"},
            MasterName: "mymaster",
            Password: "your-strong-password",
        },
    }
    
    broker := machinery_redis.NewGR(cnf, createRedisClient())
    backend := machinery_redis.New(cnf, createRedisClient())
    
    return machinery.NewServer(cnf, broker, backend, nil)
}
```

---

## 三、代码增强方案

### 3.1 消息可靠性增强 - Broker 层

#### 方案 1: 实现消息确认机制

**核心思路**: 使用 Redis List 作为处理中队列，处理完成后才从队列删除

```go
// brokers/redis/goredis.go

// 新增字段
type BrokerGR struct {
    common.Broker
    rclient redis.UniversalClient
    consumingWG sync.WaitGroup
    processingWG sync.WaitGroup
    delayedWG sync.WaitGroup
    redisDelayedTasksKey string
    
    // 新增：处理中队列的 key
    processingQueueKey string
}

// 修改 nextTask：使用 RPOPLPUSH 原子操作
func (b *BrokerGR) nextTaskReliable(queue string) (result []byte, err error) {
    processingKey := b.processingQueueKey + ":" + queue
    
    // 使用 RPOPLPUSH 原子地将消息从队列移动到处理中队列
    items, err := b.rclient.BRPopLPush(
        context.Background(), 
        queue,           // source
        processingKey,   // destination
        pollPeriod,
    ).Result()
    
    if err != nil {
        return []byte{}, err
    }
    
    return []byte(items), nil
}

// 新增：确认消息处理完成
func (b *BrokerGR) AckTask(queue string, signature *tasks.Signature) error {
    processingKey := b.processingQueueKey + ":" + queue
    msg, err := json.Marshal(signature)
    if err != nil {
        return err
    }
    // 从处理中队列删除
    return b.rclient.LRem(context.Background(), processingKey, 1, msg).Err()
}

// 新增：消息处理失败，重新入队
func (b *BrokerGR) RejectTask(queue string, signature *tasks.Signature) error {
    processingKey := b.processingQueueKey + ":" + queue
    msg, err := json.Marshal(signature)
    if err != nil {
        return err
    }
    // 从处理中队列移除并重新放入原队列
    pipe := b.rclient.TxPipeline()
    pipe.LRem(context.Background(), processingKey, 1, msg)
    pipe.LPush(context.Background(), queue, msg)
    _, err = pipe.Exec(context.Background())
    return err
}
```

#### 方案 2: 发布确认机制

```go
// 修改 Publish 方法，增加持久化确认
func (b *BrokerGR) PublishReliable(ctx context.Context, signature *tasks.Signature) error {
    b.Broker.AdjustRoutingKey(signature)
    
    msg, err := json.Marshal(signature)
    if err != nil {
        return fmt.Errorf("JSON marshal error: %s", err)
    }
    
    if signature.ETA != nil {
        now := time.Now().UTC()
        if signature.ETA.After(now) {
            score := signature.ETA.UnixNano()
            
            // 使用事务确保原子性
            pipe := b.rclient.TxPipeline()
            pipe.ZAdd(context.Background(), b.redisDelayedTasksKey, 
                redis.Z{Score: float64(score), Member: msg})
            
            // 记录消息发布日志，用于审计和恢复
            pipe.HSet(context.Background(), "machinery:msg_log", signature.UUID, 
                fmt.Sprintf(`{"queue":"%s","status":"pending","timestamp":%d}`, 
                    signature.RoutingKey, time.Now().UnixNano()))
            
            _, err = pipe.Exec(context.Background())
            return err
        }
    }
    
    // 使用事务：推入队列 + 记录日志
    pipe := b.rclient.TxPipeline()
    pipe.RPush(context.Background(), signature.RoutingKey, msg)
    pipe.HSet(context.Background(), "machinery:msg_log", signature.UUID,
        fmt.Sprintf(`{"queue":"%s","status":"published","timestamp":%d}`,
            signature.RoutingKey, time.Now().UnixNano()))
    
    _, err = pipe.Exec(context.Background())
    return err
}
```

---

### 3.2 消息可靠性增强 - Worker 层

#### 方案 3: Worker 恢复机制

```go
// worker.go

// 新增：启动时恢复未完成的任务
func (worker *Worker) RecoverUnfinishedTasks() error {
    cnf := worker.server.GetConfig()
    broker := worker.server.GetBroker()
    
    // 获取处理中队列的消息
    processingKey := "machinery:processing:" + worker.Queue
    if worker.Queue == "" {
        processingKey = "machinery:processing:" + cnf.DefaultQueue
    }
    
    // 获取所有处理中的消息
    msgs, err := broker.GetPendingTasks(processingKey)
    if err != nil {
        return err
    }
    
    for _, signature := range msgs {
        // 检查任务状态
        state, err := worker.server.GetBackend().GetState(signature.UUID)
        if err != nil {
            log.GetLogger().Warnf("Failed to get state for task %s: %v", signature.UUID, err)
            continue
        }
        
        // 如果状态是 STARTED 或 RECEIVED，说明任务未完成
        if state.State == tasks.StateStarted || state.State == tasks.StateReceived {
            log.GetLogger().Infof("Recovering unfinished task: %s (state: %s)", 
                signature.UUID, state.State)
            
            // 重新发送任务
            _, err := worker.server.SendTask(tasks.CopySignature(signature))
            if err != nil {
                log.GetLogger().Errorf("Failed to recover task %s: %v", signature.UUID, err)
            }
        }
    }
    
    return nil
}

// 修改 LaunchAsync，启动时恢复任务
func (worker *Worker) LaunchAsync(errorsChan chan<- error) {
    cnf := worker.server.GetConfig()
    broker := worker.server.GetBroker()
    
    // 恢复未完成的任务
    if err := worker.RecoverUnfinishedTasks(); err != nil {
        log.GetLogger().Warnf("Failed to recover unfinished tasks: %v", err)
    }
    
    // ... 原有启动逻辑
}
```

#### 方案 4: 任务超时机制

```go
// worker.go

// 新增字段
type Worker struct {
    server            *Server
    ConsumerTag       string
    Concurrency       int
    Queue             string
    errorHandler      func(err error)
    preTaskHandler    func(*tasks.Signature)
    postTaskHandler   func(*tasks.Signature)
    preConsumeHandler func(*Worker) bool
    
    // 新增：任务超时时间
    TaskTimeout time.Duration
}

// 修改 Process 方法，增加超时控制
func (worker *Worker) ProcessWithContext(ctx context.Context, signature *tasks.Signature) error {
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
    
    // 创建带超时的 context
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
    
    // 使用通道来捕获超时
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
    
    select {
    case <-ctx.Done():
        // 超时
        if ctx.Err() == context.DeadlineExceeded {
            log.GetLogger().Errorf("Task %s timed out after %v", 
                signature.UUID, worker.TaskTimeout)
            return worker.taskFailed(signature, fmt.Errorf("task timeout after %v", 
                worker.TaskTimeout))
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

---

### 3.3 消息可靠性增强 - Backend 层

#### 方案 5: 状态更新事务保障

```go
// backends/redis/goredis.go

// 修改 updateState，增加事务保障
func (b *BackendGR) updateStateReliable(taskState *tasks.TaskState) error {
    encoded, err := json.Marshal(taskState)
    if err != nil {
        return err
    }
    
    expiration := b.getExpiration()
    
    // 使用 WATCH 确保状态更新的原子性
    for retries := 0; retries < 3; retries++ {
        err = b.rclient.Watch(context.Background(), func(tx *redis.Tx) error {
            // 执行状态更新
            _, err := tx.Set(context.Background(), taskState.TaskUUID, encoded, expiration).Result()
            return err
        }, taskState.TaskUUID)
        
        if err == nil {
            return nil // 成功
        }
        
        if err == redis.TxFailedErr {
            continue // 重试
        }
        
        return err // 其他错误
    }
    
    return fmt.Errorf("failed to update state after 3 retries: %w", err)
}
```

#### 方案 6: 结果备份机制

```go
// backends/redis/goredis.go

// 新增：设置状态同时写入备份
func (b *BackendGR) SetStateSuccessWithBackup(signature *tasks.Signature, results []*tasks.TaskResult) error {
    taskState := tasks.NewSuccessTaskState(signature, results)
    b.mergeNewTaskState(taskState)
    
    encoded, err := json.Marshal(taskState)
    if err != nil {
        return err
    }
    
    expiration := b.getExpiration()
    
    // 使用 Pipeline 同时写入主存储和备份
    pipe := b.rclient.TxPipeline()
    pipe.Set(context.Background(), taskState.TaskUUID, encoded, expiration)
    
    // 写入备份 hash，按状态分类
    backupKey := "machinery:backup:" + taskState.State
    pipe.HSet(context.Background(), backupKey, taskState.TaskUUID, encoded)
    
    // 设置备份的过期时间（通常比主存储更长）
    backupExpiration := expiration * 2
    pipe.Expire(context.Background(), backupKey, backupExpiration)
    
    _, err = pipe.Exec(context.Background())
    return err
}
```

---

## 四、配置参数优化建议

### 4.1 Machinery 配置

```go
import (
    "time"
    "github.com/redis/go-redis/v9"
    "github.com/RichardKnop/machinery/v2/config"
)

func CreateReliableConfig() *config.Config {
    return &config.Config{
        // 基本配置
        DefaultQueue:    "machinery_tasks",
        ResultsExpireIn: 7200,  // 2小时，增加保留时间
        TaskPrefix:      "task_",
        DefaultMaxRetry: 5,     // 增加默认重试次数
        
        // Redis 连接配置
        UniversalOptions: redis.UniversalOptions{
            Addrs: []string{":26379", ":26380", ":26381"},
            MasterName: "mymaster",
            Password: "your-strong-password",
            DB: 0,
            
            // 连接池配置
            MaxIdleConns:     10,
            MaxActiveConns:   100,
            
            // 超时配置
            DialTimeout:      5 * time.Second,
            ReadTimeout:      3 * time.Second,
            WriteTimeout:     3 * time.Second,
            
            // 重试配置
            MaxRetries:       3,
            MinRetryBackoff:  8 * time.Millisecond,
            MaxRetryBackoff:  512 * time.Millisecond,
        },
        
        // 任务轮询配置
        NormalTasksPollPeriod:  1 * time.Second,
        DelayedTasksPollPeriod: 200 * time.Millisecond,  // 更频繁检查延迟任务
        DelayedTasksKey:        "machinery:delayed_tasks",
    }
}
```

### 4.2 Worker 配置

```go
func CreateReliableWorker(server *machinery.Server) *machinery.Worker {
    worker := server.NewWorker("reliable_worker", 10)
    
    // 设置错误处理器
    worker.SetErrorHandler(func(err error) {
        log.GetLogger().Errorf("Worker error: %v", err)
        // 可以在这里添加告警逻辑
    })
    
    // 设置预处理处理器
    worker.SetPreConsumeHandler(func(worker *machinery.Worker) bool {
        // 可以在这里添加健康检查
        return true
    })
    
    return worker
}
```

---

## 五、监控与告警

### 5.1 关键指标监控

```go
// 监控指标
type MachineryMetrics struct {
    // 队列长度
    QueueLength int
    
    // 处理中任务数
    ProcessingTasks int
    
    // 延迟任务数
    DelayedTasks int
    
    // 失败任务数
    FailedTasks int
    
    // 任务平均处理时间
    AvgProcessingTime time.Duration
    
    // Redis 连接数
    RedisConnections int
}

// 定期检查函数
func CheckMachineryHealth(broker iface.Broker, backend iface.Backend) error {
    // 检查队列积压
    pendingTasks, err := broker.GetPendingTasks("")
    if err != nil {
        return fmt.Errorf("failed to get pending tasks: %w", err)
    }
    
    if len(pendingTasks) > 1000 {
        log.GetLogger().Warnf("Queue backlog warning: %d pending tasks", len(pendingTasks))
        // 发送告警
    }
    
    return nil
}
```

### 5.2 健康检查端点

```go
import "net/http"

func HealthHandler(broker iface.Broker, backend iface.Backend) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // 检查 Redis 连接
        ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
        defer cancel()
        
        // ... 健康检查逻辑
        w.WriteHeader(http.StatusOK)
        w.Write([]byte(`{"status":"healthy"}`))
    }
}
```

---

## 六、部署建议

### 6.1 Redis 高可用架构

```
┌─────────────────────────────────────────┐
│          Redis Sentinel Cluster         │
│                                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐│
│  │Sentinel1│  │Sentinel2│  │Sentinel3││
│  └─────────┘  └─────────┘  └─────────┘│
└─────────────────────────────────────────┘
         │              │              │
┌────────▼──────────────▼──────────────▼────────┐
│            Redis Master-Slave                  │
│                                                │
│  ┌──────────┐     ┌──────────┐  ┌──────────┐ │
│  │  Master  │────▶│  Slave1  │  │  Slave2  │ │
│  │ (读写)   │ 同步 │  (只读)  │  │  (只读)  │ │
│  └──────────┘     └──────────┘  └──────────┘ │
│       │                                      │
│  ┌────▼─────┐                                │
│  │  AOF +   │                                │
│  │   RDB    │                                │
│  └──────────┘                                │
└──────────────────────────────────────────────┘
```

### 6.2 Machinery 部署架构

```
┌──────────────────────────────────────────────┐
│            Producer Application              │
│                                              │
│  ┌────────────┐     ┌────────────┐          │
│  │  Server    │────▶│   Broker   │          │
│  │            │     │            │          │
│  └────────────┘     └────────────┘          │
└──────────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────┐
│              Redis Cluster                   │
└──────────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────┐
│            Worker Application                │
│                                              │
│  ┌────────────┐     ┌────────────┐          │
│  │   Worker   │◀────│   Broker   │          │
│  │            │     │            │          │
│  └────────────┘     └────────────┘          │
│         │                                   │
│         ▼                                   │
│  ┌────────────┐                             │
│  │  Backend   │                             │
│  └────────────┘                             │
└──────────────────────────────────────────────┘
```

---

## 七、总结与最佳实践

### 7.1 当前问题总结

| 风险场景 | 严重程度 | 影响范围 | 解决方案 |
|----------|----------|----------|----------|
| RPush 后 Redis 崩溃 | 高 | 任务丢失 | AOF everysec + 消息日志 |
| BLPop 后 Worker 崩溃 | 高 | 任务丢失 | RPOPLPUSH + 确认机制 |
| 延迟任务事务失败 | 中 | 任务丢失 | 完善事务重试 |
| 状态更新丢失 | 中 | 结果不准确 | WATCH 事务 + 备份 |
| Chord 触发失败 | 中 | 回调不执行 | 分布式锁优化 |
| Worker 处理中超时 | 高 | 任务卡住 | 超时机制 + 恢复 |
| 未注册任务被忽略 | 低 | 任务丢失 | 谨慎使用 IgnoreWhenTaskNotRegistered |

### 7.2 最佳实践清单

- [ ] **Redis 持久化**: 启用 AOF + RDB，配置 `appendfsync everysec`
- [ ] **高可用**: 使用 Redis Sentinel 或 Cluster
- [ ] **连接重试**: 配置 `MaxRetries` 和合理的重试间隔
- [ ] **消息确认**: 实现类似 ACK 的确认机制
- [ ] **任务恢复**: Worker 启动时恢复未完成的任务
- [ ] **超时控制**: 为任务设置合理的超时时间
- [ ] **重试策略**: 配置合理的 `RetryCount` 和退避策略
- [ ] **监控告警**: 监控队列长度、失败任务数等关键指标
- [ ] **备份机制**: 定期备份 Redis 数据
- [ ] **优雅关闭**: 确保 Worker 优雅退出，完成任务后再关闭
- [ ] **幂等性**: 任务设计要支持幂等，防止重复执行
- [ ] **死信队列**: 实现死信队列处理最终失败的任务

### 7.3 立即可以做的优化

1. **修改 Redis 配置**（无需改代码）
   ```bash
   # 在 redis.conf 中
   appendonly yes
   appendfsync everysec
   maxmemory-policy noeviction
   ```

2. **增加重试配置**（最小改动）
   ```go
   cnf := &config.Config{
       DefaultMaxRetry: 5,
       UniversalOptions: redis.UniversalOptions{
           MaxRetries: 3,
           // ...
       },
   }
   ```

3. **添加 Worker 恢复逻辑**（中等改动）
   - 参考方案 3 的实现

4. **实现完整的消息确认机制**（较大改动）
   - 参考方案 1 和方案 2 的实现

---

## 八、替代方案考虑

如果 Redis 的可靠性无法满足需求，可以考虑：

1. **RabbitMQ**: 
   - 原生支持消息确认（ACK）
   - 支持消息持久化
   - 支持死信队列
   - 更强的可靠性保障

2. **Apache Kafka**:
   - 高吞吐量
   - 强持久化保证
   - 支持消息重放

3. **AWS SQS / 阿里云消息队列**:
   - 托管服务，高可用
   - 内置消息确认和重试
   - 降低运维成本

