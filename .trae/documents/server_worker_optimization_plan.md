# Server 和 Worker 代码优化计划

## 一、代码审阅结果

### 1.1 server.go 审阅

#### 发现的问题

| 问题类型 | 位置 | 描述 | 风险等级 |
|----------|------|------|----------|
| **Goroutine 泄漏** | L49 | `go srv.scheduler.Run()` - 没有停止机制，Server 关闭时可能泄漏 | 中 |
| **错误处理** | L200 | `SendChain` 只发送第一个任务，没有处理后续任务错误 | 低 |
| **资源未释放** | 无 | Server 没有 `Stop()` 或 `Close()` 方法来释放资源 | 中 |
| **并发安全** | L110-121 | `RegisterTasks` 和 `RegisterTask` 调用 `broker.SetRegisteredTaskNames` 在并发场景下可能有问题 | 低 |

#### 代码注释问题

| 位置 | 当前注释 | 建议修改 |
|------|----------|----------|
| L37 | `// NewServer creates Server instance` | `// NewServer creates a new Server instance with the given configuration and components` |
| L54 | `// NewWorker creates Worker instance` | `// NewWorker creates a new Worker instance` |
| L94 | `// GetConfig returns connection object` | `// GetConfig returns the server configuration` |

---

### 1.2 worker.go 审阅

#### 发现的问题

| 问题类型 | 位置 | 描述 | 风险等级 |
|----------|------|------|----------|
| **⚠️ Panic 风险 - Callback** | L163-164, L168-169 | `preTaskHandler` 和 `postTaskHandler` 调用没有 recover，用户回调 panic 会导致 worker 崩溃 | **高** |
| **错误忽略** | L127-133 | 任务未注册时直接返回 nil，可能隐藏问题 | 低 |
| **错误忽略** | L255 | `broker.Publish` 错误被记录但没有正确处理 | 中 |
| **Goroutine 泄漏** | L67-83 | 消费循环 goroutine 在 worker 退出时可能没有正确清理 | 中 |
| **竞态条件** | L177-189 | 任务重试逻辑中 `signature.RetryCount` 和 `signature.RetryTimeout` 的修改不是线程安全的 | 中 |

#### Worker Callback Panic 风险详细分析

**问题位置：**
```go
// L163-164
if worker.preTaskHandler != nil {
    worker.preTaskHandler(signature)  // 用户回调可能 panic
}

// L168-169
if worker.postTaskHandler != nil {
    defer worker.postTaskHandler(signature)  // 用户回调可能 panic
}
```

**风险描述：**
- 如果用户设置的 `preTaskHandler` 或 `postTaskHandler` 发生 panic，会导致整个 worker 进程崩溃
- 这会影响正在处理的其他任务
- 没有错误恢复机制

**建议修复：**
```go
// 添加 recover 保护
if worker.preTaskHandler != nil {
    func() {
        defer func() {
            if r := recover(); r != nil {
                log.ERROR.Printf("PreTaskHandler panic: %v", r)
            }
        }()
        worker.preTaskHandler(signature)
    }()
}
```

---

## 二、安全风险分析

### 2.1 潜在安全问题

| 问题 | 位置 | 描述 | 建议 |
|------|------|------|------|
| **日志敏感信息** | L63 | Redis 地址被记录，可能包含密码 | 脱敏处理 |
| **任务注入** | L124-131 | 任务注册没有验证任务名称合法性 | 添加验证 |
| **资源耗尽** | L237-268 | SendGroup 使用无界 goroutine 池，可能导致资源耗尽 | 限制并发数 |

### 2.2 错误处理安全问题

- `taskFailed` 方法将错误信息记录到日志，可能包含敏感信息
- 建议：对错误信息进行脱敏处理

---

## 三、性能问题分析

### 3.1 性能瓶颈

| 问题 | 位置 | 描述 | 优化建议 |
|------|------|------|----------|
| **频繁内存分配** | L174 | `taskResults = make([]*TaskResult, len(results)-1)` 每次调用都分配 | 考虑对象池 |
| **反射性能** | L176-177 | `reflect.TypeOf(val).String()` 调用较昂贵 | 缓存类型信息 |
| **锁竞争** | L317-320 | `registeredTasks.Range` 在任务多时会遍历所有任务 | 考虑分区或缓存 |
| **字符串拼接** | L164 | `fmt.Sprintf` 在热路径上 | 使用字符串拼接 |

### 3.2 并发性能

- `SendGroup` 使用 channel 作为信号量，实现正确
- 但 `pool` channel 的填充使用单独的 goroutine (L238-242)，可以优化

---

## 四、优化建议

### 4.1 高优先级（必须修复）

#### 1. 修复 Worker Callback Panic 风险

**文件：** `worker.go`

```go
// 在 Process 方法中添加 recover 保护

// Run handler before the task is called
if worker.preTaskHandler != nil {
    func() {
        defer func() {
            if r := recover(); r != nil {
                log.ERROR.Printf("PreTaskHandler panic recovered: %v\n%s", r, debug.Stack())
            }
        }()
        worker.preTaskHandler(signature)
    }()
}

// Defer run handler for the end of the task
if worker.postTaskHandler != nil {
    defer func() {
        defer func() {
            if r := recover(); r != nil {
                log.ERROR.Printf("PostTaskHandler panic recovered: %v\n%s", r, debug.Stack())
            }
        }()
        worker.postTaskHandler(signature)
    }()
}
```

#### 2. 添加 Server 停止方法

**文件：** `server.go`

```go
// Stop gracefully stops the server and releases resources
func (server *Server) Stop() {
    if server.scheduler != nil {
        server.scheduler.Stop()
    }
}
```

### 4.2 中优先级（建议修复）

#### 3. 优化注释

| 文件 | 位置 | 优化后注释 |
|------|------|------------|
| server.go | L37 | `// NewServer creates a new Server instance with the given configuration and components` |
| server.go | L54 | `// NewWorker creates a new Worker instance for processing tasks` |
| server.go | L64 | `// NewCustomQueueWorker creates a new Worker instance with a custom queue` |
| server.go | L94 | `// GetConfig returns the server configuration` |
| worker.go | L35-38 | 修正 `ErrWorkerQuitAbruptly` 的注释错误（当前两行都是 `ErrWorkerQuitGracefully`） |
| worker.go | L41 | `// Launch starts a new worker process and blocks until the worker stops` |
| worker.go | L51 | `// LaunchAsync starts a new worker process asynchronously` |

#### 4. 修复注释错误

**文件：** `worker.go` L37

```go
// 当前：
// ErrWorkerQuitGracefully is return when worker quit abruptly

// 修正为：
// ErrWorkerQuitAbruptly is returned when worker quits abruptly
```

### 4.3 低优先级（可选优化）

#### 5. 性能优化 - 减少内存分配

**文件：** `task.go`

```go
// 使用 sync.Pool 减少内存分配
var taskResultPool = sync.Pool{
    New: func() interface{} {
        return &TaskResult{}
    },
}
```

#### 6. 日志脱敏

**文件：** `worker.go` L63

```go
// 脱敏 Redis 地址
addrs := make([]string, len(cnf.Addrs))
for i, addr := range cnf.Addrs {
    addrs[i] = RedactURL(addr)
}
log.INFO.Printf("- Redis Addrs: %v", addrs)
```

---

## 五、实施计划

### 阶段 1：安全修复（高优先级）

1. **修复 Worker Callback Panic 风险**
   - 添加 recover 保护
   - 添加 panic 日志记录

2. **添加 Server Stop 方法**
   - 实现资源释放

### 阶段 2：代码质量优化（中优先级）

3. **优化注释**
   - 修正所有注释错误
   - 完善函数注释

4. **修复注释错误**
   - 修正 `ErrWorkerQuitAbruptly` 的注释

### 阶段 3：性能优化（低优先级）

5. **性能优化**
   - 使用对象池
   - 优化字符串拼接

---

## 六、风险评估

| 修改项 | 风险等级 | 影响范围 | 回滚难度 |
|--------|----------|----------|----------|
| Worker Callback Panic 保护 | 低 | Worker 任务处理 | 容易 |
| Server Stop 方法 | 低 | Server 生命周期 | 容易 |
| 注释优化 | 无 | 无 | 容易 |

---

## 七、测试建议

1. **单元测试**
   - 测试 callback panic 被正确捕获
   - 测试 nil config 的处理

2. **集成测试**
   - 测试 Server 启动和停止
   - 测试 Worker 优雅退出

---

*计划更新时间: 2026-04-17*
*更新说明: 移除 addrs、config 等 nil 检查相关建议*
