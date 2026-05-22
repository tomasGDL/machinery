# Runner

Runner 是一个高性能、Lock-Free 的消息调度执行框架，专为解决**队头阻塞（Head-of-Line Blocking）**和**资源约束并发**而设计。它通过双批处理队列（Pre/Post Batcher）和引用计数机制，实现消息的批量预处理、并发执行和后置处理。

## 目录

- [核心特性](#核心特性)
- [架构设计](#架构设计)
- [消息流转流程](#消息流转流程)
- [约束机制](#约束机制)
- [配置说明](#配置说明)
- [使用示例](#使用示例)
- [性能调优](#性能调优)

---

## 核心特性

| 特性 | 说明 |
|------|------|
| **Lock-Free 设计** | 所有可变状态由单 goroutine 串行管理，通过 channel 传递事件，无锁竞争 |
| **队头阻塞缓解** | `blockBuffer` 提供消息 peek 能力，可跳过冲突消息调度就绪消息 |
| **资源约束并发** | 通过 `ResourceLimits` 限制特定资源的并发使用数 |
| **双批处理队列** | Pre-Batcher（预处理）+ Post-Batcher（后置处理），降低下游压力 |
| **优雅关闭** | 支持 `Stop()` 等待所有 worker 和 batcher 完成后再退出 |
| **全链路 panic 防护** | 所有 goroutine 和回调函数均有 `recover()` 保护 |

---

## 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              外部生产者                                   │
│                         (MQ Consumer / HTTP API)                         │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ Process()
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              Runner 内部                                  │
│                                                                         │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────────────────┐   │
│  │  recvChan   │────>│ blockBuffer │────>│      preBatcher         │   │
│  │   (解耦)     │     │  (HOL缓冲)   │     │  (批量预处理: 32×2=64)   │   │
│  │   容量: 8    │     │  容量: 256   │     │  FlushDelay: 100ms      │   │
│  └─────────────┘     └─────────────┘     └───────────┬─────────────┘   │
│                                                      │                  │
│                                                      │ wrapPreBatchFn   │
│                                                      ▼                  │
│                                           ┌─────────────────┐          │
│                                           │    msgChan      │          │
│                                           │   (worker缓冲)   │          │
│                                           │    容量: 16      │          │
│                                           └────────┬────────┘          │
│                                                    │                    │
│              ┌─────────────────────────────────────┘                    │
│              ▼                                                          │
│  ┌─────────────────────────┐    Concurrency: 512    ┌────────────────┐ │
│  │      handleEventLoop    │<──── worker goroutine ──>│   ProcessFn    │ │
│  │      (调度执行)          │                        │  (业务逻辑)     │ │
│  └───────────┬─────────────┘                        └────────────────┘ │
│              │                                                          │
│              │ postBatcher.Send()                                        │
│              ▼                                                          │
│  ┌─────────────────────────┐     ┌─────────────────────────────────┐   │
│  │      postBatcher        │────>│         refFlushChan            │   │
│  │  (批量后置处理: 64×2=128) │     │      (引用释放事件)              │   │
│  │  FlushDelay: 200ms      │     │         容量: 128               │   │
│  └─────────────────────────┘     └─────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     handleReceiveLoop (单 goroutine)              │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐   │   │
│  │  │ blockBuffer │  │ references  │  │     scheduleTimer       │   │   │
│  │  │ (状态管理)   │  │ (引用计数)   │  │    (退避重试定时器)       │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 关键组件

#### 2.2.1 `blockBuffer` — 队头阻塞缓冲

```go
type bufferedMsg struct {
    msg   MessageContext
    valid bool  // false 表示已调度，等待压缩
}
```

- **作用**：缓存消息，提供 peek 能力，解决队头阻塞
- **容量**：256（与 Concurrency 无强制关联，根据冲突密度设定）
- **调度策略**：`SelectRunables()` 遍历 blockBuffer，跳过冲突消息，选择就绪消息
- **压缩策略**：`pendingCompact > len(blockBuffer)/4` 时触发 `compactBuffer()`，移除已调度消息

#### 2.2.2 `references` — 引用计数

```go
references map[string]int
```

- **作用**：跟踪正在被处理的消息的 EntryID 和 Label 资源占用
- **mark**：消息进入 preBatcher 时，标记 EntryID 和 Labels
- **unmark**：消息从 postBatcher 释放时，解除标记
- **线程安全**：仅在 `handleReceiveLoop` 中访问，Lock-Free

#### 2.2.3 `preBatcher` / `postBatcher` — 双批处理队列

| 属性 | preBatcher | postBatcher |
|------|-----------|-------------|
| 作用 | 批量预处理（DB查询、权限校验） | 批量后置处理（状态更新、通知） |
| MaxBatching | 32 | 64 |
| MaxPendingMessages | 2 | 2 |
| FlushDelay | 100ms | 200ms |
| 在途消息上限 | 64 | 128 |

---

## 消息流转流程

### 3.1 完整链路

```
外部生产者
    │
    ▼ Process(ctx, msgCtx)
┌─────────────┐
│  recvChan   │  ──> 解耦缓冲，容量 8
│  (8/8 满时  │      写入阻塞，天然背压)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ blockBuffer │  ──> HOL 缓冲，容量 256
│ (SelectRun  │      遍历选择就绪消息)
│ ables 扫描) │
└──────┬──────┘
       │
       ▼ trySchedule()
┌─────────────┐
│ preBatcher  │  ──> 批量预处理，最多 64 条在途
│ SendAsync() │      满时触发退避定时器
└──────┬──────┘
       │ wrapPreBatchFn (batcher goroutine)
       ▼
┌─────────────┐
│   msgChan   │  ──> worker 缓冲，容量 16
│ (满时阻塞   │      preBatcher 回调)
└──────┬──────┘
       │
       ▼ worker goroutine (512 并发)
┌─────────────┐
│  ProcessFn  │  ──> 业务逻辑执行
│ (用户定义)   │      执行耗时 = T_process
└──────┬──────┘
       │
       ▼ postBatcher.Send()
┌─────────────┐
│ postBatcher │  ──> 批量后置处理，最多 128 条在途
│             │      FlushDelay: 200ms
└──────┬──────┘
       │ wrapPostBatchFn (batcher goroutine)
       ▼
┌─────────────┐
│ refFlushChan│  ──> 引用释放事件，容量 128
│             │      触发 unmark()
└──────┬──────┘
       │
       ▼ handleReceiveLoop
┌─────────────┐
│  doPostFl   │  ──> 释放引用，触发 OnPostFlushFn
│   ushed()   │      MQ 可在此 ack
└─────────────┘
```

### 3.2 调度时序

```
时间轴 ──────────────────────────────────────────────────────────────>

外部消息:  [A]  [B]  [C]  [D]  [E]  [F]  [G]  [H]  ...
           │    │    │    │    │    │    │    │
           ▼    ▼    ▼    ▼    ▼    ▼    ▼    ▼
recvChan:  [A]  [B]  [C]  [D]  [E]  [F]  [G]  [H]  (容量 8，快速转移)
           │    │    │    │    │    │    │    │
           ▼    ▼    ▼    ▼    ▼    ▼    ▼    ▼
blockBuffer:[A]  [B]  [C]  [D]  [E]  [F]  [G]  [H]  ... (容量 256)
            │    │    │    │    │    │    │    │
            ▼    ▼    ▼    ▼    ▼    ▼    ▼    ▼
SelectRunables():
  - A: 就绪 ──> 调度 ──> mark(A) ──> preBatcher
  - B: 与 A 冲突(资源X满) ──> 跳过
  - C: 就绪 ──> 调度 ──> mark(C) ──> preBatcher
  - D: 与 A 冲突 ──> 跳过
  - E: 就绪 ──> 调度 ──> mark(E) ──> preBatcher
  - ...

preBatcher: [A,C,E,...] ──> Flush (32条或100ms) ──> PreBatchFn()
            │
            ▼
msgChan:    [A]  [C]  [E]  ... (容量 16)
            │    │    │
            ▼    ▼    ▼
worker:     [A]  [C]  [E]  ... (Concurrency=512 并发)
            │    │    │
            ▼    ▼    ▼
ProcessFn:  A────C────E──── ... (执行耗时 T)
            │    │    │
            ▼    ▼    ▼
postBatch:  [A]  [C]  [E]  ... ──> Flush (64条或200ms)
            │
            ▼
refFlush:   [A]  ──> unmark(A) ──> OnPostFlushFn(A) ──> MQ ack(A)
```

### 3.3 退避重试机制

```
trySchedule():
  for msg in blockBuffer:
    if preBatcher.SendAsync(msg) == false:
      # preBatcher 满（pending 64 条）
      scheduleTimer = 10ms 后退避
      break  # 停止调度

10ms 后:
  handleReceiveLoop 被唤醒
  trySchedule() 重试
```

---

## 约束机制

### 4.1 约束类型

| 约束 | 配置项 | 说明 |
|------|--------|------|
| **唯一执行约束** | `UniqueEntryRunning` | 相同 EntryID 的消息串行执行 |
| **资源限制约束** | `ResourceLimits` | 特定资源的最大并发数 |

### 4.2 约束检查流程

```go
func (m *Runner) runable(ider batchqueue.Identifier, references map[string]int) (map[string]int, bool) {
    // 1. 唯一执行约束
    if m.UniqueEntryRunning {
        if m.references[ider.EntryID()] > 0 {
            return references, false  // 相同 EntryID 正在执行
        }
    }
    
    // 2. 资源限制约束
    for _, label := range ider.Labels() {
        limit := m.ResourceLimits[Resource(label.Name)]
        current := m.references[label.Value]
        if current >= limit {
            return references, false  // 资源已满
        }
    }
    
    return references, true  // 就绪
}
```

### 4.3 约束对吞吐量的影响

```
场景：消息争抢同一资源（如数据库连接）

无约束时：
  吞吐量 = Concurrency / T_process = 512 / T

有 ResourceLimits={"db_conn": 10} 时：
  吞吐量 = min(Concurrency, 10) / T_process = 10 / T

BlockSize 的作用：
  BlockSize=256 时，可以向后扫描 256 条消息，找到不冲突的就绪消息
  若 256 条全部冲突，则吞吐量 = 10 / T（资源限制成为瓶颈）
```

---

## 配置说明

### 5.1 默认配置

```go
const (
    DefaultRecvSize                  = 8
    DefaultBlockSize                 = 256
    DefaultConcurrency               = 512
    DefaultMsgChanSize               = 16
    DefaultReferenceSize             = 128
    DefaultPreMaxBatching            = 32
    DefaultPostMaxBatching           = 64
    DefaultPreMaxPendingMessages     = 2
    DefaultPostMaxPendingMessages    = 2
    DefaultPreBatchingMaxFlushDelay  = 100 * time.Millisecond
    DefaultPostBatchingMaxFlushDelay = 200 * time.Millisecond
    DefaultScheduleBackoff           = 10 * time.Millisecond
)
```

### 5.2 配置项详解

| 配置项 | 默认值 | 说明 | 调优建议 |
|--------|--------|------|----------|
| `Concurrency` | 512 | worker 最大并发数 | 根据 CPU 和下游承载能力设定 |
| `BlockSize` | 256 | HOL 缓冲容量 | 根据消息冲突密度设定，与 Concurrency 无关 |
| `RecvSize` | 8 | 接收缓冲 | 固定小值，纯解耦 |
| `MsgChanSize` | 16 | worker 缓冲 | 固定小值，纯解耦 |
| `PreMaxBatching` | 32 | 预处理批次大小 | 小批量快速处理 |
| `PreMaxPendingMessages` | 2 | 预处理 pending 数 | 控制下游压力 |
| `PostMaxBatching` | 64 | 后置处理批次大小 | 小批量快速处理 |
| `PostMaxPendingMessages` | 2 | 后置处理 pending 数 | 控制下游压力 |

### 5.3 中间状态估算

```
Concurrency = 512 时：

recvChan:        8
blockBuffer:     256
preBatcher:      32 × 2 = 64
msgChan:         16
worker:          512
postBatcher:     64 × 2 = 128
─────────────────────────
总计:            984

精简配置（RecvSize=0, MsgChanSize=0）：
总计 ≈ 256 + 512 + 64 + 128 = 960
```

---

## 使用示例

### 6.1 基础使用

```go
package main

import (
    "context"
    "fmt"
    "time"
    
    "github.com/RichardKnop/machinery/v2/extend/batchqueue"
    "github.com/RichardKnop/machinery/v2/extend/runner"
)

// 实现 MessageContext 接口
type MyMessage struct {
    batchqueue.UnimplementedIder
    ID      string
    Payload string
}

func (m *MyMessage) EntryID() string { return m.ID }
func (m *MyMessage) Labels() []batchqueue.Label { return nil }
func (m *MyMessage) Duplicate() batchqueue.Identifier { return m }
func (m *MyMessage) AppendLogs(...string) {}
func (m *MyMessage) Elapsed() time.Duration { return 0 }
func (m *MyMessage) Context() context.Context { return context.Background() }
func (m *MyMessage) IsRetry() bool { return false }

func main() {
    config := &runner.RunnerConfig{
        Name:        "MY_RUNNER",
        Concurrency: 128,
        BlockSize:   256,
        
        // 业务处理逻辑
        ProcessFn: func(msgCtx runner.MessageContext) {
            msg := msgCtx.(*MyMessage)
            fmt.Printf("Processing: %s\n", msg.Payload)
            time.Sleep(100 * time.Millisecond) // 模拟处理
        },
        
        // 预处理：批量查询数据库
        PreBatchFn: func(msgs []interface{}) ([]batchqueue.Identifier, error) {
            fmt.Printf("PreBatch: %d messages\n", len(msgs))
            return nil, nil
        },
        
        // 后置处理：批量更新状态
        PostBatchFn: func(msgs []interface{}) ([]batchqueue.Identifier, error) {
            fmt.Printf("PostBatch: %d messages\n", len(msgs))
            iders := make([]batchqueue.Identifier, len(msgs))
            for i, msg := range msgs {
                iders[i] = msg.(batchqueue.Identifier)
            }
            return iders, nil
        },
        
        // 处理完成回调：可用于 MQ ack
        OnPostFlushFn: func(ider batchqueue.Identifier) {
            fmt.Printf("Completed: %s\n", ider.EntryID())
        },
    }
    
    r := runner.NewRunner(config)
    r.Start()
    defer r.Stop()
    
    // 投递消息
    for i := 0; i < 1000; i++ {
        msg := &MyMessage{
            ID:      fmt.Sprintf("msg-%d", i),
            Payload: fmt.Sprintf("payload-%d", i),
        }
        r.Process(context.Background(), msg)
    }
    
    time.Sleep(10 * time.Second)
}
```

### 6.2 资源约束示例

```go
config := &runner.RunnerConfig{
    Name:        "DB_CONSTRAINED_RUNNER",
    Concurrency: 512,
    BlockSize:   256,
    
    // 限制数据库连接并发数为 10
    ResourceLimits: map[runner.Resource]int{
        "db_conn": 10,
    },
    
    // 消息携带资源标签
    ProcessFn: func(msgCtx runner.MessageContext) {
        // 处理逻辑...
    },
}
```

消息实现：

```go
func (m *MyMessage) Labels() []batchqueue.Label {
    return []batchqueue.Label{
        {Name: "db_conn", Value: "primary_db"},
    }
}
```

### 6.3 MQ Consumer 集成

```go
func runMQConsumer(r *runner.Runner, mqChan <-chan Message) {
    for msg := range mqChan {
        // Runner 提供背压：recvChan 满时 Process() 阻塞
        err := r.Process(context.Background(), msg)
        if err != nil {
            // 处理失败，nack 或重试
            msg.Nack()
            continue
        }
        // 不 ack！等待 OnPostFlushFn 回调
    }
}

// 在 OnPostFlushFn 中 ack
config.OnPostFlushFn = func(ider batchqueue.Identifier) {
    msg := findMsgByID(ider.EntryID())
    msg.Ack()
}
```

---

## 性能调优

### 7.1 调优 checklist

| 指标 | 检查项 | 优化方向 |
|------|--------|----------|
| **CPU** | `Concurrency` 是否超过 CPU 核数 × 2 | 降低 Concurrency |
| **内存** | `blockBuffer` 是否频繁压缩 | 增大 BlockSize 或降低冲突 |
| **延迟** | `PreBatchingMaxFlushDelay` 是否过大 | 降低至 50-100ms |
| **吞吐** | `preBatcher.SendAsync()` 是否频繁返回 false | 增大 MaxPendingMessages |
| **下游** | DB/Redis 是否成为瓶颈 | 降低 Concurrency 或 ResourceLimits |

### 7.2 监控指标

```go
metrics := runner.Metrics()
fmt.Printf("blockBuffer: %d\n", metrics.BlockBufferSize)
fmt.Printf("recvChan: %d\n", metrics.RecvChanSize)
fmt.Printf("references: %d\n", metrics.ReferencesCount)
fmt.Printf("preBatcher: %d\n", metrics.PreBatcherSize)
fmt.Printf("postBatcher: %d\n", metrics.PostBatcherSize)
```

### 7.3 常见问题

**Q: `blockBuffer` 频繁压缩怎么办？**
A: 增大 `BlockSize` 或降低消息冲突密度（如拆分资源标签）。

**Q: `preBatcher.SendAsync()` 频繁返回 false？**
A: 增大 `PreMaxPendingMessages` 或降低 `PreBatchingMaxFlushDelay`。

**Q: 消息处理延迟高？**
A: 检查 `ProcessFn` 耗时，或降低 `Pre/PostBatchingMaxFlushDelay`。

**Q: 内存占用高？**
A: 降低 `BlockSize` 和 `Concurrency`，或缩短 FlushDelay。

---

## 设计哲学

1. **Lock-Free 优先**：所有可变状态由单 goroutine 管理，消除锁竞争
2. **背压传递**：recvChan 满时阻塞写入，天然背压到上游生产者
3. **小批量快速**：pre/post Batcher 采用小批量 + 短延迟，减少中间状态
4. **约束驱动**：通过 `ResourceLimits` 和 `UniqueEntryRunning` 精确控制并发
5. **业务无关**：Runner 只负责调度执行，重试/反馈由上层业务实现
