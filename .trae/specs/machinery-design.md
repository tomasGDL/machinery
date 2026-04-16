# Machinery 设计文档

## 1. 项目概述

### 1.1 项目简介

**Machinery** 是一个基于分布式消息传递的异步任务队列/作业队列库，使用 Go 语言开发。它提供了一种简洁而强大的方式来处理异步任务，支持任务重试、任务结果存储、工作流编排等特性。

### 1.2 核心特性

- **异步任务执行**: 将耗时的操作放入队列异步执行
- **任务重试机制**: 支持基于 Fibonacci 序列的自动重试
- **任务状态追踪**: 完整记录任务生命周期状态
- **工作流编排**: 支持 Chain（链式）、Group（并行）、Chord（带回调的并行）工作流
- **定时任务**: 基于 cron 表达式的周期性任务调度
- **分布式锁**: 确保定时任务的原子性执行
- **多 broker 支持**: AMQP、Redis、SQS、GCP PubSub
- **多 backend 支持**: Redis、Memcache、MongoDB、DynamoDB、AMQP
- **分布式追踪**: 集成 OpenTracing

## 2. 架构设计

### 2.1 整体架构

```
                                    ┌─────────────────┐
                                    │   Application   │
                                    └────────┬────────┘
                                             │
                                    ┌────────▼────────┐
                                    │     Server      │
                                    │  (任务注册/发送)  │
                                    └────────┬────────┘
                                             │
            ┌───────────────────┬─────────────┼─────────────┬───────────────────┐
            │                   │             │             │                   │
   ┌────────▼────────┐  ┌────────▼──────┐  │  ┌──────────▼─────────┐  ┌────────▼────────┐
   │     Broker      │  │    Backend    │  │  │    Scheduler       │  │      Lock       │
   │ (消息代理/队列)  │  │  (结果存储)    │  │  │   (定时调度)        │  │   (分布式锁)     │
   └────────┬────────┘  └────────▲──────┘  │  └────────────────────┘  └──────────────────┘
            │                     │          │
            ▼                     │          │
   ┌─────────────────┐            │          │
   │ Message Broker  │◄───────────┘          │
   │ AMQP/Redis/SQS  │                       │
   │ /GCP PubSub     │                       │
   └─────────────────┘                       │
            │                                │
            ▼                                │
   ┌─────────────────┐                       │
   │     Worker      │◄──────────────────────┘
   │   (任务执行)     │
   └─────────────────┘
```

### 2.2 核心组件

#### Server
- 负责任务注册、任务发送、工作流编排
- 持有配置、broker、backend、lock 的引用
- 提供 SendTask、SendGroup、SendChain、SendChord 等 API

#### Worker
- 负责从 broker 消费任务并执行
- 支持并发控制、信号处理（优雅退出）
- 负责任务状态更新和回调触发

#### Broker
- 消息代理接口，抽象不同消息队列的实现
- 负责任务发布和消费
- 支持延迟任务、任务路由

#### Backend
- 结果后端接口，抽象不同存储的实现
- 负责存储任务状态和结果
- 支持状态查询、组任务完成检查

#### Lock
- 分布式锁接口
- 确保定时任务不会重复执行

### 2.3 消息流程

1. **任务发送**: Client → Server.SendTask() → Broker.Publish() → Message Broker
2. **任务消费**: Message Broker → Broker.Consume() → Worker.Process() → Task.Call()
3. **状态更新**: Worker → Backend.SetStateXxx() → Result Storage
4. **结果获取**: Client → Server → Backend.GetState() → Result

## 3. 接口定义

### 3.1 Broker 接口

```go
type Broker interface {
    GetConfig() *config.Config
    SetRegisteredTaskNames(names []string)
    IsTaskRegistered(name string) bool
    StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
    StopConsuming()
    Publish(ctx context.Context, task *tasks.Signature) error
    GetPendingTasks(queue string) ([]*tasks.Signature, error)
    GetDelayedTasks() ([]*tasks.Signature, error)
    AdjustRoutingKey(s *tasks.Signature)
}
```

### 3.2 Backend 接口

```go
type Backend interface {
    // Group 相关
    InitGroup(groupUUID string, taskUUIDs []string) error
    GroupCompleted(groupUUID string, groupTaskCount int) (bool, error)
    GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error)
    TriggerChord(groupUUID string) (bool, error)

    // 状态管理
    SetStatePending(signature *tasks.Signature) error
    SetStateReceived(signature *tasks.Signature) error
    SetStateStarted(signature *tasks.Signature) error
    SetStateRetry(signature *tasks.Signature) error
    SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error
    SetStateFailure(signature *tasks.Signature, err string) error
    GetState(taskUUID string) (*tasks.TaskState, error)

    // 清理
    IsAMQP() bool
    PurgeState(taskUUID string) error
    PurgeGroupMeta(groupUUID string) error
}
```

### 3.3 Lock 接口

```go
type Lock interface {
    LockWithRetries(key string, value int64) error
    Lock(key string, value int64) error
}
```

### 3.4 TaskProcessor 接口

```go
type TaskProcessor interface {
    Process(signature *tasks.Signature) error
    CustomQueue() string
    PreConsumeHandler() bool
}
```

## 4. 数据结构

### 4.1 Signature（任务签名）

```go
type Signature struct {
    UUID           string
    Name           string
    RoutingKey     string
    ETA            *time.Time              // 延迟执行时间
    GroupUUID      string                  // 所属组ID
    GroupTaskCount int                     // 组内任务数
    Args           []Arg                   // 任务参数
    Headers        Headers                 // 追踪头信息
    Priority       uint8
    Immutable      bool                    // 是否不可变（影响结果传递）
    RetryCount     int                     // 重试次数
    RetryTimeout   int                     // 重试超时（秒）
    OnSuccess      []*Signature            // 成功回调
    OnError        []*Signature            // 错误回调
    ChordCallback  *Signature              // Chord 回调
    // ... SQS 特定字段
}
```

### 4.2 TaskState（任务状态）

```go
type TaskState struct {
    TaskUUID  string
    State     string  // PENDING/RECEIVED/STARTED/RETRY/SUCCESS/FAILURE
    Results   []*TaskResult
    Error     string
    CreatedAt time.Time
    // ...
}
```

### 4.3 工作流结构

```go
// Chain - 链式执行，后一个任务以前一个任务的结果为参数
type Chain struct {
    Tasks []*Signature
}

// Group - 并行执行，所有任务同时运行
type Group struct {
    GroupUUID string
    Tasks     []*Signature
}

// Chord - 带回调的并行执行，所有任务完成后执行回调
type Chord struct {
    Group    *Group
    Callback *Signature
}
```

### 4.4 任务状态机

```
PENDING → RECEIVED → STARTED → SUCCESS
                      ↓
                   RETRY → (循环直到成功或重试次数耗尽)
                      ↓
                   FAILURE
```

## 5. 配置说明

### 5.1 Config 结构

```go
type Config struct {
    Broker                  string           // Broker URL
    Lock                    string           // Lock URL
    MultipleBrokerSeparator string           // 多 broker 分隔符
    DefaultQueue            string           // 默认队列名
    ResultBackend           string           // 结果后端 URL
    ResultsExpireIn         int              // 结果过期时间（秒）
    AMQP                    *AMQPConfig
    SQS                     *SQSConfig
    Redis                   *RedisConfig
    GCPPubSub               *GCPPubSubConfig
    MongoDB                 *MongoDBConfig
    DynamoDB                *DynamoDBConfig
    TLSConfig               *tls.Config
    NoUnixSignals           bool             // 禁用信号处理
}
```

### 5.2 Broker URL 格式

| Broker | URL 格式 |
|--------|----------|
| AMQP | `amqp://[user:pass@]host[:port]` |
| Redis | `redis://[pass@]host[:port][/db]` |
| Redis Socket | `redis+socket://[pass@]/path/to/file.sock[:/db]` |
| SQS | `https://sqs.region.amazonaws.com/account_id` |
| GCP PubSub | `gcppubsub://project_id/subscription_name` |

### 5.3 Backend URL 格式

| Backend | URL 格式 |
|---------|----------|
| Redis | `redis://[pass@]host[:port][/db]` |
| Memcache | `memcache://host[:port],...` |
| MongoDB | `mongodb://[user:pass@]host[:port][/db]` |
| AMQP | `amqp://[user:pass@]host[:port]` |
| DynamoDB | 通过 AWS SDK 配置 |

## 6. 技术栈

### 6.1 Go 版本

- **最低版本**: Go 1.15
- **推荐版本**: Go 1.18+

### 6.2 主要依赖

| 依赖 | 版本 | 用途 |
|------|------|------|
| streadway/amqp | v1.0.0 | AMQP 客户端 |
| go-redis/redis | v8.6.0 | Redis 客户端 (Go Redis) |
| gomodule/redigo | v2.0.0+incompatible | Redis 客户端 (Redigo) |
| aws/aws-sdk-go | v1.37.16 | AWS SDK (SQS/DynamoDB) |
| go.mongodb.org/mongo-driver | v1.4.6 | MongoDB 客户端 |
| cloud.google.com/go/pubsub | v1.10.0 | GCP PubSub 客户端 |
| opentracing/opentracing-go | v1.2.0 | 分布式追踪 |
| robfig/cron/v3 | v3.0.1 | 定时任务调度 |
| google/uuid | v1.2.0 | UUID 生成 |
| go-redsync/redsync/v4 | v4.0.4 | 分布式锁 |
| stretchr/testify | v1.7.0 | 测试框架 |

### 6.3 目录结构详解

```
machinery/
├── v2/                              # V2 主版本（推荐）
│   ├── brokers/                     # Broker 实现
│   │   ├── iface/interfaces.go      # Broker 接口定义
│   │   ├── amqp/                    # RabbitMQ 实现
│   │   ├── redis/                   # Redis 实现
│   │   ├── sqs/                     # AWS SQS 实现
│   │   ├── gcppubsub/               # GCP PubSub 实现
│   │   ├── eager/                   # 同步测试用实现
│   │   └── errs/errors.go          # 错误定义
│   ├── backends/                    # Backend 实现
│   │   ├── iface/interfaces.go      # Backend 接口定义
│   │   ├── amqp/                    # RabbitMQ 实现
│   │   ├── redis/                   # Redis 实现
│   │   ├── memcache/                # Memcache 实现
│   │   ├── mongo/mongodb.go         # MongoDB 实现
│   │   ├── dynamodb/                # DynamoDB 实现
│   │   ├── result/async_result.go   # 异步结果
│   │   └── null/                    # 空实现
│   ├── locks/                       # Lock 实现
│   │   ├── iface/interfaces.go      # Lock 接口定义
│   │   ├── redis/                   # Redis 分布式锁
│   │   └── eager/                   # 同步测试用实现
│   ├── tasks/                       # 任务核心
│   │   ├── task.go                  # Task 定义和执行
│   │   ├── signature.go             # 任务签名
│   │   ├── state.go                 # 任务状态
│   │   ├── result.go                # 任务结果
│   │   ├── workflow.go             # Chain/Group/Chord
│   │   ├── errors.go                # 任务错误
│   │   ├── validate.go              # 任务验证
│   │   └── reflect.go               # 反射工具
│   ├── config/                      # 配置管理
│   │   ├── config.go                # 配置结构
│   │   ├── env.go                   # 环境变量配置
│   │   └── file.go                  # YAML 文件配置
│   ├── common/                      # 公共代码
│   │   ├── broker.go                # Broker 公共逻辑
│   │   ├── backend.go               # Backend 公共逻辑
│   │   ├── redis.go                 # Redis 公共逻辑
│   │   └── amqp.go                  # AMQP 公共逻辑
│   ├── retry/                       # 重试策略
│   │   ├── retry.go                 # 重试逻辑
│   │   └── fibonacci.go             # Fibonacci 重试间隔
│   ├── tracing/                     # 追踪
│   │   └── tracing.go               # OpenTracing 集成
│   ├── log/                         # 日志
│   │   └── log.go                   # 日志接口
│   ├── utils/                       # 工具函数
│   │   ├── uuid.go                  # UUID 生成
│   │   ├── deepcopy.go              # 深拷贝
│   │   └── utils.go                 # 通用工具
│   ├── server.go                    # Server 实现
│   ├── worker.go                    # Worker 实现
│   └── example/                     # V2 示例
│       ├── amqp/
│       ├── redis/
│       └── tasks/
├── v1/                              # V1 版本（已废弃）
├── example/                         # V1 示例
├── integration-tests/               # 集成测试
└── Makefile                         # 构建命令
```

## 7. API 概览

### 7.1 Server API

```go
// 创建 Server
server := machinery.NewServer(cnf, broker, backend, lock)

// 注册任务
server.RegisterTasks(map[string]interface{}{
    "add": Add,
})
server.RegisterTask("multiply", Multiply)

// 发送任务
asyncResult, err := server.SendTask(signature)

// 发送工作流
server.SendChain(chain)
server.SendGroup(group, concurrency)
server.SendChord(chord, concurrency)

// 定时任务
server.RegisterPeriodicTask("0 6 * * ?", "periodic-task", signature)
server.RegisterPeriodicChain(spec, name, signatures...)
server.RegisterPeriodicGroup(spec, name, concurrency, signatures...)
server.RegisterPeriodicChord(spec, name, concurrency, callback, signatures...)

// 创建 Worker
worker := server.NewWorker("worker_name", 10)
worker.Launch()
```

### 7.2 Worker API

```go
// 设置错误处理器
worker.SetErrorHandler(func(err error) {
    // 自定义错误处理
})

// 设置任务前后处理器
worker.SetPreTaskHandler(func(signature *tasks.Signature) {
    // 任务执行前
})
worker.SetPostTaskHandler(func(signature *tasks.Signature) {
    // 任务执行后
})

// 优雅退出
worker.Quit()
```

### 7.3 AsyncResult API

```go
// 获取任务状态
state := asyncResult.GetState()
state.IsCompleted()
state.IsSuccess()
state.IsFailure()

// 阻塞获取结果
results, err := asyncResult.Get(time.Second * 5)
```

## 8. 错误处理

### 8.1 任务级错误

```go
// 任务返回标准错误
func MyTask(args ...int64) (int64, error) {
    if invalid {
        return 0, errors.New("invalid arguments")
    }
    return result, nil
}

// 任务返回延迟重试错误
func MyTask(args ...int64) (int64, error) {
    if temporaryFailure {
        return 0, tasks.NewErrRetryTaskLater("try later", 4*time.Hour)
    }
    return result, nil
}
```

### 8.2 Worker 错误处理

```go
// 设置全局错误处理器
worker.SetErrorHandler(func(err error) {
    sentry.CaptureException(err)
    metrics.increment("task.errors")
})
```

## 9. 测试

### 9.1 单元测试

```bash
make test
```

### 9.2 集成测试

```bash
# 需要启动相应服务
export AMQP_URL=amqp://guest:guest@localhost:5672/
export REDIS_URL=localhost:6379
export MEMCACHE_URL=localhost:11211
export MONGODB_URL=localhost:27017

make ci  # Docker Compose 方式运行
```

### 9.3 测试覆盖

```bash
make test-with-coverage
```

## 10. 使用示例

### 10.1 基础任务

```go
// 定义任务
func Add(args ...int64) (int64, error) {
    var sum int64
    for _, arg := range args {
        sum += arg
    }
    return sum, nil
}

// 注册
server.RegisterTasks(map[string]interface{}{
    "add": Add,
})

// 发送
signature := &tasks.Signature{
    Name: "add",
    Args: []tasks.Arg{
        {Type: "int64", Value: 1},
        {Type: "int64", Value: 2},
    },
}
asyncResult, _ := server.SendTask(signature)
result, _ := asyncResult.Get(time.Second * 5)
```

### 10.2 Chain 工作流

```go
chain, _ := tasks.NewChain(&sig1, &sig2, &sig3)
server.SendChain(chain)
```

### 10.3 Group 工作流

```go
group, _ := tasks.NewGroup(&sig1, &sig2)
server.SendGroup(group, 0) // 0 表示无并发限制
```

### 10.4 Chord 工作流

```go
group := tasks.NewGroup(&sig1, &sig2)
chord, _ := tasks.NewChord(group, &callbackSig)
server.SendChord(chord, 0)
```

## 11. 扩展开发

### 11.1 实现自定义 Broker

1. 实现 `iface.Broker` 接口
2. 实现 `StartConsuming`、`StopConsuming`、`Publish` 等方法
3. 在 `AdjustRoutingKey` 中实现队列路由逻辑

### 11.2 实现自定义 Backend

1. 实现 `iface.Backend` 接口
2. 实现所有状态管理方法
3. 实现 `GroupCompleted`、`TriggerChord` 等组任务方法

### 11.3 实现自定义 Lock

1. 实现 `iface.Lock` 接口
2. 实现 `Lock` 和 `LockWithRetries` 方法