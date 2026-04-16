# 基于 Machinery 改造成纯 Redis 异步任务框架计划

## 1. 项目背景与目标

### 1.1 当前状态

Machinery 是一个支持多种消息代理（AMQP、Redis、SQS、GCP PubSub）和结果后端（Redis、Memcache、MongoDB、DynamoDB、AMQP）的异步任务队列库。项目已有完整的 Redis 实现，包括：

* **Redis Broker**: 支持 redigo 和 go-redis 两种客户端

* **Redis Backend**: 支持 redigo 和 go-redis 两种客户端

* **Redis Lock**: 分布式锁实现

### 1.2 改造目标

将 Machinery 精简为一个**纯 Redis 的异步任务框架**，具有以下特点：

1. **单一依赖**: 只依赖 Redis，移除所有其他 broker/backend 依赖
2. **简化配置**: 配置项大幅减少，开箱即用
3. **性能优化**: 针对 Redis 场景优化实现
4. **功能增强**: 添加 Redis 特有的高级功能
5. **代码精简**: 移除不必要的抽象层

### 1.3 预期收益

| 方面    | 改造前 | 改造后 |
| ----- | --- | --- |
| 依赖数量  | 10+ | 3-4 |
| 配置复杂度 | 高   | 低   |
| 包体积   | 大   | 小   |
| 学习曲线  | 陡峭  | 平缓  |
| 维护成本  | 高   | 低   |

***

## 2. 架构改造方案

### 2.1 目录结构改造

**改造前：**

```
v2/
├── brokers/
│   ├── amqp/
│   ├── redis/
│   ├── sqs/
│   ├── gcppubsub/
│   ├── eager/
│   └── iface/
├── backends/
│   ├── amqp/
│   ├── redis/
│   ├── memcache/
│   ├── mongo/
│   ├── dynamodb/
│   ├── null/
│   └── iface/
├── locks/
│   ├── redis/
│   ├── eager/
│   └── iface/
└── ...
```

**改造后：**

```
v2/
├── broker/              # Redis Broker（重命名，移除 iface）
│   ├── broker.go        # Broker 实现
│   ├── options.go       # 配置选项
│   └── broker_test.go
├── backend/             # Redis Backend（重命名，移除 iface）
│   ├── backend.go       # Backend 实现
│   ├── options.go       # 配置选项
│   └── backend_test.go
├── lock/                # Redis Lock（重命名，移除 iface）
│   ├── lock.go          # Lock 实现
│   └── lock_test.go
├── task/                # 任务核心（重命名 tasks）
│   ├── signature.go
│   ├── state.go
│   ├── result.go
│   ├── workflow.go
│   └── task.go
├── config/              # 简化配置
│   └── config.go
├── server.go
├── worker.go
└── example/
```

### 2.2 接口简化

**改造前（多接口）：**

```go
type Broker interface { ... }    // 9 个方法
type Backend interface { ... }   // 14 个方法
type Lock interface { ... }      // 2 个方法
```

**改造后（具体类型）：**

```go
type Broker struct { ... }       // 直接使用具体类型
type Backend struct { ... }      // 直接使用具体类型
type Lock
```

**改造后（直接使用结构体）：**

```go
type Broker struct { ... }       // 直接暴露结构体
type Backend struct { ... }      // 直接暴露结构体
type Lock struct { ... }         // 直接暴露结构体
```

### 2.3 配置简化

**改造前：**

```go
type Config struct {
    Broker                  string
    Lock                    string
    MultipleBrokerSeparator string
    DefaultQueue            string
    ResultBackend           string
    ResultsExpireIn         int
    AMQP                    *AMQPConfig
    SQS                     *SQSConfig
    Redis                   *RedisConfig
    GCPPubSub               *GCPPubSubConfig
    MongoDB                 *MongoDBConfig
    DynamoDB                *DynamoDBConfig
    TLSConfig               *tls.Config
    NoUnixSignals           bool
}
```

**改造后：**

```go
type Config struct {
    // Redis 连接
    Addrs        []string      // Redis 地址（支持集群）
    Password     string        // 密码
    DB           int           // 数据库
    MasterName   string        // Sentinel 主节点名

    // 队列配置
    DefaultQueue    string     // 默认队列名
    ResultsExpireIn int        // 结果过期时间（秒）

    // 性能配置
    PoolSize       int        // 连接池大小
    MinIdleConns   int        // 最小空闲连接
    MaxRetries    int        // 最大重试次数
    PollPeriod      int        // 轮询周期（毫秒）

    // 功能开关
    NoUnixSignals   bool       // 禁用信号处理
}
```

***

## 3. 具体实施步骤

### 阶段一：代码精简（预计 2-3 小时）

#### 步骤 1.1：移除非 Redis broker

**删除目录：**

* `v2/brokers/amqp/`

* `v2/brokers/sqs/`

* `v2/brokers/gcppubsub/`

* `v2/brokers/eager/`

* `v2/brokers/errs/`

* `v2/brokers/iface/`

**保留并重构：**

* `v2/brokers/redis/` → `v2/broker/`

#### 步骤 1.2：移除非 Redis backend

**删除目录：**

* `v2/backends/amqp/`

* `v2/backends/memcache/`

* `v2/backends/mongo/`

* `v2/backends/dynamodb/`

* `v2/backends/null/`

* `v2/backends/result/`（合并到 backend）

* `v2/backends/iface/`

**保留并重构：**

* `v2/backends/redis/` → `v2/backend/`

#### 步骤 1.3：移除非 Redis lock

**删除目录：**

* `v2/locks/eager/`

* `v2/locks/iface/`

**保留并重构：**

* `v2/locks/redis/` → `v2/lock/`

#### 步骤 1.4：移除 common 包中的非 Redis 代码

**删除文件：**

* `v2/common/amqp.go`

**保留并重构：**

* `v2/common/redis.go` → 合并到 broker/backend

* `v2/common/broker.go` → 合并到 broker

* `v2/common/backend.go` → 合并到 backend

### 阶段二：接口重构（预计 2-3 小时）

#### 步骤 2.1：重构 Broker

**文件：** `v2/broker/broker.go`

```go
package broker

import (
    "context"
    "github.com/go-redis/redis/v8"
    // ...
)

type Broker struct {
    client         redis.UniversalClient
    config         *Config
    // ...
}

func New(config *Config) *Broker {
    // ...
}

func (b *Broker) Publish(ctx context.Context, task *tasks.Signature) error {
    // ...
}

func (b *Broker) StartConsuming(concurrency int, processor TaskProcessor) error {
    // ...
}

func (b *Broker) StopConsuming() {
    // ...
}

// 其他方法...
```

#### 步骤 2.2：重构 Backend

**文件：** `v2/backend/backend.go`

```go
package backend

import (
    "context"
    "github.com/go-redis/redis/v8"
    // ...
)

type Backend struct {
    client redis.UniversalClient
    config *Config
    // ...
}

func New(config *Config) *Backend {
    // ...
}

func (b *Backend) SetStatePending(signature *tasks.Signature) error {
    // ...
}

// 其他状态方法...

func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
    // ...
}
```

#### 步骤 2.3：重构 Lock

**文件：** `v2/lock/lock.go`

```go
package lock

import (
    "github.com/go-redis/redis/v8"
    // ...
)

type Lock struct {
    client redis.UniversalClient
    // ...
}

func New(client redis.UniversalClient) *Lock {
    // ...
}

func (l *Lock) Acquire(key string, ttl time.Duration) error {
    // ...
}

func (l *Lock) Release(key string) error {
    // ...
}
```

### 阶段三：配置简化（预计 1-2 小时）

#### 步骤 3.1：简化 Config 结构

**文件：** `v2/config/config.go`

```go
package config

import "time"

type Config struct {
    // Redis 配置
    Redis RedisConfig

    // 队列配置
    Queue QueueConfig

    // 功能配置
    Features FeaturesConfig
}

type RedisConfig struct {
    Addrs      []string
    Password   string
    DB         int
    MasterName string // Sentinel

    PoolSize     int
    MinIdleConns int
    MaxRetries   int
    DialTimeout  time.Duration
    ReadTimeout  time.Duration
    WriteTimeout time.Duration
}

type QueueConfig struct {
    Name           string
    ResultsExpireIn int
    PollPeriod      time.Duration
}

type FeaturesConfig struct {
    EnableTracing   bool
    NoUnixSignals   bool
}
```

#### 步骤 3.2：添加便捷构造函数

```go
// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
    return &Config{
        Redis: RedisConfig{
            Addrs:        []string{"localhost:6379"},
            PoolSize:     10,
            MinIdleConns: 5,
            MaxRetries:   3,
            DialTimeout:  5 * time.Second,
            ReadTimeout:  3 * time.Second,
            WriteTimeout: 3 * time.Second,
        },
        Queue: QueueConfig{
            Name:            "machinery_tasks",
            ResultsExpireIn: 3600,
            PollPeriod:      100 * time.Millisecond,
        },
        Features: FeaturesConfig{
            EnableTracing: false,
            NoUnixSignals: false,
        },
    }
}

// WithAddrs 设置 Redis 地址
func (c *Config) WithAddrs(addrs ...string) *Config {
    c.Redis.Addrs = addrs
    return c
}

// WithPassword 设置密码
func (c *Config) WithPassword(password string) *Config {
    c.Redis.Password = password
    return c
}

// WithDB 设置数据库
func (c *Config) WithDB(db int) *Config {
    c.Redis.DB = db
    return c
}
```

### 阶段四：Server/Worker 重构（预计 2-3 小时）

#### 步骤 4.1：简化 Server 构造

**文件：** `v2/server.go`

```go
package machinery

import (
    "github.com/RichardKnop/machinery/v2/broker"
    "github.com/RichardKnop/machinery/v2/backend"
    "github.com/RichardKnop/machinery/v2/lock"
    "github.com/RichardKnop/machinery/v2/config"
)

type Server struct {
    config   *config.Config
    broker   *broker.Broker
    backend  *backend.Backend
    lock     *lock.Lock
    tasks    *sync.Map
}

// New 创建 Server（简化版）
func New(config *config.Config) *Server {
    b := broker.New(config)
    be := backend.New(config)
    l := lock.New(b.Client())

    return &Server{
        config:  config,
        broker:  b,
        backend: be,
        lock:    l,
        tasks:   new(sync.Map),
    }
}

// NewWithRedis 使用 Redis 客户端创建 Server
func NewWithRedis(client redis.UniversalClient, opts ...Option) *Server {
    // ...
}
```

#### 步骤 4.2：简化 Worker 构造

**文件：** `v2/worker.go`

```go
// 保持现有 Worker 实现，移除不必要的接口依赖
```

### 阶段五：依赖清理（预计 1 小时）

#### 步骤 5.1：更新 go.mod

**移除依赖：**

* `github.com/streadway/amqp` (AMQP)

* `github.com/aws/aws-sdk-go` (SQS/DynamoDB)

* `cloud.google.com/go/pubsub` (GCP PubSub)

* `github.com/bradfitz/gomemcache` (Memcache)

* `go.mongodb.org/mongo-driver` (MongoDB)

* `github.com/gomodule/redigo` (可选，只保留 go-redis)

**保留依赖：**

* `github.com/go-redis/redis/v8` (Redis 客户端)

* `github.com/go-redsync/redsync/v4` (分布式锁)

* `github.com/google/uuid` (UUID 生成)

* `github.com/robfig/cron/v3` (定时任务)

* `github.com/opentracing/opentracing-go` (可选，追踪)

* `github.com/stretchr/testify` (测试)

#### 步骤 5.2：更新 go.sum

```bash
go mod tidy
```

### 阶段六：功能增强（可选，预计 2-4 小时）

#### 步骤 6.1：添加 Redis Stream 支持

```go
// 使用 Redis Stream 替代 List，支持消费者组
func (b *Broker) PublishToStream(ctx context.Context, task *tasks.Signature) error {
    // XADD
}

func (b *Broker) ConsumeFromStream(group, consumer string) error {
    // XREADGROUP
}
```

#### 步骤 6.2：添加任务优先级

```go
// 使用多个队列实现优先级
type PriorityQueue struct {
    high   string
    medium string
    low    string
}
```

#### 步骤 6.3：添加任务进度追踪

```go
// 使用 HSET 存储任务进度
func (b *Backend) SetProgress(taskUUID string, progress float64) error {
    // HSET task:uuid progress 50.0
}

func (b *Backend) GetProgress(taskUUID string) (float64, error) {
    // HGET task:uuid progress
}
```

#### 步骤 6.4：添加任务取消

```go
// 使用 Redis Pub/Sub 实现任务取消
func (b *Broker) CancelTask(taskUUID string) error {
    // PUBLISH task:cancel taskUUID
}

func (b *Broker) WatchCancellation(taskUUID string) <-chan struct{} {
    // SUBSCRIBE task:cancel
}
```

### 阶段七：测试与文档（预计 2-3 小时）

#### 步骤 7.1：更新单元测试

* 更新所有 import 路径

* 添加新的测试用例

* 移除非 Redis 相关测试

#### 步骤 7.2：更新集成测试

* 只保留 Redis 相关的集成测试

* 简化测试环境配置

#### 步骤 7.3：更新文档

* 更新 README.md

* 更新示例代码

* 添加迁移指南

***

## 4. 风险与注意事项

### 4.1 破坏性变更

| 变更         | 影响          | 解决方案     |
| ---------- | ----------- | -------- |
| 移除 iface 包 | 现有代码依赖接口    | 提供迁移指南   |
| 重命名目录      | import 路径变更 | 提供迁移脚本   |
| 配置结构变更     | 现有配置不兼容     | 提供配置转换函数 |

### 4.2 兼容性考虑

* **V1 用户**: 建议继续使用原 Machinery

* **V2 用户**: 提供迁移工具和文档

* **新用户**: 直接使用新版本

### 4.3 性能考虑

* go-redis vs redigo: 建议使用 go-redis（更活跃的维护）

* 连接池配置: 提供合理的默认值

* Pipeline 批量操作: 在可能的地方使用

***

## 5. 时间估算

| 阶段                   | 预计时间   | 优先级    |
| -------------------- | ------ | ------ |
| 阶段一：代码精简             | 2-3 小时 | P0     |
| 阶段二：接口重构             | 2-3 小时 | P0     |
| 阶段三：配置简化             | 1-2 小时 | P0     |
| 阶段四：Server/Worker 重构 | 2-3 小时 | P0     |
| 阶段五：依赖清理             | 1 小时   | P0     |
| 阶段六：功能增强             | 2-4 小时 | P1（可选） |
| 阶段七：测试与文档            | 2-3 小时 | P0     |

**总计：** 10-15 小时（不含可选功能增强）

***

## 6. 交付物

1. **代码**

   * 重构后的 v2 目录

   * 更新后的 go.mod/go.sum

   * 更新后的测试代码

2. **文档**

   * README.md（更新）

   * 迁移指南（新增）

   * API 文档（更新）

3. **示例**

   * 基础使用示例

   * 工作流示例

   * 定时任务示例

