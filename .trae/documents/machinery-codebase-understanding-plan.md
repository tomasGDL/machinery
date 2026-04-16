# Machinery 仓库理解计划

## 项目概述

**Machinery** 是一个基于分布式消息传递的异步任务队列/作业队列库，使用 Go 语言开发。

### 核心功能

* 异步任务执行

* 任务重试机制（基于 Fibonacci 序列）

* 任务状态追踪

* 工作流编排（Chain、Group、Chord）

* 定时任务（基于 cron 表达式）

* 分布式锁

* 多 Broker 支持（AMQP、Redis、SQS、GCP PubSub）

* 多 Backend 支持（Redis、Memcache、MongoDB、DynamoDB、AMQP）

* 分布式追踪（OpenTracing）

***

## 技术栈

| 项目          | 版本/说明                                          |
| ----------- | ---------------------------------------------- |
| Go 版本       | 1.15+                                          |
| AMQP 客户端    | streadway/amqp v1.0.0                          |
| Redis 客户端   | go-redis/redis v8.6.0 / gomodule/redigo v2.0.0 |
| AWS SDK     | aws/aws-sdk-go v1.37.16                        |
| MongoDB 客户端 | go.mongodb.org/mongo-driver v1.4.6             |
| GCP PubSub  | cloud.google.com/go/pubsub v1.10.0             |
| 分布式锁        | go-redsync/redsync/v4 v4.0.4                   |
| 定时任务        | robfig/cron/v3 v3.0.1                          |
| 测试框架        | stretchr/testify v1.7.0                        |

***

## 目录结构

```
machinery/
├── v2/                          # V2 主版本（推荐）
│   ├── brokers/                 # Broker 实现
│   │   ├── iface/interfaces.go  # Broker 接口定义
│   │   ├── amqp/                # RabbitMQ 实现
│   │   ├── redis/               # Redis 实现
│   │   ├── sqs/                 # AWS SQS 实现
│   │   ├── gcppubsub/           # GCP PubSub 实现
│   │   └── eager/               # 同步测试用实现
│   ├── backends/                # Backend 实现
│   │   ├── iface/interfaces.go  # Backend 接口定义
│   │   ├── amqp/                # RabbitMQ 实现
│   │   ├── redis/               # Redis 实现
│   │   ├── memcache/            # Memcache 实现
│   │   ├── mongo/               # MongoDB 实现
│   │   ├── dynamodb/            # DynamoDB 实现
│   │   └── result/              # 异步结果
│   ├── locks/                   # Lock 实现
│   │   ├── iface/interfaces.go  # Lock 接口定义
│   │   ├── redis/               # Redis 分布式锁
│   │   └── eager/               # 同步测试用实现
│   ├── tasks/                   # 任务核心
│   │   ├── task.go              # Task 定义和执行
│   │   ├── signature.go         # 任务签名
│   │   ├── state.go             # 任务状态
│   │   ├── result.go            # 任务结果
│   │   ├── workflow.go          # Chain/Group/Chord
│   │   ├── errors.go            # 任务错误
│   │   ├── validate.go          # 任务验证
│   │   └── reflect.go           # 反射工具
│   ├── config/                  # 配置管理
│   ├── common/                  # 公共代码
│   ├── retry/                   # 重试策略
│   ├── tracing/                 # 分布式追踪
│   ├── log/                     # 日志
│   ├── utils/                   # 工具函数
│   ├── server.go                # Server 实现
│   ├── worker.go                # Worker 实现
│   └── example/                 # V2 示例
├── v1/                          # V1 版本（已废弃）
├── example/                     # V1 示例
├── integration-tests/           # 集成测试
└── Makefile                     # 构建命令
```

***

## 核心架构

### 1. Server

* 负责任务注册、任务发送、工作流编排

* 持有配置、broker、backend、lock 的引用

* 提供 SendTask、SendGroup、SendChain、SendChord 等 API

### 2. Worker

* 负责从 broker 消费任务并执行

* 支持并发控制、信号处理（优雅退出）

* 负责任务状态更新和回调触发

### 3. Broker（消息代理）

* 抽象不同消息队列的实现

* 负责任务发布和消费

* 支持延迟任务、任务路由

### 4. Backend（结果后端）

* 抽象不同存储的实现

* 负责存储任务状态和结果

* 支持状态查询、组任务完成检查

### 5. Lock（分布式锁）

* 确保定时任务不会重复执行

***

## 关键接口

### Broker 接口

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

### Backend 接口

```go
type Backend interface {
    InitGroup(groupUUID string, taskUUIDs []string) error
    GroupCompleted(groupUUID string, groupTaskCount int) (bool, error)
    GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error)
    TriggerChord(groupUUID string) (bool, error)
    SetStatePending(signature *tasks.Signature) error
    SetStateReceived(signature *tasks.Signature) error
    SetStateStarted(signature *tasks.Signature) error
    SetStateRetry(signature *tasks.Signature) error
    SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error
    SetStateFailure(signature *tasks.Signature, err string) error
    GetState(taskUUID string) (*tasks.TaskState, error)
    IsAMQP() bool
    PurgeState(taskUUID string) error
    PurgeGroupMeta(groupUUID string) error
}
```

### Lock 接口

```go
type Lock interface {
    LockWithRetries(key string, value int64) error
    Lock(key string, value int64) error
}
```

***

## 任务状态机

```
PENDING → RECEIVED → STARTED → SUCCESS
                      ↓
                   RETRY → (循环直到成功或重试次数耗尽)
                      ↓
                   FAILURE
```

***

## 工作流类型

1. **Chain（链式）**: 任务按顺序执行，后一个任务以前一个任务的结果为参数
2. **Group（并行）**: 所有任务同时并行执行
3. **Chord（带回调的并行）**: 并行执行一组任务，全部完成后执行回调任务

***

## 测试

* **单元测试**: `make test`

* **集成测试**: `make ci`（使用 Docker Compose）

* **测试覆盖**: `make test-with-coverage`

***

## 开发规范

根据 `.trae/rules/SKILL.md`，项目遵循以下开发规范：

1. **代码风格**: 使用 Go fmt、gometalinter、golint
2. **命名约定**: 遵循 Go 社区惯例（驼峰命名、导出/非导出规则）
3. **错误处理**: 必须返回 error，禁止忽略错误
4. **Context 传递**: 作为第一个参数
5. **接口实现**: Broker、Backend、Lock 必须实现所有方法
6. **测试要求**: 单元测试（`*_test.go`）、集成测试（`integration-tests/`）

***

## V1 vs V2

* **V1**: 使用工厂模式，会导入所有 broker 和 backend 的依赖

* **V2**: 推荐版本，通过依赖注入方式创建 Server，只导入需要的依赖

***

## 下一步建议

1. 深入阅读 `v2/` 目录下的核心代码实现
2. 查看 `example/` 目录了解使用方式
3. 阅读 `integration-tests/` 了解测试模式
4. 根据需求进行功能扩展或 Bug 修复

