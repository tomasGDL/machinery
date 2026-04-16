# Machinery Redis-Only 重构计划

## 目标

1. 合并 v1/v2，保留 v2 核心代码和 v2 依赖的部分 v1 代码
2. 删除 AMQP 依赖
3. 删除 Eager 依赖
4. 删除 GCP PubSub 依赖
5. 删除 SQS 依赖
6. 仅保留 Redis 依赖

---

## 当前代码结构分析

### v2 目录结构
```
v2/
├── brokers/
│   ├── amqp/          # ❌ 删除
│   ├── eager/         # ❌ 删除
│   ├── gcppubsub/     # ❌ 删除
│   ├── sqs/           # ❌ 删除
│   ├── redis/         # ✅ 保留
│   ├── errs/          # ✅ 保留（错误定义）
│   └── iface/         # ✅ 保留（接口定义）
├── backends/
│   ├── amqp/          # ❌ 删除
│   ├── dynamodb/      # ❌ 删除
│   ├── eager/         # ❌ 删除
│   ├── memcache/      # ❌ 删除
│   ├── mongo/         # ❌ 删除
│   ├── redis/         # ✅ 保留
│   ├── result/        # ✅ 保留（异步结果）
│   ├── null/          # ❌ 删除
│   └── iface/         # ✅ 保留（接口定义）
├── locks/
│   ├── eager/         # ❌ 删除
│   ├── redis/         # ✅ 保留
│   └── iface/         # ✅ 保留（接口定义）
├── tasks/             # ✅ 保留
├── config/            # ✅ 保留（需清理非 Redis 配置）
├── common/            # ✅ 保留
├── retry/             # ✅ 保留
├── tracing/           # ✅ 保留
├── log/               # ✅ 保留
├── utils/             # ✅ 保留
├── server.go          # ✅ 保留
├── worker.go          # ✅ 保留
└── go.mod             # ✅ 保留（需清理依赖）
```

### 需要删除的依赖包

| 依赖 | 用途 | 操作 |
|------|------|------|
| `github.com/streadway/amqp` | AMQP broker/backend | 删除 |
| `cloud.google.com/go/pubsub` | GCP PubSub broker | 删除 |
| `github.com/aws/aws-sdk-go` | SQS broker / DynamoDB backend | 删除 |
| `go.mongodb.org/mongo-driver` | MongoDB backend | 删除 |
| `github.com/bradfitz/gomemcache` | Memcache backend | 删除 |

### 需要保留的依赖包

| 依赖 | 用途 |
|------|------|
| `github.com/go-redis/redis/v8` | Redis broker/backend (go-redis) |
| `github.com/gomodule/redigo` | Redis broker/backend (redigo) |
| `github.com/go-redsync/redsync/v4` | Redis 分布式锁 |
| `github.com/google/uuid` | UUID 生成 |
| `github.com/robfig/cron/v3` | 定时任务调度 |
| `github.com/opentracing/opentracing-go` | 分布式追踪 |
| `github.com/stretchr/testify` | 测试框架 |
| `gopkg.in/yaml.v2` | YAML 配置解析 |
| `github.com/kelseyhightower/envconfig` | 环境变量配置 |

---

## 重构步骤

### 阶段 1: 删除非 Redis Broker 实现

1. **删除目录**
   - `v2/brokers/amqp/`
   - `v2/brokers/eager/`
   - `v2/brokers/gcppubsub/`
   - `v2/brokers/sqs/`

2. **更新 go.mod**
   - 移除 `github.com/streadway/amqp`
   - 移除 `cloud.google.com/go/pubsub`
   - 移除 `github.com/aws/aws-sdk-go`

### 阶段 2: 删除非 Redis Backend 实现

1. **删除目录**
   - `v2/backends/amqp/`
   - `v2/backends/dynamodb/`
   - `v2/backends/eager/`
   - `v2/backends/memcache/`
   - `v2/backends/mongo/`
   - `v2/backends/null/`

2. **更新 go.mod**
   - 移除 `go.mongodb.org/mongo-driver`
   - 移除 `github.com/bradfitz/gomemcache`

### 阶段 3: 删除非 Redis Lock 实现

1. **删除目录**
   - `v2/locks/eager/`

### 阶段 4: 清理 Config

1. **更新 `v2/config/config.go`**
   - 删除 `AMQPConfig` 结构体
   - 删除 `SQSConfig` 结构体
   - 删除 `DynamoDBConfig` 结构体
   - 删除 `GCPPubSubConfig` 结构体
   - 删除 `MongoDBConfig` 结构体
   - 更新 `Config` 结构体，移除相关字段
   - 更新 `defaultCnf`，移除 AMQP 相关默认值

### 阶段 5: 创建 Factories（从 v1 迁移）

创建 `v2/factories.go`，仅支持 Redis：

```go
// BrokerFactory - 仅支持 Redis
func BrokerFactory(cnf *config.Config) (brokeriface.Broker, error) {
    // 仅支持 redis:// 和 redis+socket://
}

// BackendFactory - 仅支持 Redis
func BackendFactory(cnf *config.Config) (backendiface.Backend, error) {
    // 仅支持 redis:// 和 redis+socket://
}

// LockFactory - 仅支持 Redis
func LockFactory(cnf *config.Config) (lockiface.Lock, error) {
    // 仅支持 redis://
}
```

从 v1/factories.go 复制以下辅助函数：
- `ParseRedisURL`
- `ParseRedisSocketURL`

### 阶段 6: 合并 v1/v2

1. **根目录 go.mod 更新**
   - 保留 v2 作为核心代码
   - 根模块直接引用 v2 包

2. **删除 v1 目录**（可选，根据需求）
   - 或保留 v1 作为兼容层

### 阶段 7: 更新示例代码

1. **更新 `v2/example/`**
   - 删除 AMQP 示例
   - 保留 Redis 示例

2. **更新根目录 `example/`**
   - 同步更新或删除

### 阶段 8: 更新测试

1. **清理集成测试**
   - 删除 AMQP 相关集成测试
   - 删除 SQS 相关集成测试
   - 保留 Redis 相关集成测试

2. **运行测试验证**
   - `go test ./...`

---

## 文件变更清单

### 删除的文件/目录

```
v2/brokers/amqp/
v2/brokers/eager/
v2/brokers/gcppubsub/
v2/brokers/sqs/
v2/backends/amqp/
v2/backends/dynamodb/
v2/backends/eager/
v2/backends/memcache/
v2/backends/mongo/
v2/backends/null/
v2/locks/eager/
```

### 修改的文件

```
v2/go.mod                    # 清理依赖
v2/config/config.go          # 清理非 Redis 配置
v2/factories.go              # 新建，从 v1 迁移并简化
```

### 保留的核心文件

```
v2/brokers/redis/
v2/brokers/iface/
v2/brokers/errs/
v2/backends/redis/
v2/backends/iface/
v2/backends/result/
v2/locks/redis/
v2/locks/iface/
v2/tasks/
v2/config/
v2/common/
v2/retry/
v2/tracing/
v2/log/
v2/utils/
v2/server.go
v2/worker.go
```

---

## 依赖清理后的 go.mod

```go
module github.com/RichardKnop/machinery/v2

go 1.15

require (
	github.com/RichardKnop/logging v0.0.0-20190827224416-1a693bdd4fae
	github.com/go-redis/redis/v8 v8.6.0
	github.com/go-redsync/redsync/v4 v4.0.4
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/uuid v1.2.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli v1.22.5
	gopkg.in/yaml.v2 v2.4.0
)
```

---

## 风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 删除过多文件 | 编译失败 | 分阶段删除，每次验证编译 |
| 依赖冲突 | 运行时错误 | 运行完整测试套件 |
| 配置不兼容 | 现有用户代码失效 | 明确文档说明，提供迁移指南 |

---

## 验证清单

- [ ] `go build ./...` 成功
- [ ] `go test ./...` 通过
- [ ] 无未使用的导入
- [ ] 示例代码可运行
- [ ] 文档已更新
