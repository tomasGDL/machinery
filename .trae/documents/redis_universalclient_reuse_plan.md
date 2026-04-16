# Redis UniversalClient 复用优化计划

## 目标
修改 `backends/redis`、`brokers/redis`、`locks/redis` 三个包，使其接受外部传入的 `redis.UniversalClient` 参数，而不是在内部创建，从而实现 Redis 连接的复用。

## 当前问题分析

### 1. backends/redis/redis.go (第36-59行)
```go
func New(cnf *config.Config, addrs []string, db int) iface.Backend {
    // ... 密码解析逻辑 ...
    ropt := &redis.UniversalOptions{...}
    b.rclient = redis.NewUniversalClient(ropt)  // 内部创建 client
    b.redsync = redsync.New(redsyncgoredis.NewPool(b.rclient))
    return b
}
```

### 2. brokers/redis/redis.go (第40-67行)
```go
func New(cnf *config.Config, addrs []string, db int) iface.Broker {
    // ... 密码解析逻辑 ...
    ropt := &redis.UniversalOptions{...}
    b.rclient = redis.NewUniversalClient(ropt)  // 内部创建 client
    // ...
    return b
}
```

### 3. locks/redis/redis.go (第24-50行)
```go
func New(cnf *config.Config, addrs []string, db, retries int) Lock {
    // ... 密码解析逻辑 ...
    ropt := &redis.UniversalOptions{...}
    lock.rclient = redis.NewUniversalClient(ropt)  // 内部创建 client
    return lock
}
```

**问题**：三个组件各自创建独立的 Redis 连接，无法复用连接池。

## 优化方案

### 方案：新增带 client 参数的构造函数

保留原有 `New` 函数（保持向后兼容），新增一个接受 `redis.UniversalClient` 的构造函数。

### 1. backends/redis/redis.go 修改

新增函数 `NewWithClient`：
```go
// NewWithClient creates Backend instance with an existing redis client
func NewWithClient(cnf *config.Config, client redis.UniversalClient) iface.Backend {
    b := &Backend{
        Backend: common.NewBackend(cnf),
        rclient: client,
    }
    b.redsync = redsync.New(redsyncgoredis.NewPool(b.rclient))
    return b
}
```

同时删除无用字段（根据之前的分析）：
- `host`、`db`、`socketPath`、`redisOnce`

### 2. brokers/redis/redis.go 修改

新增函数 `NewWithClient`：
```go
// NewWithClient creates new Broker instance with an existing redis client
func NewWithClient(cnf *config.Config, client redis.UniversalClient) iface.Broker {
    b := &Broker{Broker: common.NewBroker(cnf)}
    b.rclient = client
    if cnf.Redis.DelayedTasksKey != "" {
        b.redisDelayedTasksKey = cnf.Redis.DelayedTasksKey
    } else {
        b.redisDelayedTasksKey = defaultRedisDelayedTasksKey
    }
    return b
}
```

检查并删除无用字段：
- `socketPath`、`redisOnce` 目前未被使用

### 3. locks/redis/redis.go 修改

新增函数 `NewWithClient`：
```go
// NewWithClient creates Lock instance with an existing redis client
func NewWithClient(cnf *config.Config, client redis.UniversalClient, retries int) Lock {
    if retries <= 0 {
        return Lock{}
    }
    return Lock{
        rclient: client,
        retries: retries,
    }
}
```

注意：lock 包中的 `interval` 字段需要处理，原代码中未设置，需要检查是否需要添加设置逻辑。

## 实施步骤

### Step 1: 修改 backends/redis/redis.go
1. 删除 `Backend` 结构体中的无用字段：`host`、`db`、`socketPath`、`redisOnce`
2. 新增 `NewWithClient` 函数
3. 更新 `New` 函数，使其调用 `NewWithClient`（可选，保持代码整洁）

### Step 2: 修改 brokers/redis/redis.go
1. 检查并删除无用字段：`socketPath`、`redisOnce`
2. 新增 `NewWithClient` 函数

### Step 3: 修改 locks/redis/redis.go
1. 新增 `NewWithClient` 函数
2. 检查 `interval` 字段的使用情况

### Step 4: 更新测试文件
1. 确保测试文件可以正常工作
2. 如有必要，添加新的测试用例测试 `NewWithClient` 函数

## 向后兼容性

- 保留原有的 `New` 函数不变，确保现有代码可以继续使用
- 新增的 `NewWithClient` 函数提供复用 client 的能力
- 所有修改都是新增功能，不会破坏现有 API

## 预期收益

1. **连接复用**：多个组件可以共享同一个 Redis 连接池
2. **资源节省**：减少 Redis 连接数
3. **配置集中**：Redis 连接配置可以在一处统一管理
4. **测试友好**：便于在测试中使用 mock 的 redis client
