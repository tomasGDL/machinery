# v2 代码审查计划

## 审查范围

* `v2/backends/redis/goredis.go`

* `v2/brokers/redis/goredis.go`

* `v2/locks/redis/redis.go`

***

## 1. backends/redis/goredis.go

### 1.1 无用字段（建议删除）

| 字段           | 行号 | 问题描述   |
| ------------ | -- | ------ |
| `host`       | 25 | 定义但未使用 |
| `password`   | 26 | 定义但未使用 |
| `db`         | 27 | 定义但未使用 |
| `socketPath` | 29 | 定义但未使用 |
| `redisOnce`  | 31 | 定义但未使用 |

### 1.2 未使用的 import

* `sync` 包（因为 `redisOnce` 未使用）

### 1.3 代码简化机会

#### InitGroup 方法 (第45-64行)

```go
// 当前代码
err = b.rclient.Set(context.Background(), groupUUID, encoded, expiration).Err()
if err != nil {
    return err
}
return nil

// 建议简化为
return b.rclient.Set(context.Background(), groupUUID, encoded, expiration).Err()
```

#### PurgeState 方法 (第204-211行)

```go
// 当前代码
err := b.rclient.Del(context.Background(), taskUUID).Err()
if err != nil {
    return err
}
return nil

// 建议简化为
return b.rclient.Del(context.Background(), taskUUID).Err()
```

#### PurgeGroupMeta 方法 (第214-221行)

与 PurgeState 类似，可以简化。

#### updateState 方法 (第272-285行)

```go
// 当前代码
_, err = b.rclient.Set(context.Background(), taskState.TaskUUID, encoded, expiration).Result()
if err != nil {
    return err
}
return nil

// 建议简化为
_, err = b.rclient.Set(context.Background(), taskState.TaskUUID, encoded, expiration).Result()
return err
```

***

## 2. brokers/redis/goredis.go

### 2.1 无用字段（建议删除）

| 字段           | 行号 | 问题描述   |
| ------------ | -- | ------ |
| `socketPath` | 35 | 定义但未使用 |
| `redsync`    | 36 | 定义但未使用 |
| `redisOnce`  | 37 | 定义但未使用 |

### 2.2 未使用的 import

* `github.com/go-redsync/redsync/v4`（因为 `redsync` 字段未使用）

### 2.3 代码问题

#### StopConsuming 方法 (第157-165行)

```go
func (b *BrokerGR) StopConsuming() {
    b.Broker.StopConsuming()
    b.delayedWG.Wait()
    b.consumingWG.Wait()
    b.rclient.Close()  // 问题：如果 client 是共享的，这里会关闭共享连接
}
```

**问题**：当多个组件共享同一个 Redis client 时，一个组件调用 `StopConsuming` 会关闭共享连接，影响其他组件。

**建议**：

* 方案1：移除 `b.rclient.Close()`，由调用方管理 client 生命周期

* 方案2：添加标志位控制是否关闭 client

***

## 3. locks/redis/redis.go

### 3.1 代码问题

#### interval 字段未设置

```go
type Lock struct {
    rclient  redis.UniversalClient
    retries  int
    interval time.Duration  // 从未设置，默认为 0
}
```

在 `LockWithRetries` 中使用了 `time.Sleep(r.interval)`，如果 interval 为 0，会导致无限重试时无间隔。

**建议**：

* 在 `New` 函数中添加 interval 参数，或设置默认值

#### 注释语言不一致

```go
// 成功拿到锁，返回  // 中文注释
```

建议统一为英文注释。

***

## 优化建议总结

### 高优先级

1. **删除无用字段**：`backends/redis/goredis.go` 中的 `host`, `password`, `db`, `socketPath`, `redisOnce`
2. **删除无用字段**：`brokers/redis/goredis.go` 中的 `socketPath`, `redsync`, `redisOnce`
3. **修复 StopConsuming**：避免关闭共享的 Redis client

### 中优先级

1. **简化错误返回**：多处 `if err != nil { return err }; return nil` 可简化
2. **删除未使用的 import**：`sync`, `redsync` 等
3. **修复 locks 的 interval**：添加默认值或参数

### 低优先级

1. **统一注释语言**：将中文注释改为英文

