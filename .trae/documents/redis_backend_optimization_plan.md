# Redis Backend 代码优化计划

## 目标
优化 `v2/backends/redis/redis.go` 文件，去除无用代码，提升代码质量。

## 分析结果

### 1. 无用字段

| 字段 | 位置 | 问题描述 |
|------|------|----------|
| `host` | 第27行 | 定义了但未在任何地方使用 |
| `db` | 第28行 | 定义了但未在任何地方使用（`db` 参数被传递给 `redis.UniversalOptions`，但结构体字段未被使用）|
| `socketPath` | 第30行 | 定义了但未在任何地方使用 |
| `redisOnce` | 第32行 | 定义了但未在任何地方使用 |

### 2. 代码简化机会

#### 2.1 `InitGroup` 方法 (第62-81行)
当前代码：
```go
err = b.rclient.Set(context.Background(), groupUUID, encoded, expiration).Err()
if err != nil {
    return err
}
return nil
```
可以简化为直接返回错误。

#### 2.2 `PurgeState` 方法 (第221-228行)
当前代码：
```go
err := b.rclient.Del(context.Background(), taskUUID).Err()
if err != nil {
    return err
}
return nil
```
可以简化为直接返回错误。

#### 2.3 `PurgeGroupMeta` 方法 (第231-238行)
当前代码与 `PurgeState` 类似，可以简化。

#### 2.4 `updateState` 方法 (第289-302行)
当前代码：
```go
_, err = b.rclient.Set(context.Background(), taskState.TaskUUID, encoded, expiration).Result()
if err != nil {
    return err
}
return nil
```
可以简化为直接返回错误。

### 3. 测试文件问题

`redis_test.go` 第73行使用 `return` 跳过测试，应改为 `t.Skip()` 以符合 Go 测试规范。

## 优化步骤

1. **删除无用字段**：从 `Backend` 结构体中删除 `host`、`db`、`socketPath`、`redisOnce`
2. **简化错误返回**：将多处 `if err != nil { return err }; return nil` 简化为 `return err`
3. **修复测试文件**：将 `return` 改为 `t.Skip()`

## 预期结果

- 代码行数减少
- 代码可读性提升
- 去除未使用的字段，减少内存占用
- 测试文件符合 Go 规范
