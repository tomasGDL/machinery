# 删除 Redigo 实现计划

## 目标

删除所有 redigo 相关的实现，统一使用 go-redis 版本。

## 需要删除的文件

### 1. Broker 层
- ✅ `v2/brokers/redis/redis.go` - redigo broker 实现

### 2. Backend 层
- ✅ `v2/backends/redis/redis.go` - redigo backend 实现
- ✅ `v2/backends/redis/redis_test.go` - redigo backend 测试

## 需要修改的文件

### 1. Broker 层
- ✅ `v2/brokers/redis/goredis.go` - 重命名为 `redis.go`

### 2. Backend 层
- ✅ `v2/backends/redis/goredis.go` - 重命名为 `redis.go`
- ✅ `v2/backends/redis/goredis_test.go` - 重命名为 `redis_test.go`

### 3. 示例代码
- ✅ `v2/example/redigo/main.go` - 删除或修改为使用 go-redis
- ✅ `v2/example/go-redis/main.go` - 可能不需要，合并到 redigo 目录

### 4. 其他引用
- ✅ `v2/server_test.go` - 检查是否使用 redigo
- ✅ `v2/go.mod` - 清理 redigo 依赖

## 实施步骤

### 阶段 1: 重命名 go-redis 文件
1. 将 `brokers/redis/goredis.go` 重命名为 `brokers/redis/redis.go`
2. 将 `backends/redis/goredis.go` 重命名为 `backends/redis/redis.go`
3. 将 `backends/redis/goredis_test.go` 重命名为 `backends/redis/redis_test.go`

### 阶段 2: 删除 redigo 文件
1. 删除 `brokers/redis/redis.go` (原 redigo 版本)
2. 删除 `backends/redis/redis.go` (原 redigo 版本)
3. 删除 `backends/redis/redis_test.go` (原 redigo 版本)

### 阶段 3: 更新示例代码
1. 更新 `example/redigo/main.go` 使用 go-redis
2. 删除 `example/go-redis/` 目录（合并到 redigo）

### 阶段 4: 清理依赖
1. 从 `go.mod` 中移除 `github.com/gomodule/redigo`
2. 移除 `common/redis.go` (如果不再需要)
3. 运行 `go mod tidy`

### 阶段 5: 验证
1. 运行 `go build ./...`
2. 运行 `go test ./...`

## 注意事项

1. **构造函数变化**:
   - redigo: `New(cnf, host, password, socketPath string, db int)`
   - go-redis: `NewGR(cnf, addrs []string, db int)`
   - 需要统一为 go-redis 风格

2. **Redis URL 解析**:
   - redigo 版本支持 socket 路径
   - go-redis 版本使用地址列表
   - 需要确保兼容性

3. **测试环境变量**:
   - redigo 测试使用 `REDIS_URL`
   - go-redis 测试使用 `REDIS_URL_GR`
   - 需要统一

## 预期结果

- 代码库只保留 go-redis 实现
- 依赖减少（移除 redigo）
- 代码更简洁，维护更容易
