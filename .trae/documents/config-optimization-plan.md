# Machinery 配置优化计划

## 目标
简化 Machinery 配置系统，使其更加易用，使用 redis.UniversalOptions 内嵌替换原有 Redis 配置，并提供简洁的默认配置。

## 当前问题
1. 配置可以从环境变量、YAML 文件加载，过于复杂
2. Redis 配置结构冗余，与 go-redis 的 UniversalOptions 重复
3. 代码中存在硬编码值（如轮询周期、任务前缀等）
4. Broker、Lock、ResultBackend、TLSConfig 等字段在有了 UniversalOptions 后变得冗余

## 优化方案

### 1. 简化配置结构 (config/config.go)

**变更内容：**
- 移除 `envconfig` 和 `yaml` 标签
- 使用 `redis.UniversalOptions` 内嵌替换 `RedisConfig`
- **删除冗余字段**：Broker、Lock、ResultBackend、TLSConfig（这些都可以从 UniversalOptions 推导）
- 添加可配置项替代硬编码值
- 提供 `DefaultConfig()` 函数返回默认配置

**新的 Config 结构：**
```go
type Config struct {
    // 队列配置
    DefaultQueue  string        // 默认队列名称，默认 "machinery_tasks"

    // 任务配置
    ResultsExpireIn        int           // 结果过期时间（秒），默认 3600
    TaskPrefix             string        // 任务ID前缀，默认 "task_"
    DefaultMaxRetry        int           // 默认最大重试次数

    // Worker 配置
    NoUnixSignals          bool          // 是否禁用 Unix 信号处理

    // 轮询配置
    NormalTasksPollPeriod  time.Duration // 普通任务轮询周期，默认 1s
    DelayedTasksPollPeriod time.Duration // 延迟任务轮询周期，默认 500ms
    DelayedTasksKey        string        // 延迟任务存储键，默认 "delayed_tasks"

    // Redis 配置 - 内嵌 UniversalOptions
    redis.UniversalOptions
}
```

**DefaultConfig 函数：**
```go
func DefaultConfig() *Config {
    return &Config{
        DefaultQueue:           "machinery_tasks",
        ResultsExpireIn:        3600,
        TaskPrefix:             "task_",
        DefaultMaxRetry:        3,
        NoUnixSignals:          false,
        NormalTasksPollPeriod:  1 * time.Second,
        DelayedTasksPollPeriod: 500 * time.Millisecond,
        DelayedTasksKey:        "delayed_tasks",
        UniversalOptions: redis.UniversalOptions{
            Addrs: []string{"localhost:6379"},
            DB:    0,
        },
    }
}
```

**使用方式：**
```go
// 使用默认配置
cnf := config.DefaultConfig()

// 自定义配置（基于默认配置修改）
cnf := config.DefaultConfig()
cnf.DefaultQueue = "my_queue"
cnf.Addrs = []string{"redis-cluster:6379"}
cnf.ClusterMode = true
```

### 2. 删除环境变量和文件配置加载

**删除文件：**
- `config/env.go` - 环境变量加载
- `config/env_test.go` - 环境变量测试
- `config/file.go` - YAML 文件加载
- `config/file_test.go` - 文件测试
- `config/test.env` - 测试环境变量文件
- `config/testconfig.yml` - 测试 YAML 文件

### 3. 更新 Broker、Backend、Lock 创建方式

由于 Broker、Lock、ResultBackend 字段已被删除，需要通过 UniversalOptions 来创建 Redis 客户端。

**文件：** `brokers/redis/goredis.go`
- 修改 `New` 函数签名，接收 `redis.UniversalClient` 或从 Config 创建
- 使用 `config.NormalTasksPollPeriod` 替代硬编码 1000ms
- 使用 `config.DelayedTasksPollPeriod` 替代硬编码 500ms
- 使用 `config.DelayedTasksKey` 替代硬编码 "delayed_tasks"

### 4. 更新 Server

**文件：** `server.go`
- 使用 `config.TaskPrefix` 替代硬编码 "task_"
- 移除对 Broker、ResultBackend 字符串配置的依赖

### 5. 更新 Worker

**文件：** `worker.go`
- 移除硬编码的 "localhost:6379" 日志输出

### 6. 更新测试

**文件：** `server_test.go`
- 更新测试用例使用新的配置结构
- 使用 `DefaultConfig()` 作为基础

### 7. 更新 go.mod

- 移除 `github.com/kelseyhightower/envconfig` 依赖
- 移除 `gopkg.in/yaml.v2` 依赖
- 运行 `go mod tidy`

## 实施步骤

1. **修改 config/config.go**
   - 重写 Config 结构体，内嵌 redis.UniversalOptions
   - 删除 Broker、Lock、ResultBackend、TLSConfig 字段
   - 添加 DefaultConfig() 函数
   - 更新默认值

2. **删除不必要的文件**
   - 删除 env.go, env_test.go
   - 删除 file.go, file_test.go
   - 删除 test.env, testconfig.yml

3. **更新 brokers/redis/goredis.go**
   - 使用新的配置字段
   - 适配 UniversalOptions

4. **更新 backends/redis/goredis.go**
   - 适配新的配置结构

5. **更新 locks/redis/redis.go**
   - 适配新的配置结构

6. **更新 server.go**
   - 使用 TaskPrefix 配置

7. **更新 worker.go**
   - 修复硬编码日志

8. **更新 server_test.go**
   - 适配新的配置结构

9. **更新 go.mod**
   - 运行 `go mod tidy` 清理依赖

## 预期结果

1. 配置更加简单直观，直接通过代码创建 Config 对象或使用 DefaultConfig()
2. Redis 配置与 go-redis 库保持一致，支持单机、Sentinel、Cluster 模式
3. 所有硬编码值变为可配置
4. 删除冗余的 Broker、Lock、ResultBackend 字符串配置字段
5. 代码库减少约 300 行代码
6. 移除两个外部依赖

## 向后兼容性

这是一个破坏性变更，使用者需要：
1. 不再使用 `config.NewFromEnvironment()` 和 `config.NewFromYaml()`
2. 使用 `config.DefaultConfig()` 或手动创建 `config.Config` 结构体
3. Redis 配置直接使用 `config.UniversalOptions` 字段
4. 不再需要设置 Broker、Lock、ResultBackend 字符串字段

## 使用示例

**优化前：**
```go
cnf, _ := config.NewFromEnvironment()
broker := redisbroker.New(cnf, client)
backend := redisbackend.New(cnf, client)
lock := redislock.New(client, 3, 100*time.Millisecond)
server := machinery.NewServer(cnf, broker, backend, lock)
```

**优化后：**
```go
// 使用默认配置
cnf := config.DefaultConfig()

// 创建 Redis 客户端
client := redis.NewUniversalClient(&cnf.UniversalOptions)

// 创建组件
broker := redisbroker.New(cnf, client)
backend := redisbackend.New(cnf, client)
lock := redislock.New(client, 3, 100*time.Millisecond)

// 创建 Server
server := machinery.NewServer(cnf, broker, backend, lock)
```

**自定义配置：**
```go
cnf := config.DefaultConfig()
cnf.DefaultQueue = "my_queue"
cnf.ResultsExpireIn = 7200
cnf.UniversalOptions.Addrs = []string{"redis.example.com:6379"}
cnf.UniversalOptions.Password = "secret"

client := redis.NewUniversalClient(&cnf.UniversalOptions)
broker := redisbroker.New(cnf, client)
backend := redisbackend.New(cnf, client)
lock := redislock.New(client, 3, 100*time.Millisecond)
server := machinery.NewServer(cnf, broker, backend, lock)
```
