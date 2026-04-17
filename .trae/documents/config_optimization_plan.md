# Config 配置优化计划

## 优化目标

简化配置模块，使其更加简单易用：
1. 不要从环境变量加载配置
2. 不要从命令行加载配置
3. 不要从配置文件加载配置
4. 优化掉不必要的配置项
5. 优化注释代码

---

## 当前配置项使用情况分析

### 实际使用的配置项（必须保留）

| 配置项 | 使用位置 | 用途 | 必要性 |
|--------|----------|------|--------|
| `DefaultQueue` | brokers/redis/goredis.go:193, common/broker.go:138, worker.go:60 | 默认队列名称，用于任务路由 | **必须保留** |
| `ResultsExpireIn` | backends/redis/goredis.go:264 | 任务结果过期时间（秒） | **必须保留** |
| `NoUnixSignals` | worker.go:90 | 禁用Unix信号处理 | **必须保留** |

### 未使用的配置项（可以删除）

| 配置项 | 状态 | 说明 |
|--------|------|------|
| `TLSConfig` | **未使用** | 只在config.go定义，代码中无实际使用 |
| `MultipleBrokerSeparator` | **未使用** | 只在config.go定义，代码中无实际使用 |

### Redis相关配置

当前 `RedisConfig` 结构体有14个字段，全部用于Redis连接配置。这些将被 `redis.UniversalOptions` 替代。

---

## 优化方案

### 1. 简化 Config 结构体

**优化后结构体：**
```go
package config

import "github.com/redis/go-redis/v9"

// Config holds all configuration for machinery
type Config struct {
    // RedisOptions - go-redis 通用配置选项
    // 包含 Addrs, DB, Password, PoolSize, MinIdleConns 等所有 Redis 连接配置
    RedisOptions redis.UniversalOptions
    
    // DefaultQueue - 默认队列名称，默认 "machinery_tasks"
    DefaultQueue string
    
    // ResultsExpireIn - 任务结果过期时间（秒），默认 3600
    ResultsExpireIn int
    
    // NoUnixSignals - 是否禁用 Unix 信号处理，默认 false
    NoUnixSignals bool
}
```

**删除内容：**
- `Broker`, `Lock`, `ResultBackend` - 统一使用 `RedisOptions.Addrs`
- `MultipleBrokerSeparator` - 未使用
- `TLSConfig` - 未使用
- `RedisConfig` 结构体 - 被 `redis.UniversalOptions` 替代
- 所有 `yaml` 和 `envconfig` 标签

### 2. 删除的文件

- `env.go` - 环境变量加载
- `file.go` - 配置文件加载
- `env_test.go` - 环境变量测试
- `file_test.go` - 文件加载测试
- `test.env`
- `testconfig.yml`

### 3. 提供构造函数

```go
// NewConfig 创建默认配置
func NewConfig() *Config {
    return &Config{
        RedisOptions: redis.UniversalOptions{
            Addrs: []string{"localhost:6379"},
        },
        DefaultQueue:    "machinery_tasks",
        ResultsExpireIn: 3600,
        NoUnixSignals:   false,
    }
}

// NewConfigWithRedis 使用指定 Redis 地址创建配置
func NewConfigWithRedis(addr string) *Config {
    return &Config{
        RedisOptions: redis.UniversalOptions{
            Addrs: []string{addr},
        },
        DefaultQueue:    "machinery_tasks",
        ResultsExpireIn: 3600,
        NoUnixSignals:   false,
    }
}

// NewClusterConfig 创建 Redis 集群配置
func NewClusterConfig(addrs []string) *Config {
    return &Config{
        RedisOptions: redis.UniversalOptions{
            Addrs: addrs,
        },
        DefaultQueue:    "machinery_tasks",
        ResultsExpireIn: 3600,
        NoUnixSignals:   false,
    }
}
```

---

## 优化后使用示例

```go
// 方式 1：使用默认配置（本地单机 Redis）
cnf := config.NewConfig()

// 方式 2：使用指定 Redis 地址
cnf := config.NewConfigWithRedis("192.168.1.100:6379")

// 方式 3：自定义 Redis 配置
cnf := &config.Config{
    RedisOptions: redis.UniversalOptions{
        Addrs:    []string{"localhost:6379"},
        Password: "mypassword",
        DB:       1,
        PoolSize: 20,
    },
    DefaultQueue:    "my_queue",
    ResultsExpireIn: 7200,
}

// 方式 4：Redis 集群
cnf := config.NewClusterConfig([]string{
    "192.168.1.101:6379",
    "192.168.1.102:6379",
    "192.168.1.103:6379",
})
```

---

## 实施步骤

1. **修改 config.go**
   - 删除 `RedisConfig` 结构体
   - 简化 `Config` 结构体字段（保留4个字段）
   - 内嵌 `redis.UniversalOptions`
   - 删除结构体标签
   - 添加构造函数
   - 优化注释

2. **删除文件**
   - `env.go`
   - `file.go`
   - `env_test.go`
   - `file_test.go`
   - `test.env`
   - `testconfig.yml`

3. **更新依赖**
   - 从 go.mod 中移除 `envconfig` 和 `yaml.v2`

4. **验证测试**
   - 运行所有测试确保通过

---

## 预期效果

1. **配置更简单** - 使用 go-redis 标准配置，无需学习自定义配置格式
2. **功能更强大** - 直接支持单机、Sentinel、Cluster 三种模式
3. **代码更简洁** - 删除 4 个文件，减少约 200 行代码
4. **维护更容易** - 减少外部依赖（envconfig, yaml.v2）
