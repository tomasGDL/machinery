# Machinery v2 - Redis Only

Machinery 是一个基于分布式消息传递的异步任务队列/作业队列库，使用 Go 语言开发。本版本（v2）是 Redis 专用版本，仅支持 Redis 作为消息代理和结果后端。

## 核心特性

- **异步任务执行** - 将耗时的操作放入队列异步执行
- **任务重试机制** - 支持基于 Fibonacci 序列的自动重试
- **任务状态追踪** - 完整记录任务生命周期状态（PENDING → RECEIVED → STARTED → SUCCESS/RETRY/FAILURE）
- **工作流编排** - 支持 Chain（链式）、Group（并行）、Chord（带回调的并行）工作流
- **定时任务** - 基于 cron 表达式的周期性任务调度
- **分布式锁** - 确保定时任务的原子性执行
- **分布式追踪** - 集成 OpenTracing

## 技术栈

- **Go 版本**: 1.15+
- **Redis 客户端**: 
  - [go-redis/redis](https://github.com/go-redis/redis) v8.6.0 (推荐，支持集群模式)
  - [gomodule/redigo](https://github.com/gomodule/redigo) v2.0.0 (传统支持)
- **分布式锁**: [go-redsync/redsync](https://github.com/go-redsync/redsync) v4.0.4
- **定时任务**: [robfig/cron](https://github.com/robfig/cron) v3.0.1
- **分布式追踪**: [opentracing/opentracing-go](https://github.com/opentracing/opentracing-go) v1.2.0

## 安装

```bash
go get github.com/RichardKnop/machinery/v2
```

## 快速开始

### 1. 定义任务

```go
package main

import (
    "github.com/RichardKnop/machinery/v2"
    "github.com/RichardKnop/machinery/v2/config"
    "github.com/RichardKnop/machinery/v2/tasks"
)

// 定义一个简单的加法任务
func Add(args ...int64) (int64, error) {
    sum := int64(0)
    for _, arg := range args {
        sum += arg
    }
    return sum, nil
}

// 定义一个乘法任务
func Multiply(args ...int64) (int64, error) {
    result := int64(1)
    for _, arg := range args {
        result *= arg
    }
    return result, nil
}
```

### 2. 创建 Server

```go
package main

import (
    "github.com/RichardKnop/machinery/v2"
    "github.com/RichardKnop/machinery/v2/config"
    redisbackend "github.com/RichardKnop/machinery/v2/backends/redis"
    redisbroker "github.com/RichardKnop/machinery/v2/brokers/redis"
    redislock "github.com/RichardKnop/machinery/v2/locks/redis"
)

func main() {
    // 配置
    cnf := &config.Config{
        Broker:        "redis://localhost:6379",
        ResultBackend: "redis://localhost:6379",
        Lock:          "redis://localhost:6379",
        DefaultQueue:  "machinery_tasks",
        Redis: &config.RedisConfig{
            MaxIdle:                3,
            IdleTimeout:            240,
            ReadTimeout:            15,
            WriteTimeout:           15,
            ConnectTimeout:         15,
            NormalTasksPollPeriod:  1000,
            DelayedTasksPollPeriod: 500,
        },
    }

    // 使用 go-redis (推荐，支持集群)
    broker := redisbroker.NewGR(cnf, []string{"localhost:6379"}, 0)
    backend := redisbackend.NewGR(cnf, []string{"localhost:6379"}, 0)
    lock := redislock.New(cnf, []string{"localhost:6379"}, 0, 3)

    // 或使用 redigo
    // broker := redisbroker.New(cnf, "localhost:6379", "", "", 0)
    // backend := redisbackend.New(cnf, "localhost:6379", "", "", 0)
    // lock := redislock.New(cnf, []string{"localhost:6379"}, 0, 3)

    server := machinery.NewServer(cnf, broker, backend, lock)

    // 注册任务
    server.RegisterTasks(map[string]interface{}{
        "add":      Add,
        "multiply": Multiply,
    })
}
```

### 3. 启动 Worker

```go
func main() {
    // ... 创建 server ...

    // 创建 worker
    worker := server.NewWorker("worker_name", 10)

    // 启动 worker
    err := worker.Launch()
    if err != nil {
        log.FATAL.Fatal(err)
    }
}
```

### 4. 发送任务

```go
func main() {
    // ... 创建 server ...

    // 创建任务签名
    signature := &tasks.Signature{
        Name: "add",
        Args: []tasks.Arg{
            {Type: "int64", Value: 1},
            {Type: "int64", Value: 2},
        },
    }

    // 发送任务
    asyncResult, err := server.SendTask(signature)
    if err != nil {
        log.FATAL.Fatal(err)
    }

    // 获取结果
    results, err := asyncResult.Get(time.Second * 5)
    if err != nil {
        log.FATAL.Fatal(err)
    }

    log.INFO.Printf("Result: %v", tasks.HumanReadableResults(results))
}
```

## 配置

### Redis URL 格式

```
redis://[password@]host[:port][/db_num]
redis+socket://[password@]/path/to/file.sock[:/db_num]
```

示例：
- `redis://localhost:6379`
- `redis://password@localhost:6379/0`
- `redis+socket://password@/tmp/redis.sock:/0`

### Redis 集群

```go
cnf := &config.Config{
    Broker:        "redis://localhost:6379",
    ResultBackend: "redis://localhost:6379",
    Redis: &config.RedisConfig{
        ClusterMode: true,
    },
}

// 集群模式需要多个地址
broker := redisbroker.NewGR(cnf, []string{
    "localhost:7000",
    "localhost:7001",
    "localhost:7002",
}, 0)
```

### 配置选项

```go
type RedisConfig struct {
    MaxIdle                int    // 最大空闲连接数 (默认: 3)
    MaxActive              int    // 最大活跃连接数 (默认: 100)
    IdleTimeout            int    // 空闲超时时间，秒 (默认: 240)
    Wait                   bool   // 连接池满时是否等待 (默认: true)
    ReadTimeout            int    // 读取超时，秒 (默认: 15)
    WriteTimeout           int    // 写入超时，秒 (默认: 15)
    ConnectTimeout         int    // 连接超时，秒 (默认: 15)
    NormalTasksPollPeriod  int    // 普通任务轮询周期，毫秒 (默认: 1000)
    DelayedTasksPollPeriod int    // 延迟任务轮询周期，毫秒 (默认: 500)
    DelayedTasksKey        string // 延迟任务存储键 (默认: "delayed_tasks")
    MasterName             string // Redis Sentinel 主节点名称
    ClusterMode            bool   // 是否启用集群模式
}
```

## 工作流

### Chain（链式任务）

任务按顺序执行，后一个任务以前一个任务的结果为参数：

```go
chain, _ := tasks.NewChain(&task1, &task2, &task3)
chainAsyncResult, _ := server.SendChain(chain)
results, _ := chainAsyncResult.Get(time.Second * 5)
```

### Group（并行任务）

所有任务同时并行执行：

```go
group, _ := tasks.NewGroup(&task1, &task2, &task3)
asyncResults, _ := server.SendGroup(group, 10) // 10 是并发数，0 表示无限制
```

### Chord（带回调的并行任务）

并行执行一组任务，全部完成后执行回调任务：

```go
group, _ := tasks.NewGroup(&task1, &task2, &task3)
chord, _ := tasks.NewChord(group, &callback)
chordAsyncResult, _ := server.SendChord(chord, 10)
```

## 定时任务

```go
// 注册定时任务（每 3 分钟执行一次）
signature := &tasks.Signature{
    Name: "add",
    Args: []tasks.Arg{
        {Type: "int64", Value: 1},
        {Type: "int64", Value: 1},
    },
}
err := server.RegisterPeriodicTask("*/3 * * * *", "periodic-add", signature)

// 注册定时 Chain
chain, _ := tasks.NewChain(&task1, &task2)
err = server.RegisterPeriodicChain("0 6 * * ?", "morning-chain", &task1, &task2)

// 注册定时 Group
group, _ := tasks.NewGroup(&task1, &task2)
err = server.RegisterPeriodicGroup("0 6 * * ?", "morning-group", 0, &task1, &task2)

// 注册定时 Chord
chord, _ := tasks.NewChord(group, &callback)
err = server.RegisterPeriodicChord("0 6 * * ?", "morning-chord", 0, &callback, &task1, &task2)
```

## 延迟任务

```go
// 延迟 5 秒执行
eta := time.Now().UTC().Add(time.Second * 5)
signature.ETA = &eta
asyncResult, _ := server.SendTask(signature)
```

## 任务重试

```go
// 设置重试次数
signature.RetryCount = 3
asyncResult, _ := server.SendTask(signature)

// 或在任务中返回重试错误
func MyTask() error {
    if temporaryFailure {
        return tasks.NewErrRetryTaskLater("try later", 4*time.Hour)
    }
    return nil
}
```

## 示例代码

查看 `example/` 目录获取更多示例：

- `example/go-redis/` - 使用 go-redis 客户端的示例
- `example/redigo/` - 使用 redigo 客户端的示例

运行示例：

```bash
# 启动 worker
cd example/redigo
go run main.go worker

# 发送任务
go run main.go send
```

## 测试

```bash
# 运行所有测试
go test ./...

# 运行测试（跳过 Redis 依赖的测试）
go test ./... -short
```

## 与 v1 的区别

1. **仅支持 Redis** - 移除了 AMQP、SQS、GCP PubSub、MongoDB、DynamoDB、Memcache 支持
2. **独立模块** - v2 是完全独立的模块，不依赖 v1
3. **依赖注入** - 通过构造函数注入 broker、backend、lock，而不是使用工厂模式
4. **更少的依赖** - 移除了大量第三方依赖，更轻量级

## License

MIT License
