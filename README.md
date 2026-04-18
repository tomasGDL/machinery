# Machinery

Machinery is an asynchronous task queue/job queue based on distributed message passing.

[![GoDoc](https://godoc.org/github.com/RichardKnop/machinery/v2?status.svg)](https://godoc.org/github.com/RichardKnop/machinery/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/RichardKnop/machinery)](https://goreportcard.com/report/github.com/RichardKnop/machinery)
[![codecov](https://codecov.io/gh/RichardKnop/machinery/branch/master/graph/badge.svg)](https://codecov.io/gh/RichardKnop/machinery)

---

## Features

- **Asynchronous Task Execution** - Queue time-consuming operations for async processing
- **Task Retry Mechanism** - Automatic retry with Fibonacci backoff
- **Task State Tracking** - Complete lifecycle tracking (PENDING → RECEIVED → STARTED → SUCCESS/RETRY/FAILURE)
- **Workflow Orchestration** - Chain (sequential), Group (parallel), Chord (parallel + callback)
- **Periodic Task Scheduling** - Cron-based periodic task execution
- **Distributed Locking** - Ensures atomic execution of scheduled tasks
- **Distributed Tracing** - OpenTracing integration

## Tech Stack

- **Go Version**: 1.24+
- **Redis Client**: [go-redis/redis/v9](https://github.com/redis/go-redis) v9.17.3
- **Distributed Lock**: [go-redsync/redsync/v4](https://github.com/go-redsync/redsync) v4.16.0
- **Cron Scheduler**: [robfig/cron/v3](https://github.com/robfig/cron) v3.0.1
- **Distributed Tracing**: [opentracing/opentracing-go](https://github.com/opentracing/opentracing-go) v1.2.0

## Installation

```bash
go get github.com/RichardKnop/machinery/v2
```

## Quick Start

### 1. Define Tasks

```go
package main

import (
    "github.com/RichardKnop/machinery/v2/tasks"
)

// Add - a simple addition task
func Add(args ...int64) (int64, error) {
    sum := int64(0)
    for _, arg := range args {
        sum += arg
    }
    return sum, nil
}

// Multiply - a multiplication task
func Multiply(args ...int64) (int64, error) {
    result := int64(1)
    for _, arg := range args {
        result *= arg
    }
    return result, nil
}
```

### 2. Create Server

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
    // Configuration
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

    // Create broker, backend, and lock using go-redis
    broker := redisbroker.NewGR(cnf, []string{"localhost:6379"}, 0)
    backend := redisbackend.NewGR(cnf, []string{"localhost:6379"}, 0)
    lock := redislock.New(cnf, []string{"localhost:6379"}, 0, 3)

    // Create server with dependency injection
    server := machinery.NewServer(cnf, broker, backend, lock)

    // Register tasks
    server.RegisterTasks(map[string]interface{}{
        "add":      Add,
        "multiply": Multiply,
    })
}
```

### 3. Launch Worker

```go
func main() {
    // ... create server ...

    // Create worker with 10 concurrent goroutines
    worker := server.NewWorker("worker_name", 10)

    // Launch worker
    err := worker.Launch()
    if err != nil {
        // handle error
    }
}
```

### 4. Send Task

```go
import (
    "github.com/RichardKnop/machinery/v2/tasks"
)

signature := &tasks.Signature{
    Name: "add",
    Args: []tasks.Arg{
        {Type: "int64", Value: 1},
        {Type: "int64", Value: 2},
    },
}

asyncResult, err := server.SendTask(signature)
if err != nil {
    // handle error
}

// Get result (blocking with timeout)
results, err := asyncResult.Get(time.Second * 5)
if err != nil {
    // handle error
}
```

## Configuration

### Redis URL Format

```
redis://[password@]host[:port][/db_num]
redis+socket://[password@]/path/to/file.sock[:/db_num]
```

Examples:
- `redis://localhost:6379`
- `redis://password@localhost:6379/0`
- `redis+socket://password@/tmp/redis.sock:/0`

### Redis Cluster

```go
cnf := &config.Config{
    Broker:        "redis://localhost:6379",
    ResultBackend: "redis://localhost:6379",
    Lock:          "redis://localhost:6379",
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

// Multiple addresses for cluster mode
broker := redisbroker.NewGR(cnf, []string{
    "localhost:7000",
    "localhost:7001",
    "localhost:7002",
}, 0)
```

### Configuration Options

```go
type Config struct {
    Broker        string // Broker URL
    DefaultQueue  string // Default queue name
    ResultBackend string // Result backend URL
    Lock          string // Lock backend URL
}

type RedisConfig struct {
    MaxIdle                int    // Max idle connections (default: 3)
    MaxActive              int    // Max active connections (default: 100)
    IdleTimeout            int    // Idle timeout in seconds (default: 240)
    Wait                   bool   // Wait when pool is full (default: true)
    ReadTimeout            int    // Read timeout in seconds (default: 15)
    WriteTimeout           int    // Write timeout in seconds (default: 15)
    ConnectTimeout         int    // Connect timeout in seconds (default: 15)
    NormalTasksPollPeriod  int    // Normal task polling period in ms (default: 1000)
    DelayedTasksPollPeriod int    // Delayed task polling period in ms (default: 500)
    DelayedTasksKey        string // Delayed tasks key (default: "delayed_tasks")
    MasterName             string // Redis Sentinel master name
}
```

## Workflows

### Chain (Sequential)

Tasks executed sequentially, each task's result is passed to the next task.

```go
signature1 := tasks.Signature{
    Name: "add",
    Args: []tasks.Arg{
        {Type: "int64", Value: 1},
        {Type: "int64", Value: 1},
    },
}

signature2 := tasks.Signature{
    Name: "add",
    Args: []tasks.Arg{
        {Type: "int64", Value: 5},
        {Type: "int64", Value: 5},
    },
}

signature3 := tasks.Signature{
    Name: "multiply",
    Args: []tasks.Arg{
        {Type: "int64", Value: 4},
    },
}

chain, _ := tasks.NewChain(&signature1, &signature2, &signature3)
chainAsyncResult, err := server.SendChain(chain)
```

Result: `4 * (5 + 5 + (1 + 1)) = 48`

### Group (Parallel)

Tasks executed in parallel.

```go
signature1 := tasks.Signature{Name: "add", Args: []tasks.Arg{{Type: "int64", Value: 1}, {Type: "int64", Value: 1}}}
signature2 := tasks.Signature{Name: "add", Args: []tasks.Arg{{Type: "int64", Value: 5}, {Type: "int64", Value: 5}}}

group, _ := tasks.NewGroup(&signature1, &signature2)
asyncResults, err := server.SendGroup(group, 10) // 10 is concurrency, 0 means unlimited
```

### Chord (Parallel + Callback)

Group of tasks executed in parallel, then a callback task is executed.

```go
signature1 := tasks.Signature{Name: "add", Args: []tasks.Arg{{Type: "int64", Value: 1}, {Type: "int64", Value: 1}}}
signature2 := tasks.Signature{Name: "add", Args: []tasks.Arg{{Type: "int64", Value: 5}, {Type: "int64", Value: 5}}}
callback := tasks.Signature{Name: "multiply"}

group, _ := tasks.NewGroup(&signature1, &signature2)
chord, _ := tasks.NewChord(group, &callback)
chordAsyncResult, err := server.SendChord(chord, 10)
```

Result: `(1 + 1) * (5 + 5) = 20`

## Task Management

### Delayed Tasks

```go
eta := time.Now().UTC().Add(time.Second * 5)
signature.ETA = &eta
asyncResult, err := server.SendTask(signature)
```

### Retry Tasks

```go
signature.RetryCount = 3
asyncResult, err := server.SendTask(signature)

// Or return retry error in task
func MyTask() error {
    if temporaryFailure {
        return tasks.NewErrRetryTaskLater("try later", 4*time.Hour)
    }
    return nil
}
```

### Periodic Tasks

```go
signature := &tasks.Signature{
    Name: "add",
    Args: []tasks.Arg{
        {Type: "int64", Value: 1},
        {Type: "int64", Value: 1},
    },
}

// Register periodic task (every 3 minutes)
err := server.RegisterPeriodicTask("*/3 * * * *", "periodic-add", signature)

// Register periodic chain
err = server.RegisterPeriodicChain("0 6 * * ?", "morning-chain", chain)

// Register periodic group
err = server.RegisterPeriodicGroup("0 6 * * ?", "morning-group", group)

// Register periodic chord
err = server.RegisterPeriodicChord("0 6 * * ?", "morning-chord", chord)
```

## Task States

```go
const (
    StatePending   = "PENDING"   // Initial state
    StateReceived  = "RECEIVED"  // Task received by worker
    StateStarted   = "STARTED"   // Worker started processing
    StateRetry     = "RETRY"     // Failed task scheduled for retry
    StateSuccess   = "SUCCESS"   // Task processed successfully
    StateFailure   = "FAILURE"   // Task processing failed
)
```

### Check Task State

```go
taskState := asyncResult.GetState()
fmt.Printf("Current state of %v task is: %s\n", taskState.TaskUUID, taskState.State)

asyncResult.GetState().IsCompleted()
asyncResult.GetState().IsSuccess()
asyncResult.GetState().IsFailure()
```

## Custom Logger

Implement the following interface:

```go
type Interface interface {
    Print(...interface{})
    Printf(string, ...interface{})
    Println(...interface{})
    Fatal(...interface{})
    Fatalf(string, ...interface{})
    Fatalln(...interface{})
    Panic(...interface{})
    Panicf(string, ...interface{})
    Panicln(...interface{})
}

log.Set(myCustomLogger)
```

## Development

### Requirements

- Go 1.24+
- Redis

### Running Tests

```bash
# Run all tests
make v2-test

# Run tests with coverage
make v2-test-with-coverage

# Format code
make v2-fmt

# Run linter
make v2-lint
```

### Project Structure

```
v2/
├── backends/          # Result backends
│   ├── iface/         # Backend interfaces
│   ├── redis/         # Redis backend implementation
│   └── result/        # Async result handling
├── brokers/           # Message brokers
│   ├── errs/          # Broker errors
│   ├── iface/         # Broker interfaces
│   └── redis/         # Redis broker implementation
├── locks/             # Distributed locks
│   ├── iface/         # Lock interfaces
│   └── redis/         # Redis lock implementation
├── config/            # Configuration
├── common/            # Common utilities
├── example/           # Example code
├── log/               # Logging
├── retry/             # Retry mechanisms
├── tasks/             # Task definitions and handling
├── tracing/           # Distributed tracing
├── utils/             # Utility functions
├── server.go          # Main server
└── worker.go          # Worker implementation
```

## License

MIT License
