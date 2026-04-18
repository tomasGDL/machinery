# 修复Makefile和重新生成README计划

## 当前状态分析

### Makefile
- 当前Makefile位于根目录 `d:\Tomas\projects\machinery\Makefile`
- 采用简化的命令格式，适用于Windows PowerShell环境
- 包含命令：fmt, lint, golint, test, test-with-coverage, ci
- 缺少v2子目录的特殊处理

### README.md
- 当前README.md混合了英文和中文内容，结构混乱
- 包含大量重复内容
- 目录索引与实际内容不匹配
- 部分代码示例可能过时

## 修复计划

### 1. 修复Makefile

#### 目标
创建一个更完善的Makefile，支持v2子目录的构建和测试

#### 具体步骤
1. 保留现有的简化命令格式
2. 添加v2子目录的专用命令
3. 确保命令在不同环境中都能正常执行
4. 添加help命令显示所有可用目标

#### 修改内容
```makefile
.PHONY: fmt lint golint test test-with-coverage ci help v2-fmt v2-lint v2-test

help:
	@echo "Available targets:"
	@echo "  fmt          - Format code"
	@echo "  lint         - Run linter"
	@echo "  golint       - Run golint"
	@echo "  test         - Run tests"
	@echo "  test-with-coverage - Run tests with coverage"
	@echo "  ci           - Run CI tests"
	@echo "  v2-fmt       - Format v2 code"
	@echo "  v2-lint      - Run v2 linter"
	@echo "  v2-test      - Run v2 tests"

fmt:
	go fmt ./...

lint:
	golangci-lint run ./...

golint:
	golint -set_exit_status ./...

test:
	go test ./...

test-with-coverage:
	@echo "" > coverage.out
	@echo "mode: set" > coverage-all.out
	go test -coverprofile=coverage.out -covermode=set ./...
	@tail -n +2 coverage.out >> coverage-all.out

ci:
	@echo "CI command requires docker-compose, skipping..."

# v2 targets
v2-fmt:
	cd v2 && go fmt ./...

v2-lint:
	cd v2 && golangci-lint run ./...

v2-test:
	cd v2 && go test ./...
```

### 2. 重新生成README.md

#### 目标
创建一个结构清晰、内容准确的README.md文档

#### 具体步骤
1. 删除当前混乱的README.md内容
2. 重新组织文档结构
3. 确保内容与实际代码库状态一致
4. 保持英文为主，中文为辅的语言结构

#### 新README结构
```markdown
# Machinery

Machinery is an asynchronous task queue/job queue based on distributed message passing.

[![GoDoc](https://godoc.org/github.com/RichardKnop/machinery/v2?status.svg)](https://godoc.org/github.com/RichardKnop/machinery/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/RichardKnop/machinery)](https://goreportcard.com/report/github.com/RichardKnop/machinery)
[![codecov](https://codecov.io/gh/RichardKnop/machinery/branch/master/graph/badge.svg)](https://codecov.io/gh/RichardKnop/machinery)

## Features

- Asynchronous task execution
- Task retry mechanism
- Task state tracking
- Workflow orchestration (Chain, Group, Chord)
- Periodic task scheduling
- Distributed locking
- Distributed tracing

## Installation

```bash
go get github.com/RichardKnop/machinery/v2
```

## Quick Start

### 1. Define Tasks

```go
func Add(args ...int64) (int64, error) {
    sum := int64(0)
    for _, arg := range args {
        sum += arg
    }
    return sum, nil
}
```

### 2. Create Server

```go
import (
    "github.com/RichardKnop/machinery/v2"
    "github.com/RichardKnop/machinery/v2/config"
    redisbackend "github.com/RichardKnop/machinery/v2/backends/redis"
    redisbroker "github.com/RichardKnop/machinery/v2/brokers/redis"
    redislock "github.com/RichardKnop/machinery/v2/locks/redis"
)

cnf := &config.Config{
    Broker:        "redis://localhost:6379",
    ResultBackend: "redis://localhost:6379",
    Lock:          "redis://localhost:6379",
    DefaultQueue:  "machinery_tasks",
}

broker := redisbroker.NewGR(cnf, []string{"localhost:6379"}, 0)
backend := redisbackend.NewGR(cnf, []string{"localhost:6379"}, 0)
lock := redislock.New(cnf, []string{"localhost:6379"}, 0, 3)

server := machinery.NewServer(cnf, broker, backend, lock)

server.RegisterTasks(map[string]interface{}{
    "add": Add,
})
```

### 3. Launch Worker

```go
worker := server.NewWorker("worker_name", 10)
err := worker.Launch()
```

### 4. Send Task

```go
signature := &tasks.Signature{
    Name: "add",
    Args: []tasks.Arg{
        {Type: "int64", Value: 1},
        {Type: "int64", Value: 2},
    },
}

asyncResult, err := server.SendTask(signature)
```

## Configuration

### Redis URL Format

```
redis://[password@]host[:port][/db_num]
```

### Redis Cluster

```go
cnf := &config.Config{
    Broker:        "redis://localhost:6379",
    ResultBackend: "redis://localhost:6379",
    Redis: &config.RedisConfig{
        ClusterMode: true,
    },
}
```

## Workflows

### Chain
Tasks executed sequentially, each task's result is passed to the next task.

### Group
Tasks executed in parallel.

### Chord
Group of tasks executed in parallel, then a callback task is executed.

## Periodic Tasks

```go
err := server.RegisterPeriodicTask("*/3 * * * *", "periodic-add", signature)
```

## Development

### Running Tests

```bash
make test
make v2-test
```

## License

MIT License
```

## 预期结果

1. Makefile包含完整的构建和测试命令，支持v2子目录
2. README.md结构清晰，内容准确，与实际代码库状态一致
3. 文档语言以英文为主，中文为辅
4. 所有代码示例都是最新且可执行的