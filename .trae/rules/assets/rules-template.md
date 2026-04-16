# Machinery 开发规则

> 本文档定义了 Machinery 项目的开发规范，适用于所有代码贡献者。

## 目录

1. [Go 语言与工具链要求](#1-go-语言与工具链要求)
2. [代码风格与格式化](#2-代码风格与格式化)
3. [命名约定](#3-命名约定)
4. [Go 特定规范](#4-go-特定规范)
5. [接口实现规范](#5-接口实现规范)
6. [包导入规则](#6-包导入规则)
7. [错误处理规范](#7-错误处理规范)
8. [测试规范](#8-测试规范)
9. [依赖管理](#9-依赖管理)
10. [CI/CD 规范](#10-cicd-规范)

---

## 1. Go 语言与工具链要求

### 1.1 Go 版本

| 要求 | 最小版本 | 推荐版本 |
|------|----------|----------|
| Go | 1.15 | 1.18+ |

### 1.2 必需工具

| 工具 | 用途 | 安装方式 |
|------|------|----------|
| go | Go 编译器 | 官方安装 |
| go fmt | 代码格式化 | go install golang.org/x/tools/cmd/goformat@latest |
| goimports | 导入排序 | go install golang.org/x/tools/cmd/goimports@latest |
| gometalinter | Lint 检查 | go install github.com/mgechev/gometalinter@latest |
| golint | 代码风格检查 | go install golang.org/x/lint/golint@latest |

### 1.3 配置文件

- **go.mod**: 使用 Go Modules 管理依赖
- **go.sum**: 依赖校验文件（必须提交）
- **Makefile**: 构建和测试脚本

---

## 2. 代码风格与格式化

### 2.1 格式化规则

**必须执行的格式化命令：**

```bash
# 代码格式化（所有包）
make fmt

# 或手动执行
go fmt ./...
```

**格式化标准：**

- 缩进：使用 Tab，不是空格
- 行宽：无硬限制，但建议不超过 120 字符
- 空行：包声明后空一行，函数间空一行
- 括号：左括号不换行

### 2.2 Lint 检查规则

**必须执行的 Lint 命令：**

```bash
make lint
```

**关键 Lint 规则：**

| 规则 | 工具 | 说明 |
|------|------|------|
| vet | go vet | 标准检查 |
| gofmt | gofmt | 格式检查 |
| misspell | misspell | 拼写检查 |
| ineffassign | ineffassign | 无效赋值检测 |
| goimports | goimports | 导入检查 |
| deadcode | deadcode | 死代码检测 |

### 2.3 IDE 配置

**VS Code (settings.json):**

```json
{
  "go.formatTool": "goimports",
  "editor.formatOnSave": true,
  "[go]": {
    "editor.formatOnSave": true
  }
}
```

---

## 3. 命名约定

### 3.1 文件命名

| 类型 | 规则 | 示例 |
|------|------|------|
| Go 源文件 | 小写下划线或驼峰 | `redis.go`, `redis_test.go` |
| 目录 | 小写下划线 | `backends/redis/` |
| 接口文件 | 小写下划线 | `interfaces.go` |

### 3.2 结构体与类型

| 类型 | 命名规则 | 示例 |
|------|----------|------|
| 结构体 | 大写开头（导出） | `type Broker struct` |
| 结构体 | 小写开头（非导出） | `type broker struct` |
| 接口 | 大写开头，以 `er` 结尾 | `type Broker interface` |
| 类型别名 | 大写开头 | `type Handler func()` |

### 3.3 函数与方法

| 类型 | 命名规则 | 示例 |
|------|----------|------|
| 导出函数 | 大写开头 | `func NewServer(...)` |
| 非导出函数 | 小写开头 | `func newBroker()` |
| 方法 | 大写开头（导出）或小写（非导出） | `func (b *Broker) Publish(...)` |
| getter | 大写开头 | `func (s *Server) GetConfig()` |
| setter | 大写开头，以 `Set` 开头 | `func (s *Server) SetBroker(...)` |

### 3.4 变量与常量

| 类型 | 命名规则 | 示例 |
|------|----------|------|
| 常量 | 大写下划线或驼峰 | `const MaxRetries = 3` |
| 全局变量 | 大写或驼峰 | `var DefaultQueue = "machinery_tasks"` |
| 局部变量 | 短命名 | `cnf`, `err`, `sig` |
| 布尔变量 | 以 `is`, `has`, `can` 开头 | `isCompleted`, `hasError` |

### 3.5 包命名

| 规则 | 示例 |
|------|------|
| 小写 | `backends`, `brokers`, `locks` |
| 简短 | `redis`, `amqp`, `sqs` |
| 不使用下划线 | `gcppubsub`（可接受） |

---

## 4. Go 特定规范

### 4.1 错误处理（CRITICAL）

**规则：**

- 函数必须返回 `error` 作为最后一个返回值
- 不要忽略错误（使用 `_` 接收必须添加注释）
- 错误信息必须包含上下文

**正确示例：**

```go
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
    msg, err := json.Marshal(signature)
    if err != nil {
        return fmt.Errorf("JSON marshal error: %s", err)
    }
    return nil
}
```

**错误示例：**

```go
// ❌ 忽略错误
data, _ := json.Marshal(signature)

// ❌ 错误信息不明确
if err != nil {
    return err
}
```

### 4.2 Context 传递

**规则：**

- Context 必须作为第一个参数传递
- 使用 `context.Background()` 创建顶级 context
- 使用 `context.WithCancel`、`context.WithTimeout` 创建子 context

**正确示例：**

```go
func (server *Server) SendTaskWithContext(ctx context.Context, signature *tasks.Signature) (*result.AsyncResult, error) {
    // ...
}
```

### 4.3 Defer 使用

**规则：**

- 使用 defer 关闭资源（文件、连接）
- defer 总是执行，即使发生 panic

**正确示例：**

```go
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
    conn := b.open()
    defer conn.Close()
    // ...
}
```

### 4.4 Goroutine 管理

**规则：**

- 所有 goroutine 必须有退出机制
- 使用 WaitGroup 确保完成
- 避免 goroutine 泄漏

**正确示例：**

```go
var wg sync.WaitGroup
wg.Add(1)
go func() {
    defer wg.Done()
    // work
}()
wg.Wait()
```

### 4.5 禁止的写法

| 禁止 | 替代方案 |
|------|----------|
| nil channel 发送/接收 | 使用 make 创建 channel |
| shared map 不同步 | 使用 sync.Map 或 mutex |
| time.Sleep 在 goroutine | 使用 channel 或 context |
| select default 忙轮询 | 使用 channel 通知 |
| interface{} 滥用 | 使用具体类型或泛型 |

---

## 5. 接口实现规范（CRITICAL）

### 5.1 Broker 接口

所有 Broker 实现必须实现 `iface.Broker` 接口：

```go
type Broker interface {
    GetConfig() *config.Config
    SetRegisteredTaskNames(names []string)
    IsTaskRegistered(name string) bool
    StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
    StopConsuming()
    Publish(ctx context.Context, task *tasks.Signature) error
    GetPendingTasks(queue string) ([]*tasks.Signature, error)
    GetDelayedTasks() ([]*tasks.Signature, error)
    AdjustRoutingKey(s *tasks.Signature)
}
```

**实现要求：**

| 方法 | 实现要求 |
|------|----------|
| `GetConfig` | 返回 broker 配置 |
| `SetRegisteredTaskNames` | 存储已注册任务名列表 |
| `IsTaskRegistered` | 检查任务是否已注册 |
| `StartConsuming` | 启动消费循环，必须处理优雅退出 |
| `StopConsuming` | 停止消费，必须释放所有资源 |
| `Publish` | 发布任务到消息队列 |
| `GetPendingTasks` | 获取待处理任务 |
| `GetDelayedTasks` | 获取延迟任务 |
| `AdjustRoutingKey` | 根据配置调整路由键 |

### 5.2 Backend 接口

所有 Backend 实现必须实现 `iface.Backend` 接口：

```go
type Backend interface {
    // Group 相关
    InitGroup(groupUUID string, taskUUIDs []string) error
    GroupCompleted(groupUUID string, groupTaskCount int) (bool, error)
    GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error)
    TriggerChord(groupUUID string) (bool, error)

    // 状态管理
    SetStatePending(signature *tasks.Signature) error
    SetStateReceived(signature *tasks.Signature) error
    SetStateStarted(signature *tasks.Signature) error
    SetStateRetry(signature *tasks.Signature) error
    SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error
    SetStateFailure(signature *tasks.Signature, err string) error
    GetState(taskUUID string) (*tasks.TaskState, error)

    // 清理
    IsAMQP() bool
    PurgeState(taskUUID string) error
    PurgeGroupMeta(groupUUID string) error
}
```

**实现要求：**

| 方法 | 实现要求 |
|------|----------|
| `InitGroup` | 初始化组元数据 |
| `GroupCompleted` | 检查组是否完成 |
| `GroupTaskStates` | 获取组内所有任务状态 |
| `TriggerChord` | 触发 Chord，确保只触发一次 |
| `SetStateXxx` | 设置任务状态，必须原子操作 |
| `GetState` | 获取任务状态 |
| `IsAMQP` | 返回是否使用 AMQP |
| `PurgeState` | 删除任务状态 |
| `PurgeGroupMeta` | 删除组元数据 |

### 5.3 Lock 接口

所有 Lock 实现必须实现 `iface.Lock` 接口：

```go
type Lock interface {
    LockWithRetries(key string, value int64) error
    Lock(key string, value int64) error
}
```

**实现要求：**

| 方法 | 实现要求 |
|------|----------|
| `Lock` | 尝试获取锁，一次性 |
| `LockWithRetries` | 带重试的锁获取 |

### 5.4 TaskProcessor 接口

Worker 必须实现 `iface.TaskProcessor` 接口：

```go
type TaskProcessor interface {
    Process(signature *tasks.Signature) error
    CustomQueue() string
    PreConsumeHandler() bool
}
```

**实现要求：**

| 方法 | 实现要求 |
|------|----------|
| `Process` | 处理任务签名，执行任务，更新状态 |
| `CustomQueue` | 返回自定义队列名，空字符串表示默认队列 |
| `PreConsumeHandler` | 消费前检查，返回 true 继续，返回 false 跳过 |

---

## 6. 包导入规则

### 6.1 导入分组与排序

**分组顺序：**

1. 标准库
2. 第三方库
3. 本项目库

**组内排序：**按字母顺序

**正确示例：**

```go
import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/opentracing/opentracing-go"
    "github.com/robfig/cron/v3"

    "github.com/RichardKnop/machinery/v2/config"
    "github.com/RichardKnop/machinery/v2/tasks"
)
```

### 6.2 循环依赖

**禁止：**任何包之间存在循环依赖

**检查方法：**

```bash
go mod graph | grep ":"
```

### 6.3 iface 包使用

- 接口定义放在 `iface` 子包中
- 实际实现放在具体命名的包中（如 `redis`、`amqp`）

**正确结构：**

```
backends/
├── iface/
│   └── interfaces.go  # 定义 Backend 接口
├── redis/
│   └── redis.go      # 实现 Backend 接口
└── memcache/
    └── memcache.go   # 实现 Backend 接口
```

---

## 7. 错误处理规范

### 7.1 自定义错误类型

**推荐模式：**

```go
// 使用 errors.New 创建简单错误
var ErrConsumerStopped = errors.New("consumer stopped")

// 使用 fmt.Errorf 创建带上下文的错误
return fmt.Errorf("Set state to 'received' for task %s returned error: %s", signature.UUID, err)
```

### 7.2 错误日志规范

**Worker 错误处理：**

```go
// 使用 log.ERROR 记录错误
if err != nil {
    log.ERROR.Printf("Failed processing task %s. Error = %v", signature.UUID, err)
}
```

** Broker 错误处理：**

```go
// 连接错误应该触发重试
if b.GetRetry() {
    return b.GetRetry(), err
}
```

### 7.3 错误传播

**规则：**

- 底层错误包装高层错误
- 使用 `fmt.Errorf` 带 `%w` 包装
- 错误信息描述操作而非技术细节

**正确示例：**

```go
if err := b.backend.SetStatePending(signature); err != nil {
    return nil, fmt.Errorf("Set state pending error: %s", err)
}
```

---

## 8. 测试规范

### 8.1 单元测试要求

**文件命名：**`*_test.go`

**测试函数命名：**`TestXxx`

**正确示例：**

```go
func TestBroker_Publish(t *testing.T) {
    // test code
}
```

**运行测试：**

```bash
make test
```

### 8.2 集成测试要求

**目录：**`integration-tests/`

**运行方式：**使用 Docker Compose

```bash
make ci
```

**环境变量：**

```bash
export AMQP_URL=amqp://guest:guest@localhost:5672/
export REDIS_URL=localhost:6379
export MEMCACHE_URL=localhost:11211
export MONGODB_URL=localhost:27017
```

### 8.3 Mock 使用规范

**允许使用 Mock 的场景：**

- 网络调用（broker、backend）
- 时间相关操作
- 随机数生成

**不允许使用 Mock 的场景：**

- 简单工具函数
- 已有可靠实现的函数

### 8.4 测试覆盖

**最低要求：**核心逻辑（Broker、Backend、Worker）必须有测试

**查看覆盖率：**

```bash
make test-with-coverage
```

---

## 9. 依赖管理

### 9.1 Go Modules 规范

**初始化：**

```bash
go mod init github.com/RichardKnop/machinery/v2
```

**添加依赖：**

```bash
go get github.com/some/package@latest
```

**更新依赖：**

```bash
go get -u all
```

### 9.2 版本锁定

- **必须提交** `go.sum` 文件
- 使用 `go mod verify` 验证依赖

### 9.3 replace 指令

**仅在以下情况使用：**

- 本地开发调试
- fork 的第三方包

**示例：**

```go
replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
```

---

## 10. CI/CD 规范

### 10.1 Makefile 命令

| 命令 | 说明 |
|------|------|
| `make fmt` | 格式化所有代码 |
| `make lint` | 运行 Lint 检查 |
| `make golint` | 运行 Go Lint |
| `make test` | 运行单元测试 |
| `make test-with-coverage` | 运行测试并生成覆盖率报告 |
| `make ci` | 运行 Docker Compose 集成测试 |

### 10.2 构建检查

**提交前必须执行：**

```bash
make fmt
make lint
make test
```

### 10.3 Docker 测试环境

**使用 docker-compose.test.yml：**

```bash
docker-compose -f docker-compose.test.yml -p machinery_ci up --build --abort-on-container-exit --exit-code-from sut
```

---

## 附录 A：快速检查清单

### 提交代码前必须检查：

- [ ] 代码已通过 `make fmt`
- [ ] 代码已通过 `make lint`
- [ ] 单元测试通过 `make test`
- [ ] 无循环依赖
- [ ] 所有错误都已处理
- [ ] 接口实现完整（Broker/Backend/Lock）
- [ ] 测试覆盖核心逻辑
- [ ] go.mod 和 go.sum 已更新

### 代码审查重点：

- [ ] 错误处理是否完整
- [ ] 是否有 goroutine 泄漏
- [ ] Context 是否正确传递
- [ ] 接口实现是否符合规范
- [ ] 命名是否符合约定
- [ ] 注释是否清晰准确

---

## 附录 B：常见错误对照表

| 错误类型 | 错误代码 | 正确代码 |
|----------|----------|----------|
| 忽略错误 | `data, _ := json.Marshal(s)` | `data, err := json.Marshal(s); if err != nil { return err }` |
| 错误不明确 | `return err` | `return fmt.Errorf("操作描述: %w", err)` |
| Context 缺失 | `broker.Publish(sig)` | `broker.Publish(ctx, sig)` |
| 资源未关闭 | `conn := b.open()` | `conn := b.open(); defer conn.Close()` |
| 接口实现不完整 | 只实现 5 个方法 | 实现所有 9 个方法 |

---

## 附录 C：参考资源

- [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- [Effective Go](https://go.dev/doc/effective_go)
- [Go Linter Documentation](https://golangci-lint.run/)
- [Testify Documentation](https://github.com/stretchr/testify)