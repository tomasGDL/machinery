# Machinery Logger 重构计划

## 目标
重构 `v2/log` 包，定义通用的 `Logger` 接口，将当前实现作为 `innerLogger`，并提供 `UseLogger` 接口供外部设置 machinery 使用的 logger。同时需要调整所有使用 log 的代码。

## 背景分析

### 当前实现
- 当前使用 `github.com/RichardKnop/logging` 包
- `logging.Logger` 是一个数组类型 `[]LoggerInterface`，索引对应不同日志级别
- 通过全局变量 `DEBUG`, `INFO`, `WARNING`, `ERROR`, `FATAL` 提供日志级别
- 提供 `Set` 和 `SetXxx` 方法设置自定义 logger

### 当前 log 使用方式
代码中使用 `log.DEBUG.Print()`, `log.INFO.Printf()`, `log.WARNING.Printf()`, `log.ERROR.Printf()`, `log.FATAL.Print()` 等方式记录日志。

### 需求
1. 定义新的 `Logger` 接口，支持 printf 风格的格式化输出（方法名使用 Debugf, Infof 等）
2. 当前实现作为 `innerLogger`，使用 `type innerLogger struct {logger logging.Logger}` 包装
3. 提供 `UseLogger` 接口供外部设置 machinery 使用的 logger
4. 调整所有使用 log 的代码，改为使用新的 `Logger` 接口

## 实现步骤

### 1. 定义 Logger 接口
在 `log.go` 中定义新的 `Logger` 接口，方法名使用 `Debugf`, `Infof`, `Warnf`, `Errorf`, `Fatalf`：

```go
// Logger 是一个通用的日志接口，采用 printf 风格的格式化输出。
type Logger interface {
    // Debugf 记录调试级别日志，支持 format 和参数
    Debugf(format string, args ...interface{})
    // Infof 记录信息级别日志
    Infof(format string, args ...interface{})
    // Warnf 记录警告级别日志
    Warnf(format string, args ...interface{})
    // Errorf 记录错误级别日志
    Errorf(format string, args ...interface{})
    // Fatalf 记录致命错误并退出程序
    Fatalf(format string, args ...interface{})
}
```

### 2. 创建 innerLogger 结构体
使用结构体包装 `logging.Logger`：

```go
// innerLogger 是内部 logger 实现，包装 logging.Logger
type innerLogger struct {
    logger logging.Logger
}
```

### 3. 实现 Logger 接口方法
为 `innerLogger` 实现 `Logger` 接口的所有方法：

```go
func (l *innerLogger) Debugf(format string, args ...interface{}) {
    l.logger[logging.DEBUG].Printf(format, args...)
}

func (l *innerLogger) Infof(format string, args ...interface{}) {
    l.logger[logging.INFO].Printf(format, args...)
}

func (l *innerLogger) Warnf(format string, args ...interface{}) {
    l.logger[logging.WARNING].Printf(format, args...)
}

func (l *innerLogger) Errorf(format string, args ...interface{}) {
    l.logger[logging.ERROR].Printf(format, args...)
}

func (l *innerLogger) Fatalf(format string, args ...interface{}) {
    l.logger[logging.FATAL].Printf(format, args...)
}
```

### 4. 提供 UseLogger 接口
添加 `UseLogger` 函数，允许外部设置自定义 logger：

```go
// currentLogger 是当前使用的 logger 实例
var currentLogger Logger

// UseLogger 设置 machinery 使用的 logger
func UseLogger(l Logger) {
    currentLogger = l
}

// GetLogger 获取当前使用的 logger 实例
func GetLogger() Logger {
    return currentLogger
}
```

### 5. 保持向后兼容
保留原有的全局变量和 `Set` 方法，确保现有代码不受影响：
- 保留 `DEBUG`, `INFO`, `WARNING`, `ERROR`, `FATAL` 全局变量
- 保留 `Set`, `SetDebug`, `SetInfo`, `SetWarning`, `SetError`, `SetFatal` 方法

### 6. 初始化默认 logger
在 `init` 函数中初始化默认的 `innerLogger`：

```go
func init() {
    logger := logging.New(nil, nil, new(logging.ColouredFormatter))
    
    // 初始化全局变量（向后兼容）
    DEBUG = logger[logging.DEBUG]
    INFO = logger[logging.INFO]
    WARNING = logger[logging.WARNING]
    ERROR = logger[logging.ERROR]
    FATAL = logger[logging.FATAL]
    
    // 初始化新的 Logger 接口实现
    currentLogger = &innerLogger{logger: logger}
}
```

### 7. 更新测试
更新 `log_test.go`，添加对新接口的测试：

```go
func TestUseLogger(t *testing.T) {
    // 测试 UseLogger 和 GetLogger
    customLogger := &mockLogger{}
    UseLogger(customLogger)
    
    if GetLogger() != customLogger {
        t.Error("UseLogger did not set the logger correctly")
    }
    
    // 恢复默认 logger
    UseLogger(defaultLogger)
}

func TestInnerLogger(t *testing.T) {
    // 测试 innerLogger 的各个方法
    log := GetLogger()
    log.Debugf("debug message: %s", "test")
    log.Infof("info message: %s", "test")
    log.Warnf("warn message: %s", "test")
    log.Errorf("error message: %s", "test")
    // Fatalf 会退出程序，不测试
}
```

### 8. 调整使用 log 的代码
将所有使用 `log.DEBUG.Print()`, `log.INFO.Printf()` 等的地方改为使用新的接口。需要修改的文件：

| 文件 | 修改内容 |
|------|----------|
| v2/server.go | `log.ERROR.Printf(...)` → `log.GetLogger().Errorf(...)` |
| v2/worker.go | `log.INFO.Printf(...)`, `log.WARNING.Printf(...)`, `log.ERROR.Printf(...)`, `log.DEBUG.Printf(...)` → 对应的新接口 |
| v2/tasks/task.go | `log.ERROR.Printf(...)` → `log.GetLogger().Errorf(...)` |
| v2/common/broker.go | `log.WARNING.Print(...)` → `log.GetLogger().Warnf(...)` |
| v2/retry/retry.go | `log.WARNING.Printf(...)` → `log.GetLogger().Warnf(...)` |
| v2/brokers/redis/goredis.go | `log.INFO.Print(...)`, `log.WARNING.Printf(...)`, `log.ERROR.Print(...)`, `log.DEBUG.Printf(...)` → 对应的新接口 |
| v2/backends/redis/goredis.go | `log.ERROR.Print(...)` → `log.GetLogger().Errorf(...)` |
| v2/example/tasks/tasks.go | `log.INFO.Print(...)` → `log.GetLogger().Infof(...)` |

#### 修改示例

**修改前：**
```go
log.ERROR.Printf("periodic task failed. task name is: %s. error is %s", name, err.Error())
```

**修改后：**
```go
log.GetLogger().Errorf("periodic task failed. task name is: %s. error is %s", name, err.Error())
```

**修改前：**
```go
log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")
```

**修改后：**
```go
log.GetLogger().Infof("[*] Waiting for messages. To exit press CTRL+C")
```

## 文件变更

### log.go
- 添加 `Logger` 接口定义（方法名为 Debugf, Infof, Warnf, Errorf, Fatalf）
- 添加 `innerLogger` 结构体（包装 logging.Logger）
- 实现 `Logger` 接口方法
- 添加 `UseLogger` 和 `GetLogger` 函数
- 修改 `init` 函数初始化逻辑

### log_test.go
- 添加 `mockLogger` 用于测试
- 添加 `TestUseLogger` 测试
- 添加 `TestInnerLogger` 测试

### 其他使用 log 的文件
- server.go
- worker.go
- tasks/task.go
- common/broker.go
- retry/retry.go
- brokers/redis/goredis.go
- backends/redis/goredis.go
- example/tasks/tasks.go

## 验证清单

- [ ] `Logger` 接口正确定义（方法名为 Debugf, Infof, Warnf, Errorf, Fatalf）
- [ ] `innerLogger` 使用 `struct {logger logging.Logger}`，正确实现 `Logger` 接口
- [ ] `UseLogger` 可以设置自定义 logger
- [ ] `GetLogger` 可以获取当前 logger
- [ ] 所有使用 log 的代码已调整为使用新的接口
- [ ] 向后兼容：原有全局变量和方法仍然可用
- [ ] 所有测试通过
