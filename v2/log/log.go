package log

import (
	"github.com/RichardKnop/logging"
)

var (
	logger = logging.New(nil, nil, new(logging.ColouredFormatter))

	// DEBUG ...
	DEBUG = logger[logging.DEBUG]
	// INFO ...
	INFO = logger[logging.INFO]
	// WARNING ...
	WARNING = logger[logging.WARNING]
	// ERROR ...
	ERROR = logger[logging.ERROR]
	// FATAL ...
	FATAL = logger[logging.FATAL]

	// currentLogger 是当前使用的 logger 实例
	currentLogger Logger
)

func init() {
	// 初始化新的 Logger 接口实现
	currentLogger = &innerLogger{logger: logger}
}

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

// innerLogger 是内部 logger 实现，包装 logging.Logger
type innerLogger struct {
	logger logging.Logger
}

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

// UseLogger 设置 machinery 使用的 logger
func UseLogger(l Logger) {
	currentLogger = l
}

// GetLogger 获取当前使用的 logger 实例
func GetLogger() Logger {
	return currentLogger
}

// Set sets a custom logger for all log levels
func Set(l logging.LoggerInterface) {
	DEBUG = l
	INFO = l
	WARNING = l
	ERROR = l
	FATAL = l
}

// SetDebug sets a custom logger for DEBUG level logs
func SetDebug(l logging.LoggerInterface) {
	DEBUG = l
}

// SetInfo sets a custom logger for INFO level logs
func SetInfo(l logging.LoggerInterface) {
	INFO = l
}

// SetWarning sets a custom logger for WARNING level logs
func SetWarning(l logging.LoggerInterface) {
	WARNING = l
}

// SetError sets a custom logger for ERROR level logs
func SetError(l logging.LoggerInterface) {
	ERROR = l
}

// SetFatal sets a custom logger for FATAL level logs
func SetFatal(l logging.LoggerInterface) {
	FATAL = l
}
