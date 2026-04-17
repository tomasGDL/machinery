package log_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v2/log"
)

func TestDefaultLogger(t *testing.T) {
	log.INFO.Print("should not panic")
	log.WARNING.Print("should not panic")
	log.ERROR.Print("should not panic")
	log.FATAL.Print("should not panic")
}

// mockLogger 用于测试的 mock logger
type mockLogger struct {
	debugMessages []string
	infoMessages  []string
	warnMessages  []string
	errorMessages []string
	fatalMessages []string
}

func (m *mockLogger) Debugf(format string, args ...interface{}) {
	m.debugMessages = append(m.debugMessages, format)
}

func (m *mockLogger) Infof(format string, args ...interface{}) {
	m.infoMessages = append(m.infoMessages, format)
}

func (m *mockLogger) Warnf(format string, args ...interface{}) {
	m.warnMessages = append(m.warnMessages, format)
}

func (m *mockLogger) Errorf(format string, args ...interface{}) {
	m.errorMessages = append(m.errorMessages, format)
}

func (m *mockLogger) Fatalf(format string, args ...interface{}) {
	m.fatalMessages = append(m.fatalMessages, format)
}

func TestUseLogger(t *testing.T) {
	// 保存默认 logger
	defaultLogger := log.GetLogger()

	// 测试 UseLogger 和 GetLogger
	customLogger := &mockLogger{}
	log.UseLogger(customLogger)

	if log.GetLogger() != customLogger {
		t.Error("UseLogger did not set the logger correctly")
	}

	// 恢复默认 logger
	log.UseLogger(defaultLogger)
}

func TestInnerLogger(t *testing.T) {
	// 测试 innerLogger 的各个方法，确保不会 panic
	logger := log.GetLogger()
	logger.Debugf("debug message: %s", "test")
	logger.Infof("info message: %s", "test")
	logger.Warnf("warn message: %s", "test")
	logger.Errorf("error message: %s", "test")
	// Fatalf 会退出程序，不测试
}

func TestMockLogger(t *testing.T) {
	mock := &mockLogger{}

	mock.Debugf("debug: %s", "test")
	mock.Infof("info: %s", "test")
	mock.Warnf("warn: %s", "test")
	mock.Errorf("error: %s", "test")

	if len(mock.debugMessages) != 1 {
		t.Errorf("expected 1 debug message, got %d", len(mock.debugMessages))
	}
	if len(mock.infoMessages) != 1 {
		t.Errorf("expected 1 info message, got %d", len(mock.infoMessages))
	}
	if len(mock.warnMessages) != 1 {
		t.Errorf("expected 1 warn message, got %d", len(mock.warnMessages))
	}
	if len(mock.errorMessages) != 1 {
		t.Errorf("expected 1 error message, got %d", len(mock.errorMessages))
	}
}
