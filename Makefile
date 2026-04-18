# 通用变量
GO_CMD := go
GOLANGCI_LINT_CMD := golangci-lint
# GOLINT_CMD := golint

# 目录定义
ROOT_DIR := .
V2_DIR := v2

# 默认工作目录
WORKING_DIR := $(V2_DIR)

# 测试相关
TEST_FLAGS := -v
COVERAGE_OUT := coverage.out
COVERAGE_ALL := coverage-all.out

# 执行命令的通用函数
define run_command
	@cd $(WORKING_DIR) && $(1)
endef

# 执行测试的函数
define run_test
	@cd $(WORKING_DIR) && $(GO_CMD) test $(TEST_FLAGS) ./...
endef

# 执行测试并生成覆盖率的函数
define run_test_with_coverage
	@echo "" > $(COVERAGE_OUT)
	@echo "mode: set" > $(COVERAGE_ALL)
	@cd $(WORKING_DIR) && $(GO_CMD) test $(TEST_FLAGS) -coverprofile=$(COVERAGE_OUT) -covermode=set ./...
	@tail -n +2 $(COVERAGE_OUT) >> $(COVERAGE_ALL)
endef

.PHONY: fmt lint golint test test-with-coverage ci help build install clean tidy download docs release-prep version

# 帮助命令
help:
	@echo "Available targets:"
	@echo "  fmt                - Format code"
	@echo "  lint               - Run linter"
	@echo "  test               - Run tests"
	@echo "  test-with-coverage - Run tests with coverage"
	@echo "  ci                 - Run CI tests"
	@echo "  build              - Build the project"
	@echo "  install            - Install the project"
	@echo "  clean              - Clean the project"
	@echo "  tidy               - Tidy dependencies"
	@echo "  download           - Download dependencies"
	@echo "  docs               - Generate documentation"
	@echo "  version            - Show version information"
	@echo "  release-prep       - Prepare for release"
	@echo ""
	@echo "All commands are executed in $(WORKING_DIR) directory by default."

# 基础命令
fmt:
	$(call run_command, $(GO_CMD) fmt ./...)

lint:
	$(call run_command, $(GOLANGCI_LINT_CMD) run ./...)

test:
	$(call run_test)

test-with-coverage:
	$(call run_test_with_coverage)

ci:
	@echo "CI command requires docker-compose, skipping..."

# 构建相关命令
build:
	@echo "Building..."
	$(call run_command, $(GO_CMD) build ./...)

install:
	@echo "Installing..."
	$(call run_command, $(GO_CMD) install ./...)

# 清理相关命令
clean:
	@echo "Cleaning..."
	$(call run_command, $(GO_CMD) clean ./...)
	@del /f /q $(COVERAGE_OUT) $(COVERAGE_ALL) 2>nul || echo "No coverage files to delete"

# 依赖管理命令
tidy:
	@echo "Tidying dependencies..."
	$(call run_command, $(GO_CMD) mod tidy)

download:
	@echo "Downloading dependencies..."
	$(call run_command, $(GO_CMD) mod download)

# 文档生成命令
docs:
	@echo "Generating documentation..."
	$(call run_command, $(GO_CMD) doc ./...)

# 发布相关命令
release-prep:
	@echo "Preparing for release..."
	@$(MAKE) clean
	@$(MAKE) test

version:
	@echo "Go version:"
	@$(GO_CMD) version
	@echo "Module info:"
	@cd $(WORKING_DIR) && $(GO_CMD) list -m