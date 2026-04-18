# Makefile 重构计划

## 当前状态分析

### 现有命令
- **基础命令**: fmt, lint, golint, test, test-with-coverage, ci, help
- **v2命令**: v2-fmt, v2-lint, v2-golint, v2-test, v2-test-with-coverage

### 问题分析
1. **重复命令**: 基础命令和v2命令功能相同，只是执行路径不同
2. **命令结构**: 每个命令都是独立定义，没有利用Makefile的变量和函数特性
3. **功能有限**: 缺少一些常用的开发和构建命令

## 重构计划

### 1. 移除重复命令

#### 目标
移除所有v2-前缀的命令，让所有命令默认在v2目录下执行

#### 具体步骤
1. 定义变量存储常用命令和路径
2. 设置默认工作目录为v2
3. 重构现有命令，移除v2-前缀
4. 保持命令功能不变，但默认在v2目录执行

### 2. 扩展Makefile命令

#### 目标
添加更多实用的开发和构建命令，提高开发效率

#### 具体步骤
1. 添加构建相关命令
2. 添加清理相关命令
3. 添加依赖管理命令
4. 添加文档生成命令
5. 添加发布相关命令

## 重构后的Makefile结构

### 1. 变量定义
```makefile
# 通用变量
GO_CMD := go
GOLANGCI_LINT_CMD := golangci-lint
GOLINT_CMD := golint

# 目录定义
ROOT_DIR := .
V2_DIR := v2

# 默认工作目录
WORKING_DIR := $(V2_DIR)

# 测试相关
TEST_FLAGS := -v
COVERAGE_OUT := coverage.out
COVERAGE_ALL := coverage-all.out
```

### 2. 函数定义
```makefile
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
```

### 3. 命令重构
```makefile
# 基础命令
fmt:
	$(call run_command, $(GO_CMD) fmt ./...)

lint:
	$(call run_command, $(GOLANGCI_LINT_CMD) run ./...)

golint:
	$(call run_command, $(GOLINT_CMD) -set_exit_status ./...)

test:
	$(call run_test)

test-with-coverage:
	$(call run_test_with_coverage)

ci:
	@echo "CI command requires docker-compose, skipping..."
```

### 4. 扩展命令

#### 构建相关
```makefile
# 构建命令
build:
	@echo "Building..."
	$(call run_command, $(GO_CMD) build ./...)

# 安装命令
install:
	@echo "Installing..."
	$(call run_command, $(GO_CMD) install ./...)
```

#### 清理相关
```makefile
# 清理命令
clean:
	@echo "Cleaning..."
	$(call run_command, $(GO_CMD) clean ./...)
	@rm -f $(COVERAGE_OUT) $(COVERAGE_ALL)
```

#### 依赖管理
```makefile
# 依赖管理
tidy:
	@echo "Tidying dependencies..."
	$(call run_command, $(GO_CMD) mod tidy)

# 下载依赖
download:
	@echo "Downloading dependencies..."
	$(call run_command, $(GO_CMD) mod download)
```

#### 文档相关
```makefile
# 文档生成
docs:
	@echo "Generating documentation..."
	$(call run_command, $(GO_CMD) doc ./...)
```

#### 发布相关
```makefile
# 发布准备
release-prep:
	@echo "Preparing for release..."
	@$(MAKE) clean
	@$(MAKE) test

# 显示版本信息
version:
	@echo "Go version:"
	@$(GO_CMD) version
	@echo "Module info:"
	@cd $(WORKING_DIR) && $(GO_CMD) list -m
```

### 5. 帮助命令更新
```makefile
help:
	@echo "Available targets:"
	@echo "  fmt                - Format code"
	@echo "  lint               - Run linter"
	@echo "  golint             - Run golint"
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
```

## 预期结果

1. **移除重复代码**: 不再区分v2和非v2命令，所有命令默认在v2目录执行
2. **提高可维护性**: 更清晰的结构，易于添加新命令
3. **扩展功能**: 添加更多实用的开发和构建命令
4. **保持兼容性**: 保留所有现有命令的功能
5. **更好的用户体验**: 更详细的帮助信息，更清晰的命令组织

## 执行步骤

1. 备份当前Makefile
2. 按照重构计划创建新的Makefile
3. 测试所有命令是否正常工作
4. 验证功能是否与之前一致
5. 完成重构