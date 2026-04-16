# Machinery v2 代码优化计划

## 目标

1. 为接口添加完善的注释
2. 统一 Redis 连接池管理

---

## 一、为接口添加注释

### 1.1 backends/iface/interfaces.go

需要为每个方法添加详细的 GoDoc 注释：

```go
// Backend - a common interface for all result backends
type Backend interface {
	// InitGroup initializes a group meta data object for tracking group task states
	InitGroup(groupUUID string, taskUUIDs []string) error
	
	// GroupCompleted returns true if all tasks in a group have completed (success or failure)
	GroupCompleted(groupUUID string, groupTaskCount int) (bool, error)
	
	// GroupTaskStates returns the current states of all tasks in a group
	GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error)
	
	// TriggerChord checks if chord should be triggered and marks it as triggered
	// Returns true if chord should be triggered, false if already triggered
	TriggerChord(groupUUID string) (bool, error)
	
	// SetStatePending updates task state to PENDING
	SetStatePending(signature *tasks.Signature) error
	
	// SetStateReceived updates task state to RECEIVED
	SetStateReceived(signature *tasks.Signature) error
	
	// SetStateStarted updates task state to STARTED
	SetStateStarted(signature *tasks.Signature) error
	
	// SetStateRetry updates task state to RETRY
	SetStateRetry(signature *tasks.Signature) error
	
	// SetStateSuccess updates task state to SUCCESS and stores task results
	SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error
	
	// SetStateFailure updates task state to FAILURE and stores error message
	SetStateFailure(signature *tasks.Signature, err string) error
	
	// GetState returns the current state of a task by UUID
	GetState(taskUUID string) (*tasks.TaskState, error)
	
	// PurgeState deletes stored task state
	PurgeState(taskUUID string) error
	
	// PurgeGroupMeta deletes stored group meta data
	PurgeGroupMeta(groupUUID string) error
}
```

### 1.2 brokers/iface/interfaces.go

```go
// Broker - a common interface for all message brokers
type Broker interface {
	// GetConfig returns the configuration object
	GetConfig() *config.Config
	
	// SetRegisteredTaskNames sets the list of registered task names
	SetRegisteredTaskNames(names []string)
	
	// IsTaskRegistered returns true if the task name is registered
	IsTaskRegistered(name string) bool
	
	// StartConsuming enters a loop and waits for incoming messages
	// Returns retry flag and error
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	
	// StopConsuming quits the message consumption loop
	StopConsuming()
	
	// Publish places a new message on the queue
	Publish(ctx context.Context, task *tasks.Signature) error
	
	// GetPendingTasks returns a slice of task signatures waiting in the queue
	GetPendingTasks(queue string) ([]*tasks.Signature, error)
	
	// GetDelayedTasks returns a slice of task signatures scheduled for later execution
	GetDelayedTasks() ([]*tasks.Signature, error)
	
	// AdjustRoutingKey adjusts the routing key for the task signature
	AdjustRoutingKey(s *tasks.Signature)
}

// TaskProcessor - can process a delivered task
type TaskProcessor interface {
	// Process handles the execution of a task
	Process(signature *tasks.Signature) error
	
	// CustomQueue returns the custom queue name for the worker
	CustomQueue() string
	
	// PreConsumeHandler is called before consuming a message
	// Returns false to skip consuming
	PreConsumeHandler() bool
}
```

### 1.3 locks/iface/interfaces.go

```go
// Lock - a common interface for distributed locks
type Lock interface {
	// LockWithRetries attempts to acquire the lock with retry mechanism
	// key: the name of the lock
	// value: the nanosecond timestamp when the lock should be automatically released
	LockWithRetries(key string, value int64) error
	
	// Lock attempts to acquire the lock once
	// key: the name of the lock
	// value: the nanosecond timestamp when the lock should be automatically released
	Lock(key string, value int64) error
}
```

---

## 二、统一 Redis 连接池管理

### 2.1 当前问题分析

当前架构中，Broker、Backend、Lock 各自管理自己的 Redis 连接：

```
Broker (redigo pool)
├── 使用 common.RedisConnector.NewPool()
└── 每个 Broker 实例独立创建 pool

Backend (redigo pool)
├── 使用 common.RedisConnector.NewPool()
└── 每个 Backend 实例独立创建 pool

Lock (go-redis client)
├── 使用 redis.NewUniversalClient()
└── 每个 Lock 实例独立创建 client
```

**问题**:
1. 连接池重复创建，浪费资源
2. 连接数难以控制（3个组件各自维护连接池）
3. 配置分散，难以统一管理

### 2.2 优化方案

#### 方案 A: 共享连接池（推荐）

创建一个统一的 Redis 连接池管理器，供 Broker、Backend、Lock 共享使用。

**设计**:

```go
// common/redis_pool.go

// RedisPoolManager 管理共享的 Redis 连接池
type RedisPoolManager struct {
	pool      *redis.Pool
	redsync   *redsync.Redsync
	client    redis.UniversalClient  // go-redis client for lock
	config    *config.Config
	once      sync.Once
}

// NewRedisPoolManager 创建或返回现有的连接池管理器（单例模式）
func NewRedisPoolManager(cnf *config.Config) *RedisPoolManager {
	// 返回单例实例
}

// GetPool 返回 redigo 连接池（供 Broker/Backend 使用）
func (rpm *RedisPoolManager) GetPool() *redis.Pool

// GetRedsync 返回 redsync 实例（供 Backend 使用）
func (rpm *RedisPoolManager) GetRedsync() *redsync.Redsync

// GetClient 返回 go-redis 客户端（供 Lock 使用）
func (rpm *RedisPoolManager) GetClient() redis.UniversalClient
```

**修改影响**:

1. **Broker** - 改为从 PoolManager 获取 pool
2. **Backend** - 改为从 PoolManager 获取 pool 和 redsync
3. **Lock** - 改为从 PoolManager 获取 client
4. **Server** - 创建 PoolManager 并传递给各组件

**优点**:
- 单一连接池，资源可控
- 配置集中管理
- 减少连接数

**缺点**:
- 改动范围较大
- 需要修改构造函数

#### 方案 B: 依赖注入连接池

在创建 Server 时创建连接池，然后注入到 Broker、Backend、Lock。

**设计**:

```go
// Server 构造函数接收连接池
func NewServer(cnf *config.Config, broker brokersiface.Broker, backend backendsiface.Backend, lock lockiface.Lock, pool *redis.Pool) *Server

// 或者创建 RedisConnection 对象
func NewServer(cnf *config.Config, redisConn *common.RedisConnection) *Server
```

**优点**:
- 更灵活，支持外部传入连接池
- 符合依赖注入原则

**缺点**:
- API 变化较大
- 使用复杂度增加

### 2.3 推荐实现（方案 A 简化版）

**步骤 1**: 创建 RedisPoolManager

```go
// common/redis_pool_manager.go

type RedisPoolManager struct {
	pool    *redis.Pool
	redsync *redsync.Redsync
	client  redis.UniversalClient
	cnf     *config.Config
	once    sync.Once
}

var (
	managerInstance *RedisPoolManager
	managerOnce     sync.Once
)

// GetRedisPoolManager 返回单例的 PoolManager
func GetRedisPoolManager(cnf *config.Config) *RedisPoolManager {
	managerOnce.Do(func() {
		managerInstance = &RedisPoolManager{cnf: cnf}
		managerInstance.init()
	})
	return managerInstance
}

func (rpm *RedisPoolManager) init() {
	// 初始化 pool
	// 初始化 redsync
	// 初始化 go-redis client
}
```

**步骤 2**: 修改 Broker

```go
type Broker struct {
	common.Broker
	poolManager *common.RedisPoolManager
	// ... 其他字段
}

func New(cnf *config.Config, host, password, socketPath string, db int) iface.Broker {
	return &Broker{
		Broker:      common.NewBroker(cnf),
		poolManager: common.GetRedisPoolManager(cnf),
		// ...
	}
}

func (b *Broker) open() redis.Conn {
	return b.poolManager.GetPool().Get()
}
```

**步骤 3**: 修改 Backend

```go
type Backend struct {
	common.Backend
	poolManager *common.RedisPoolManager
	// ... 其他字段
}

func (b *Backend) open() redis.Conn {
	return b.poolManager.GetPool().Get()
}

func (b *Backend) getRedsync() *redsync.Redsync {
	return b.poolManager.GetRedsync()
}
```

**步骤 4**: 修改 Lock

```go
type Lock struct {
	poolManager *common.RedisPoolManager
	retries     int
	interval    time.Duration
}

func (r Lock) Lock(key string, unixTsToExpireNs int64) error {
	client := r.poolManager.GetClient()
	// ... 使用 client
}
```

---

## 三、实施步骤

### 阶段 1: 添加接口注释（低风险）
1. 修改 `backends/iface/interfaces.go`
2. 修改 `brokers/iface/interfaces.go`
3. 修改 `locks/iface/interfaces.go`
4. 运行测试验证

### 阶段 2: 创建 RedisPoolManager（中风险）
1. 创建 `common/redis_pool_manager.go`
2. 实现单例模式和初始化逻辑
3. 运行测试验证

### 阶段 3: 修改 Broker/Backend/Lock（中风险）
1. 修改 `brokers/redis/redis.go`
2. 修改 `backends/redis/redis.go`
3. 修改 `locks/redis/redis.go`
4. 运行测试验证

### 阶段 4: 验证和优化（低风险）
1. 运行完整测试套件
2. 检查连接池配置
3. 性能测试

---

## 四、风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 接口注释改动 | 低 | 仅添加注释，不影响逻辑 |
| 连接池共享 | 中 | 使用单例模式确保一致性 |
| 并发问题 | 中 | 使用 sync.Once 确保线程安全 |
| 配置不兼容 | 低 | 保持现有配置结构 |

---

## 五、预期收益

1. **代码可读性** - 完善的接口注释
2. **资源效率** - 单一连接池，减少连接数
3. **可维护性** - 集中管理 Redis 连接配置
4. **性能** - 减少连接建立开销
