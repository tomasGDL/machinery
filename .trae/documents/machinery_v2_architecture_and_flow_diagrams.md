# Machinery v2 架构设计与流程图

## 1. 整体架构设计图

```mermaid
graph TB
    subgraph "客户端应用"
        ClientApp[客户端应用代码]
    end
    
    subgraph "Machinery Server"
        Server[Server]
        Config[配置管理<br/>Config]
        TaskRegistry[任务注册表<br/>sync.Map]
    end
    
    subgraph "任务定义"
        Signature[Signature<br/>任务签名]
        Chain[Chain<br/>任务链]
        Group[Group<br/>任务组]
        Chord[Chord<br/>任务组+回调]
    end
    
    subgraph "结果查询"
        AsyncResult[AsyncResult]
        ChainAsyncResult[ChainAsyncResult]
        ChordAsyncResult[ChordAsyncResult]
    end
    
    subgraph "Broker 层"
        BrokerIF[Broker Interface]
        CommonBroker[Common Broker]
        RedisBroker[Redis Broker<br/>BrokerGR]
    end
    
    subgraph "消息队列/代理"
        Redis[(Redis Server<br/>Lists + Sorted Sets)]
    end
    
    subgraph "Worker 层"
        Worker[Worker]
        TaskProcessor[TaskProcessor Interface]
        ProcessTask[Process Task]
        RetryLogic[重试逻辑<br/>Fibonacci Backoff]
    end
    
    subgraph "Backend 层"
        BackendIF[Backend Interface]
        CommonBackend[Common Backend]
        RedisBackend[Redis Backend]
    end
    
    subgraph "状态存储"
        StateStorage[(Redis Server<br/>Task States + Group Meta)]
    end
    
    subgraph "Lock 层"
        LockIF[Lock Interface]
        RedisLock[Redis Lock]
    end
    
    subgraph "辅助模块"
        Tracing[Tracing<br/>OpenTracing]
        Logging[Logging]
        Retry[Retry Logic]
        Utils[Utils]
    end
    
    ClientApp -->|调用| Server
    Server -->|管理| Config
    Server -->|管理| TaskRegistry
    Server -->|包含| BrokerIF
    Server -->|包含| BackendIF
    Server -->|包含| LockIF
    
    Signature -->|组成| Chain
    Signature -->|组成| Group
    Signature -->|组成| Chord
    
    Server -->|发送任务| Signature
    Server -->|返回结果| AsyncResult
    Server -->|返回结果| ChainAsyncResult
    Server -->|返回结果| ChordAsyncResult
    
    BrokerIF -.实现.-> CommonBroker
    CommonBroker -->|扩展| RedisBroker
    RedisBroker -->|存储| Redis
    
    BackendIF -.实现.-> CommonBackend
    CommonBackend -->|扩展| RedisBackend
    RedisBackend -->|存储| StateStorage
    
    LockIF -.实现.-> RedisLock
    RedisLock -->|使用| Redis
    
    Server -->|创建| Worker
    Worker -->|实现| TaskProcessor
    Worker -->|处理| ProcessTask
    ProcessTask -->|失败时| RetryLogic
    
    RedisBroker -->|消费消息| Worker
    Worker -->|更新状态| BackendIF
    
    Tracing --> Server
    Tracing --> Worker
    Logging --> Server
    Logging --> Worker
    Logging --> RedisBroker
    Logging --> RedisBackend
    Retry --> RedisBroker
    Retry --> Worker
    
    style Server fill:#e1f5fe
    style Worker fill:#f3e5f5
    style RedisBroker fill:#e8f5e9
    style RedisBackend fill:#e8f5e9
    style Redis fill:#fff9c4
    style StateStorage fill:#fff9c4
```

## 2. 核心组件关系图

```mermaid
graph LR
    subgraph "Server 组件"
        S[Server]
        SC[Config]
        SB[Broker]
        SE[Backend]
        SL[Lock]
        SR[Registered Tasks]
    end
    
    S --> SC
    S --> SB
    S --> SE
    S --> SL
    S --> SR
    
    subgraph "Broker 组件"
        BI[Broker Interface]
        CB[Common Broker]
        RB[Redis Broker]
    end
    
    BI -.实现.-> CB
    CB --> RB
    
    subgraph "Backend 组件"
        EI[Backend Interface]
        CE[Common Backend]
        RE[Redis Backend]
    end
    
    EI -.实现.-> CE
    CE --> RE
    
    subgraph "Worker 组件"
        W[Worker]
        TP[TaskProcessor]
        PT[Process]
    end
    
    W --> TP
    TP --> PT
    
    subgraph "Result 组件"
        AR[AsyncResult]
        CAR[ChainAsyncResult]
        CHR[ChordAsyncResult]
    end
    
    S --> AR
    S --> CAR
    S --> CHR
    
    style S fill:#e1f5fe
    style W fill:#f3e5f5
    style RB fill:#e8f5e9
    style RE fill:#e8f5e9
```

## 3. 任务发布流程图

```mermaid
flowchart TD
    A[客户端调用 SendTask] --> B{是否提供 Context?}
    B -->|是| C[SendTaskWithContext]
    B -->|否| D[SendTask]
    D --> C
    
    C --> E[启动 OpenTracing Span]
    E --> F{Signature.UUID 为空?}
    F -->|是| G[生成新 UUID<br/>添加 TaskPrefix]
    F -->|否| H[使用现有 UUID]
    G --> I
    H --> I
    
    I[Backend.SetStatePending<br/>设置初始状态为 PENDING] --> J{设置成功?}
    J -->|否| K[返回错误<br/>Set state pending error]
    J -->|是| L{有 PrePublishHandler?}
    
    L -->|是| M[调用 PrePublishHandler]
    L -->|否| N
    M --> N
    
    N[Broker.Publish<br/>发布任务到队列] --> O{Broker 实现}
    
    O -->|Redis Broker| P{Signature.ETA 是否存在且在未来?}
    P -->|是| Q[存储到 Redis Sorted Set<br/>ZAdd delayed_tasks<br/>Score = ETA.UnixNano]
    P -->|否| R[推入 Redis List<br/>RPush RoutingKey]
    
    Q --> S[返回 AsyncResult]
    R --> S
    
    S --> T[AsyncResult 包含<br/>Signature + Backend 引用]
    T --> U[调用方可通过 AsyncResult.Get<br/>阻塞等待结果]
    
    K --> V[返回错误给调用方]
    
    style A fill:#e1f5fe
    style C fill:#e1f5fe
    style I fill:#fff9c4
    style N fill:#fff9c4
    style Q fill:#e8f5e9
    style R fill:#e8f5e9
    style S fill:#f3e5f5
```

## 4. Group 任务发布流程图

```mermaid
flowchart TD
    A[客户端调用 SendGroup] --> B{是否提供 Context?}
    B -->|是| C[SendGroupWithContext]
    B -->|否| C
    
    C --> D[启动 OpenTracing Span<br/>标注 Group 信息]
    D --> E{Backend 是否配置?}
    E -->|否| F[返回错误<br/>Result backend required]
    E -->|是| G[Backend.InitGroup<br/>初始化组元数据]
    
    G --> H[遍历 Group.Tasks<br/>设置每个任务状态为 PENDING]
    H --> I{所有任务设置成功?}
    I -->|否| J[错误写入 errorsChan]
    I -->|是| K[创建并发池<br/>大小 = sendConcurrency]
    
    K --> L[并发发布任务<br/>goroutine 池控制并发度]
    
    L --> M{发布每个任务}
    M --> N[获取并发槽位]
    N --> O[Broker.Publish 任务]
    O --> P{发布成功?}
    P -->|否| Q[错误写入 errorsChan]
    P -->|是| R[创建 AsyncResult<br/>存入结果数组]
    Q --> S[释放并发槽位]
    R --> S
    
    S --> T{所有任务完成?}
    T -->|否| L
    T -->|是| U{errorsChan 有错误?}
    
    U -->|是| V[返回部分结果 + 错误]
    U -->|否| W[返回所有 AsyncResult]
    
    style A fill:#e1f5fe
    style G fill:#fff9c4
    style L fill:#e8f5e9
    style W fill:#f3e5f5
```

## 5. Chain 任务发布流程图

```mermaid
flowchart TD
    A[客户端调用 SendChain] --> B{是否提供 Context?}
    B -->|是| C[SendChainWithContext]
    B -->|否| D[SendChain]
    D --> C
    
    C --> E[启动 OpenTracing Span<br/>标注 Chain 信息]
    E --> F[发送 Chain.Tasks[0]<br/>第一个任务]
    
    F --> G{第一个任务发布成功?}
    G -->|否| H[返回错误]
    G -->|是| I[创建 ChainAsyncResult<br/>包含所有任务 AsyncResult]
    
    I --> J[[Chain 执行机制]]
    J --> K[任务 1 执行成功]
    K --> L[Worker.taskSucceeded<br/>触发 OnSuccess 回调]
    L --> M[将任务 1 的结果<br/>追加到任务 2 的 Args]
    M --> N[发送任务 2]
    N --> O[任务 2 执行...]
    O --> P[依此类推]
    
    I --> Q[调用方可通过<br/>ChainAsyncResult.Get<br/>等待最后一个任务结果]
    
    style A fill:#e1f5fe
    style F fill:#fff9c4
    style I fill:#f3e5f5
    style L fill:#e8f5e9
    style N fill:#e8f5e9
```

## 6. Chord 任务发布流程图

```mermaid
flowchart TD
    A[客户端调用 SendChord] --> B{是否提供 Context?}
    B -->|是| C[SendChordWithContext]
    B -->|否| D[SendChord]
    D --> C
    
    C --> E[启动 OpenTracing Span<br/>标注 Chord 信息]
    E --> F[SendGroupWithContext<br/>发送所有并行任务]
    
    F --> G{Group 发送成功?}
    G -->|否| H[返回错误]
    G -->|是| I[创建 ChordAsyncResult<br/>包含 Group 结果 + Callback 结果]
    
    I --> J[[Chord 执行机制]]
    J --> K[所有 Group 任务并行执行]
    K --> L[每个任务完成后<br/>检查 GroupCompleted]
    L --> M{所有任务完成?}
    M -->|否| N[返回,等待其他任务]
    M -->|是| O[Backend.TriggerChord<br/>确保只触发一次]
    
    O --> P{应该触发?}
    P -->|否| Q[已被其他任务触发<br/>返回]
    P -->|是| R[获取所有任务状态<br/>GroupTaskStates]
    R --> S[所有任务都成功?]
    S -->|否| T[不触发回调]
    S -->|是| U[将 Group 任务结果<br/>追加到 Callback.Args]
    U --> V[发送 Callback 任务]
    
    I --> W[调用方可通过<br/>ChordAsyncResult.Get<br/>等待 Callback 结果]
    
    style A fill:#e1f5fe
    style F fill:#fff9c4
    style I fill:#f3e5f5
    style K fill:#e8f5e9
    style O fill:#ffe0b2
    style V fill:#c8e6c9
```

## 7. Worker 启动与消费流程图

```mermaid
flowchart TD
    A[Server.NewWorker] --> B[创建 Worker 实例<br/>ConsumerTag + Concurrency + Queue]
    B --> C[Worker.Launch]
    C --> D[Worker.LaunchAsync]
    
    D --> E[获取 Broker 引用]
    E --> F[启动 Broker 消费 goroutine]
    
    F --> G[Broker.StartConsuming<br/>进入消费循环]
    
    G --> H{Broker 连接成功?}
    H -->|否| I{Retry 是否启用?}
    I -->|是| J[调用 RetryFunc<br/>等待后重试]
    J --> G
    I -->|否| K[返回错误<br/>停止 Worker]
    
    H -->|是| L[创建 Deliveries Channel<br/>容量 = Concurrency]
    L --> M[初始化 Worker Pool<br/>容量 = Concurrency]
    M --> N[启动接收 Goroutine<br/>从队列拉取消息]
    
    N --> O{等待}
    O -->|StopChan 信号| P[关闭 Deliveries<br/>退出接收循环]
    O -->|Pool 有空闲| Q[BLPop 从 Redis 获取任务<br/>poll period 可配置]
    
    Q --> R{获取到任务?}
    R -->|否| O
    R -->|是| S[推送到 Deliveries Channel]
    S --> T[放回 Pool]
    T --> O
    
    N --> U[启动 Delayed Tasks Goroutine]
    U --> V[轮询 Redis Sorted Set<br/>获取到期任务]
    V --> W{有到期任务?}
    W -->|否| V
    W -->|是| X[反序列化 Signature]
    X --> Y[Broker.Publish 重新发布]
    Y --> V
    
    L --> Z[启动 consume 主循环]
    Z --> AA{等待 Deliveries 或错误}
    AA -->|Deliveries 消息| AB[从 Pool 获取槽位]
    AB --> AC[启动 goroutine 处理任务]
    AC --> AD[调用 consumeOne]
    AD --> AE[反序列化 Signature]
    AE --> AF{任务已注册?}
    
    AF -->|否| AG{IgnoreWhenTaskNotRegistered?}
    AG -->|是| AH[忽略,返回 nil]
    AG -->|否| AI[重新入队任务]
    
    AF -->|是| AJ[TaskProcessor.Process<br/>处理任务]
    
    AJ --> AK[处理完成?]
    AK -->|是| AL[释放 Pool 槽位]
    AL --> AA
    AK -->|否| AM[发送错误到 errorsChan]
    AM --> AN[释放 Pool 槽位]
    AN --> AA
    
    AA -->|Deliveries 关闭| AO[退出消费循环]
    AA -->|错误| AP[返回错误]
    
    AO --> AQ[ProcessingWG.Wait<br/>等待所有任务处理完成]
    AQ --> AR[返回 nil,退出]
    
    D --> AS[启动信号处理 Goroutine]
    AS --> AT[监听 SIGINT/SIGTERM]
    AT --> AU{收到信号}
    AU -->|第一次| AV[Worker.Quit<br/>优雅退出]
    AV --> AW[发送 ErrWorkerQuitGracefully]
    AU -->|第二次| AX[发送 ErrWorkerQuitAbruptly]
    
    style A fill:#e1f5fe
    style G fill:#e8f5e9
    style N fill:#e8f5e9
    style U fill:#e8f5e9
    style AJ fill:#fff9c4
    style Z fill:#f3e5f5
```

## 8. 任务处理详细流程图 (Worker.Process)

```mermaid
flowchart TD
    A[Worker.Process<br/>接收 Signature] --> B{任务已注册?}
    B -->|否| C[返回 nil<br/>不重试]
    B -->|是| D[获取任务函数]
    
    D --> E[Backend.SetStateReceived<br/>状态: RECEIVED]
    E --> F{设置成功?}
    F -->|否| G[返回错误]
    F -->|是| H[tasks.NewWithSignature<br/>准备任务执行]
    
    H --> I{准备成功?}
    I -->|否| J[taskFailed<br/>状态: FAILURE]
    J --> K[返回错误]
    
    I -->|是| L[Tracing.StartSpanFromHeaders<br/>创建/提取 Span]
    L --> M[Backend.SetStateStarted<br/>状态: STARTED]
    
    M --> N{设置成功?}
    N -->|否| O[返回错误]
    N -->|是| P{有 PreTaskHandler?}
    
    P -->|是| Q[调用 PreTaskHandler]
    P -->|否| R
    Q --> R
    
    R[注册 PostTaskHandler<br/>defer 调用] --> S[task.Call<br/>执行任务函数]
    
    S --> T{执行结果}
    T -->|成功| U[taskSucceeded]
    T -->|ErrRetryTaskLater| V[retryTaskIn<br/>指定时间后重试]
    T -->|其他错误| W{RetryCount > 0?}
    
    W -->|是| X[taskRetry<br/>默认重试逻辑]
    W -->|否| J
    
    U --> Y[Backend.SetStateSuccess<br/>状态: SUCCESS<br/>存储 Results]
    Y --> Z[记录成功日志]
    Z --> AA{有 OnSuccess 回调?}
    AA -->|是| AB[遍历 OnSuccess<br/>追加结果到 Args<br/>发送每个回调任务]
    AA -->|否| AC{GroupUUID 为空?}
    
    AB --> AC
    AC -->|是| AD[返回,任务处理完成]
    AC -->|否| AE{ChordCallback 为空?}
    AE -->|是| AD
    AE -->|否| AF[Backend.GroupCompleted<br/>检查组是否完成]
    
    AF --> AG{组完成?}
    AG -->|否| AD
    AG -->|是| AH[Backend.TriggerChord<br/>触发 Chord]
    
    AH --> AI{应该触发?}
    AI -->|否| AD
    AI -->|是| AJ[Backend.GroupTaskStates<br/>获取所有任务状态]
    AJ --> AK{所有任务成功?}
    AK -->|否| AD
    AK -->|是| AL[追加 Group 结果<br/>到 ChordCallback.Args]
    AL --> AM[发送 ChordCallback 任务]
    AM --> AD
    
    X --> AN[Backend.SetStateRetry<br/>状态: RETRY]
    AN --> AO[RetryCount--]
    AO --> AP[计算新 RetryTimeout<br/>FibonacciNext]
    AP --> AQ[设置 ETA = now + RetryTimeout]
    AQ --> AR[SendTask 重新发送]
    AR --> AD
    
    V --> AS[Backend.SetStateRetry]
    AS --> AT[设置 ETA = now + retryIn]
    AT --> AU[SendTask 重新发送]
    AU --> AD
    
    J --> AV[Backend.SetStateFailure<br/>状态: FAILURE<br/>存储 Error]
    AV --> AW{有 ErrorHandler?}
    AW -->|是| AX[调用 ErrorHandler]
    AW -->|否| AY[记录错误日志]
    AX --> AZ{有 OnError 回调?}
    AY --> AZ
    
    AZ -->|是| BA[遍历 OnError<br/>追加错误信息到 Args<br/>发送每个回调任务]
    AZ -->|否| BB{StopTaskDeletionOnError?}
    BA --> BB
    BB -->|是| BC[返回 ErrStopTaskDeletion]
    BB -->|否| BD[返回 nil]
    
    style A fill:#e1f5fe
    style E fill:#fff9c4
    style S fill:#e8f5e9
    style U fill:#c8e6c9
    style J fill:#ffcdd2
    style X fill:#ffe0b2
    style V fill:#ffe0b2
    style AH fill:#f3e5f5
    style AD fill:#b2ebf2
```

## 9. 状态流转图

```mermaid
stateDiagram-v2
    [*] --> PENDING: 任务创建<br/>SetStatePending
    
    PENDING --> RECEIVED: Worker 接收任务<br/>SetStateReceived
    
    RECEIVED --> STARTED: Worker 开始处理<br/>SetStateStarted
    
    STARTED --> SUCCESS: 任务执行成功<br/>SetStateSuccess
    
    STARTED --> FAILURE: 任务执行失败<br/>无重试次数<br/>SetStateFailure
    
    STARTED --> RETRY: 任务失败<br/>有重试次数<br/>SetStateRetry
    
    RETRY --> PENDING: 重新发送任务<br/>SendTask
    
    PENDING --> RECEIVED: Worker 再次接收
    
    FAILURE --> [*]: 任务结束
    
    SUCCESS --> [*]: 任务结束
    
    note right of PENDING
        初始状态
        任务已创建并入库
    end note
    
    note right of RECEIVED
        Worker 已从队列获取
        但尚未开始处理
    end note
    
    note right of STARTED
        Worker 正在执行
        任务函数调用中
    end note
    
    note right of RETRY
        任务失败但会重试
        ETA 设置为未来时间
    end note
    
    note right of SUCCESS
        任务执行成功
        包含返回结果
    end note
    
    note right of FAILURE
        任务执行失败
        包含错误信息
    end note
```

## 10. 结果获取流程图

```mermaid
flowchart TD
    A[调用方获取 AsyncResult] --> B{调用哪种方法?}
    
    B -->|Touch| C[Touch<br/>不等待,立即返回]
    B -->|Get| D[Get<br/>阻塞直到完成]
    B -->|GetWithTimeout| E[GetWithTimeout<br/>带超时阻塞]
    B -->|GetState| F[GetState<br/>仅获取状态]
    
    C --> G[获取当前状态]
    G --> H{状态类型}
    H -->|SUCCESS| I[ReflectTaskResults<br/>返回结果值]
    H -->|FAILURE| J[返回错误<br/>taskState.Error]
    H -->|其他| K[返回 nil, nil<br/>任务仍在执行]
    
    D --> L[循环调用 Touch]
    L --> M{Touch 返回结果?}
    M -->|否| N[Sleep sleepDuration]
    N --> L
    M -->|是| O[返回结果或错误]
    
    E --> P[创建超时定时器]
    P --> Q[循环调用 Touch]
    Q --> R{超时?}
    R -->|是| S[返回 ErrTimeoutReached]
    R -->|否| T{Touch 返回结果?}
    T -->|否| U[Sleep sleepDuration]
    U --> Q
    T -->|是| O
    
    F --> V{状态已完成?}
    V -->|是| W[返回缓存状态]
    V -->|否| X[Backend.GetState<br/>从存储获取最新状态]
    X --> Y[更新缓存状态]
    Y --> Z[返回状态]
    
    A1[ChainAsyncResult.Get] --> A2[遍历所有 AsyncResult]
    A2 --> A3[调用 AsyncResult.Get<br/>阻塞等待每个任务]
    A3 --> A4{任何任务失败?}
    A4 -->|是| A5[立即返回错误]
    A4 -->|否| A6[返回最后一个任务结果]
    
    A7[ChordAsyncResult.Get] --> A8[遍历所有 Group AsyncResult]
    A8 --> A9[等待所有 Group 任务完成]
    A9 --> A10[调用 ChordCallback.AsyncResult.Get]
    A10 --> A11[返回 Callback 结果]
    
    style A fill:#e1f5fe
    style C fill:#fff9c4
    style D fill:#e8f5e9
    style E fill:#e8f5e9
    style I fill:#c8e6c9
    style J fill:#ffcdd2
    style K fill:#f0f4c3
    style O fill:#b2ebf2
```

## 11. 定时任务注册与执行流程

```mermaid
flowchart TD
    A[RegisterPeriodicTask] --> B[解析 Cron 表达式 spec]
    B --> C{解析成功?}
    C -->|否| D[返回错误]
    C -->|是| E[创建定时函数 f]
    
    E --> F[创建闭包函数 f]
    F --> G[Scheduler.AddFunc<br/>注册到 cron 调度器]
    
    H[定时触发时] --> I[执行闭包函数 f]
    I --> J[计算下次执行时间<br/>schedule.Next]
    J --> K[生成 Lock Key<br/>GetLockName]
    K --> L[Lock.LockWithRetries<br/>尝试获取分布式锁]
    
    L --> M{获取锁成功?}
    M -->|否| N[返回,不执行<br/>其他实例正在执行]
    M -->|是| O[CopySignature<br/>复制任务签名]
    O --> P[Server.SendTask<br/>发送任务]
    P --> Q{发送成功?}
    Q -->|否| R[记录错误日志]
    Q -->|是| S[任务执行完成]
    
    N --> T[等待下次触发]
    R --> T
    S --> T
    
    U[Server.Stop] --> V[Scheduler.Stop<br/>停止所有定时任务]
    
    style A fill:#e1f5fe
    style E fill:#fff9c4
    style I fill:#e8f5e9
    style L fill:#ffe0b2
    style M fill:#f3e5f5
    style P fill:#c8e6c9
    style T fill:#b2ebf2
```

## 12. Redis Broker 消费流程图

```mermaid
flowchart TD
    A[BrokerGR.StartConsuming] --> B[Ping Redis 服务器]
    B --> C{Ping 成功?}
    C -->|否| D{Retry 启用?}
    D -->|是| E[RetryFunc 等待]
    E --> B
    D -->|否| F[返回 ErrConsumerStopped]
    
    C -->|是| G[创建 Deliveries Channel<br/>容量 = Concurrency]
    G --> H[初始化 Worker Pool<br/>容量 = Concurrency]
    H --> I[启动接收 Goroutine]
    H --> J[启动延迟任务 Goroutine]
    
    I --> K{等待}
    K -->|StopChan| L[关闭 Deliveries<br/>退出]
    K -->|Pool 可用| M[nextTask<br/>BLPop 获取任务]
    
    M --> N{获取到任务?}
    N -->|否| K
    N -->|是| O[推送到 Deliveries]
    O --> P[放回 Pool]
    P --> K
    
    J --> Q[循环检查延迟任务]
    Q --> R[nextDelayedTask<br/>ZRevRangeByScore 获取到期任务]
    R --> S{获取到任务?}
    S -->|否| Q
    S -->|是| T[反序列化 Signature]
    T --> U[Publish 重新发布<br/>到正常队列]
    U --> Q
    
    G --> V[consume 主循环]
    V --> W{等待 Deliveries}
    W -->|关闭| X[返回 nil]
    W -->|消息| Y[从 Pool 获取槽位]
    Y --> Z[启动 goroutine consumeOne]
    Z --> AA[反序列化 Signature]
    AA --> AB{任务已注册?}
    AB -->|否| AC{IgnoreWhenTaskNotRegistered?}
    AC -->|是| AD[返回 nil]
    AC -->|否| AE[RPush 重新入队]
    
    AB -->|是| AF[TaskProcessor.Process]
    AF --> AG{处理结果}
    AG -->|错误| AH[发送错误到 errorsChan]
    AG -->|成功| AI[处理完成]
    AH --> AJ[释放 Pool 槽位]
    AI --> AJ
    AD --> AJ
    AE --> AJ
    AJ --> W
    
    style A fill:#e1f5fe
    style B fill:#fff9c4
    style I fill:#e8f5e9
    style J fill:#e8f5e9
    style M fill:#f3e5f5
    style R fill:#f3e5f5
    style V fill:#e1f5fe
    style AF fill:#ffe0b2
```

## 13. Redis 延迟任务处理流程

```mermaid
flowchart TD
    A[BrokerGR.Publish] --> B{Signature.ETA 存在且在未来?}
    B -->|否| C[RPush 到正常队列]
    B -->|是| D[计算 Score = ETA.UnixNano]
    D --> E[ZAdd 到 Sorted Set<br/>Key: delayed_tasks<br/>Score: timestamp<br/>Member: JSON Signature]
    
    F[Delayed Tasks Goroutine] --> G[循环轮询]
    G --> H[time.Sleep<br/>DelayedTasksPollPeriod<br/>默认 500ms]
    H --> I[nextDelayedTask]
    
    I --> J[Watch delayed_tasks key]
    J --> K[ZRANGEBYSCORE 0 到 now<br/>获取到期任务]
    K --> L{有到期任务?}
    L -->|否| G
    L -->|是| M[WATCH/MULTI/EXEC 事务]
    M --> N[ZRem 删除任务]
    N --> O[返回任务 JSON]
    
    O --> P[反序列化 Signature]
    P --> Q[Publish 重新发布]
    Q --> R{ETA 是否仍有效?}
    R -->|是| E
    R -->|否| C
    
    C --> S[任务立即可用]
    E --> T[任务延迟存储]
    
    style A fill:#e1f5fe
    style D fill:#fff9c4
    style E fill:#e8f5e9
    style C fill:#c8e6c9
    style F fill:#f3e5f5
    style I fill:#ffe0b2
    style Q fill:#e8f5e9
```

## 14. 任务重试机制流程

```mermaid
flowchart TD
    A[任务执行失败] --> B{错误类型判断}
    
    B -->|ErrRetryTaskLater| C[retryTaskIn]
    B -->|其他错误| D{RetryCount > 0?}
    
    C --> E[Backend.SetStateRetry<br/>状态: RETRY]
    E --> F[设置 ETA = now + retryIn]
    F --> G[记录日志<br/>重试时间]
    G --> H[SendTask 重新发送]
    
    D -->|否| I[taskFailed<br/>Backend.SetStateFailure<br/>状态: FAILURE]
    D -->|是| J[taskRetry]
    
    J --> K[Backend.SetStateRetry<br/>状态: RETRY]
    K --> L[RetryCount--]
    L --> M[RetryTimeout = FibonacciNext<br/>指数退避]
    M --> N[设置 ETA = now + RetryTimeout]
    N --> O[记录日志<br/>重试秒数]
    O --> P[SendTask 重新发送]
    
    H --> Q[任务重新进入队列<br/>等待 Worker 消费]
    I --> R[任务彻底失败<br/>不再重试]
    P --> Q
    
    Q --> S[Worker 再次消费]
    S --> T[任务重新执行]
    
    style A fill:#e1f5fe
    style C fill:#ffe0b2
    style J fill:#ffe0b2
    style E fill:#fff9c4
    style K fill:#fff9c4
    style I fill:#ffcdd2
    style H fill:#e8f5e9
    style P fill:#e8f5e9
```

## 15. 数据流完整链路图

```mermaid
graph TB
    subgraph "生产者端"
        P1[业务代码] -->|1.创建 Signature| P2[Signature]
        P2 -->|2.封装| P3[Chain/Group/Chord]
    end
    
    subgraph "Server 处理"
        S1[Server.SendTask]
        S2[UUID 生成]
        S3[Backend.SetStatePending]
        S4[Broker.Publish]
    end
    
    P2 --> S1
    P3 --> S1
    S1 --> S2
    S2 --> S3
    S3 --> S4
    
    subgraph "消息队列 (Redis)"
        M1[正常队列<br/>List: RPUSH/BLPOP]
        M2[延迟队列<br/>Sorted Set: ZAdd/ZRange]
    end
    
    S4 -->|4.存储| M1
    S4 -->|4.存储| M2
    M2 -->|到期| M1
    
    subgraph "Worker 端"
        W1[Broker.StartConsuming]
        W2[BLPop 获取消息]
        W3[反序列化 Signature]
        W4[Worker.Process]
        W5[Task.Call 执行任务]
        W6{任务结果}
        W7[Success/Failure/Retry]
    end
    
    W1 --> W2
    W2 --> W3
    W3 --> W4
    W4 --> W5
    W5 --> W6
    W6 --> W7
    
    M1 --> W2
    
    subgraph "状态存储 (Redis)"
        B1[Task States<br/>Hash: taskUUID -> state]
        B2[Group Meta<br/>Hash: groupUUID -> meta]
    end
    
    S3 -->|PENDING 状态| B1
    W4 -->|RECEIVED/STARTED| B1
    W7 -->|SUCCESS/FAILURE/RETRY| B1
    W7 -->|Group 完成检查| B2
    W7 -->|Chord 触发| B2
    
    subgraph "结果返回"
        R1[AsyncResult]
        R2[轮询 Backend.GetState]
        R3[返回结果给调用方]
    end
    
    W7 --> R2
    R2 --> R1
    R1 --> R3
    
    S1 --> R1
    
    style P1 fill:#e1f5fe
    style S1 fill:#fff9c4
    style M1 fill:#e8f5e9
    style M2 fill:#e8f5e9
    style W4 fill:#f3e5f5
    style W5 fill:#f3e5f5
    style B1 fill:#ffe0b2
    style B2 fill:#ffe0b2
    style R3 fill:#c8e6c9
```

## 16. Worker 并发处理模型

```mermaid
flowchart TD
    A[Broker.StartConsuming<br/>concurrency=N] --> B[创建 Worker Pool<br/>容量 = N]
    B --> C[初始化 N 个槽位]
    C --> D[启动 Deliveries Channel<br/>容量 = N]
    
    D --> E[接收 Goroutine]
    E --> F{等待槽位可用}
    F --> G[BLPop 获取任务]
    G --> H[推送到 Deliveries]
    H --> I[释放槽位]
    I --> F
    
    D --> J[Consume 主循环]
    J --> K{接收 Deliveries}
    K -->|消息| L[获取槽位]
    L --> M[启动 Goroutine]
    M --> N[consumeOne]
    N --> O[TaskProcessor.Process]
    O --> P{处理结果}
    P -->|成功| Q[处理完成]
    P -->|错误| R[发送错误]
    Q --> S[释放槽位]
    R --> S
    S --> K
    
    B --> T[Delayed Tasks Goroutine]
    T --> U[轮询延迟任务]
    U --> V[Publish 到正常队列]
    V --> U
    
    style A fill:#e1f5fe
    style B fill:#fff9c4
    style E fill:#e8f5e9
    style J fill:#f3e5f5
    style N fill:#ffe0b2
    style O fill:#c8e6c9
```

## 17. 接口依赖关系图

```mermaid
classDiagram
    class Server {
        +config *Config
        +registeredTasks *sync.Map
        +broker Broker
        +backend Backend
        +lock Lock
        +scheduler *cron.Cron
        +NewServer()
        +SendTask() AsyncResult
        +SendGroup() []AsyncResult
        +SendChain() ChainAsyncResult
        +SendChord() ChordAsyncResult
        +RegisterTask()
        +RegisterPeriodicTask()
        +NewWorker() *Worker
    }
    
    class Broker {
        <<interface>>
        +GetConfig() *Config
        +SetRegisteredTaskNames()
        +IsTaskRegistered() bool
        +StartConsuming() bool, error
        +StopConsuming()
        +Publish() error
        +GetPendingTasks() []Signature
        +GetDelayedTasks() []Signature
        +AdjustRoutingKey()
    }
    
    class Backend {
        <<interface>>
        +InitGroup() error
        +GroupCompleted() bool, error
        +GroupTaskStates() []TaskState, error
        +TriggerChord() bool, error
        +SetStatePending() error
        +SetStateReceived() error
        +SetStateStarted() error
        +SetStateRetry() error
        +SetStateSuccess() error
        +SetStateFailure() error
        +GetState() TaskState, error
        +PurgeState() error
        +PurgeGroupMeta() error
    }
    
    class Lock {
        <<interface>>
        +LockWithRetries() error
        +Lock() error
    }
    
    class TaskProcessor {
        <<interface>>
        +Process() error
        +CustomQueue() string
        +PreConsumeHandler() bool
    }
    
    class Worker {
        +server *Server
        +ConsumerTag string
        +Concurrency int
        +Queue string
        +Launch() error
        +Process() error
        +Quit()
    }
    
    class Signature {
        +UUID string
        +Name string
        +RoutingKey string
        +ETA *time.Time
        +GroupUUID string
        +GroupTaskCount int
        +Args []Arg
        +Headers Headers
        +OnSuccess []Signature
        +OnError []Signature
        +ChordCallback *Signature
    }
    
    class TaskState {
        +TaskUUID string
        +TaskName string
        +State string
        +Results []TaskResult
        +Error string
        +CreatedAt time.Time
        +TTL int64
    }
    
    class Chain {
        +Tasks []Signature
    }
    
    class Group {
        +GroupUUID string
        +Tasks []Signature
    }
    
    class Chord {
        +Group *Group
        +Callback *Signature
    }
    
    Server --> Broker : 依赖
    Server --> Backend : 依赖
    Server --> Lock : 依赖
    Server --> Worker : 创建
    Worker --> TaskProcessor : 实现
    Worker --> Server : 引用
    Server --> Signature : 处理
    Server --> Chain : 处理
    Server --> Group : 处理
    Server --> Chord : 处理
    Backend --> TaskState : 管理
    Broker --> Signature : 传输
    
    style Server fill:#e1f5fe
    style Worker fill:#f3e5f5
    style Broker fill:#e8f5e9
    style Backend fill:#e8f5e9
    style Lock fill:#ffe0b2
```

## 18. 组件交互时序图

```mermaid
sequenceDiagram
    participant Client as 客户端
    participant Server as Server
    participant Broker as Broker
    participant Redis as Redis
    participant Worker as Worker
    participant Backend as Backend
    
    Note over Client,Backend: 任务发布阶段
    Client->>Server: SendTask(Signature)
    Server->>Server: 生成 UUID
    Server->>Backend: SetStatePending(Signature)
    Backend->>Redis: 存储 PENDING 状态
    Server->>Broker: Publish(ctx, Signature)
    Broker->>Redis: RPUSH queue signature
    Broker-->>Server: nil
    Server-->>Client: AsyncResult
    
    Note over Client,Backend: 任务消费阶段
    Worker->>Redis: BLPOP queue (阻塞等待)
    Redis-->>Worker: 返回 signature
    Worker->>Worker: 反序列化 Signature
    Worker->>Worker: 检查任务是否注册
    Worker->>Backend: SetStateReceived(Signature)
    Backend->>Redis: 更新 RECEIVED 状态
    Worker->>Backend: SetStateStarted(Signature)
    Backend->>Redis: 更新 STARTED 状态
    Worker->>Worker: Task.Call()
    
    Note over Client,Backend: 任务完成阶段
    alt 任务成功
        Worker->>Backend: SetStateSuccess(Signature, results)
        Backend->>Redis: 更新 SUCCESS 状态 + 结果
        Worker->>Worker: 触发 OnSuccess 回调
    else 任务失败
        Worker->>Backend: SetStateFailure(Signature, error)
        Backend->>Redis: 更新 FAILURE 状态 + 错误
        Worker->>Worker: 触发 OnError 回调
    end
    
    Note over Client,Backend: 结果获取阶段
    Client->>Server: AsyncResult.Get()
    Server->>Backend: GetState(taskUUID)
    Backend->>Redis: 获取状态
    Redis-->>Backend: 返回状态
    Backend-->>Server: TaskState
    Server-->>Client: 结果/状态
```

## 19. 周期性任务完整生命周期

```mermaid
flowchart TD
    A[Server.RegisterPeriodicTask] --> B[解析 Cron 表达式]
    B --> C{解析成功?}
    C -->|否| D[返回错误]
    C -->|是| E[创建闭包函数]
    
    E --> F[Scheduler.AddFunc]
    F --> G[[等待定时触发]]
    
    H[定时触发] --> I[计算下次执行时间]
    I --> J[生成 Lock Key<br/>name + spec]
    J --> K[Lock.LockWithRetries<br/>分布式锁]
    
    K --> L{获取锁?}
    L -->|否| M[跳过执行<br/>其他实例在处理]
    L -->|是| N[CopySignature<br/>避免并发问题]
    
    N --> O[Server.SendTask]
    O --> P[任务进入队列]
    P --> Q[Worker 消费]
    Q --> R[任务执行]
    
    R --> S{执行结果}
    S -->|成功| T[记录日志]
    S -->|失败| U[记录错误日志]
    
    M --> V[等待下次触发]
    T --> V
    U --> V
    
    V --> G
    
    W[Server.Stop] --> X[Scheduler.Stop]
    X --> Y[所有定时任务停止]
    
    style A fill:#e1f5fe
    style G fill:#f3e5f5
    style H fill:#e8f5e9
    style K fill:#ffe0b2
    style O fill:#c8e6c9
    style V fill:#b2ebf2
    style W fill:#ffcdd2
```

## 20. 错误处理与恢复机制

```mermaid
flowchart TD
    A[错误发生] --> B{错误类型}
    
    B -->|Broker 连接错误| C[Broker.StartConsuming 返回 retry=true]
    B -->|任务执行错误| D[Worker.Process 处理]
    B -->|Backend 错误| E[记录错误并返回]
    B -->|信号中断| F[Worker 信号处理]
    
    C --> G{Retry 启用?}
    G -->|是| H[RetryFunc 等待]
    H --> I[重新调用 StartConsuming]
    G -->|否| J[停止 Worker]
    
    D --> K{错误类型}
    K -->|ErrRetryTaskLater| L[指定时间后重试]
    K -->|其他错误| M{RetryCount > 0?}
    M -->|是| N[Fibonacci 退避重试]
    M -->|否| O[标记 FAILURE]
    
    L --> P[SetStateRetry]
    P --> Q[设置 ETA]
    Q --> R[SendTask 重新入队]
    
    N --> S[SetStateRetry]
    S --> T[RetryCount--]
    T --> U[计算新 RetryTimeout]
    U --> V[设置 ETA]
    V --> R
    
    O --> W[SetStateFailure]
    W --> X[触发 OnError 回调]
    X --> Y[记录错误日志]
    
    F --> Z{收到几次信号?}
    Z -->|第一次| AA[Worker.Quit<br/>优雅退出<br/>等待任务完成]
    Z -->|第二次| AB[立即退出<br/>可能丢失任务]
    
    E --> AC[错误包装返回]
    AC --> AD[调用方处理]
    
    R --> AE[任务重新进入队列]
    AE --> AF[等待 Worker 消费]
    
    style A fill:#e1f5fe
    style C fill:#ffe0b2
    style D fill:#ffe0b2
    style F fill:#ffcdd2
    style I fill:#e8f5e9
    style R fill:#e8f5e9
    style W fill:#ffcdd2
    style AA fill:#c8e6c9
    style AB fill:#ff8a80
```

