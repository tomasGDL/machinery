package runner

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RichardKnop/machinery/v2/extend/batchqueue"
	"github.com/RichardKnop/machinery/v2/log"
)

type bufferedMsg struct {
	msg   MessageContext
	valid bool
}

type Runner struct {
	*RunnerConfig

	// recv 消息信道
	recvChan chan MessageContext

	// 阻塞内存消息缓存队列
	//	1. 缓存队列消息，提供消息peek
	//	2. 缓冲重冲突的消息，解决对头阻塞
	blockBuffer    []bufferedMsg
	pendingCompact int

	// 操作之前的数据库批处理器
	preBatcher batchqueue.Batcher

	// preBatcher 和 slots 之间的消息信道
	msgChan chan MessageContext

	// 操作之后的数据库批处理器
	postBatcher batchqueue.Batcher

	// Runner 中正在被处理的消息：
	// 即进入Pre-batcher队列，但是还没有从Post-batcher队列出来的消息是被标记引用计数的
	references   map[string]int
	refFlushChan chan []batchqueue.Identifier

	// 用于调度重试的定时器
	scheduleTimer *time.Timer

	// cc     *collector
	ctx           context.Context
	cancel        context.CancelFunc
	workerWg      sync.WaitGroup
	eventLoopDone chan struct{}

	// metrics Prometheus 指标采集
	metrics *runnerMetrics

	// msgBornTime 记录每条消息进入 Runner 的时间，用于计算端到端耗时
	// key: EntryID, value: born time
	msgBornTime map[string]time.Time

	// workerProcessing 当前正在 worker 中处理的消息数
	workerProcessing int64
}

func NewRunner(config *RunnerConfig) *Runner {
	ctx, cancel := context.WithCancel(context.Background())

	// fill default configurations.
	config.Default()

	runner := &Runner{
		references:  make(map[string]int),
		blockBuffer: make([]bufferedMsg, 0, config.BlockSize),
		msgBornTime: make(map[string]time.Time),

		recvChan:     make(chan MessageContext, DefaultRecvSize),
		msgChan:      make(chan MessageContext, DefaultMsgChanSize),
		refFlushChan: make(chan []batchqueue.Identifier, DefaultReferenceSize),

		RunnerConfig: config,
		ctx:          ctx,
		cancel:       cancel,
		metrics:      newRunnerMetrics(config.Name),
	}
	if config.RecvSize > 0 {
		runner.recvChan = make(chan MessageContext, config.RecvSize)
	}

	// generate previous batcher instance.
	preBatchCfg := &batchqueue.Config{
		Name:                  fmt.Sprintf("%s-PRE-BATCHER", config.Name),
		DoBatchFn:             runner.wrapPreBatchFn(config.PreBatchFn),
		MaxBatching:           config.PreMaxBatching,
		MaxPendingMessages:    config.PreMaxPendingMessages,
		BatchingMaxFlushDelay: config.PreBatchingMaxFlushDelay}
	runner.preBatcher = batchqueue.NewBatcher(ctx, preBatchCfg)

	// generate post batcher instance.
	postBatchCfg := &batchqueue.Config{
		Name:                  fmt.Sprintf("%s-POST-BATCHER", config.Name),
		DoBatchFn:             runner.wrapPostBatchFn(config.PostBatchFn),
		MaxBatching:           config.PostMaxBatching,
		MaxPendingMessages:    config.PostMaxPendingMessages,
		BatchingMaxFlushDelay: config.PostBatchingMaxFlushDelay}
	runner.postBatcher = batchqueue.NewBatcher(ctx, postBatchCfg)

	return runner
}

func (m *Runner) Start() {
	m.runEventLoop()
}

func (m *Runner) Stop() {
	m.cancel()
	m.workerWg.Wait()
	m.preBatcher.Close()
	m.postBatcher.Close()
	if m.eventLoopDone != nil {
		<-m.eventLoopDone
	}
}

func (m *Runner) runEventLoop() {
	// 将所有的槽位都执行起来
	m.workerWg.Add(m.Concurrency)
	for i := 0; i < m.Concurrency; i++ {
		go m.handleEventLoop()
	}

	log.INFO.Printf("%d %s slot start graceful.", m.Concurrency, m.Name)
	m.eventLoopDone = make(chan struct{})
	go func() {
		defer close(m.eventLoopDone)
		m.handleReceiveLoop()
	}()

	// 启动独立 goroutine 每5秒打印指标，不受主循环阻塞影响
	go m.runMetricsTicker()
}

func (m *Runner) handleReceiveLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.ERROR.Printf("An exception occurs in Runner(%s), %v", m.Name, r)
		}
	}()

	for {
		m.trySchedule()

		select {
		case <-m.ctx.Done():
			log.INFO.Printf("%s receive loop stop graceful.", m.Name)
			return
		case msg := <-m.recvChan:
			if len(m.blockBuffer) < m.BlockSize {
				m.blockBuffer = append(m.blockBuffer, bufferedMsg{msg: msg, valid: true})
			}
			m.trySchedule()

		case iders := <-m.refFlushChan:
			m.doPostFlushed(iders)

		case <-m.scheduleTimerC():
			// 退避定时器到期，继续尝试调度
		}
	}
}

func (m *Runner) runMetricsTicker() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.showRunnerMetrics()
		}
	}
}

func (m *Runner) scheduleTimerC() <-chan time.Time {
	if m.scheduleTimer == nil {
		return nil
	}
	return m.scheduleTimer.C
}

func (m *Runner) trySchedule() {
	if m.scheduleTimer != nil {
		if !m.scheduleTimer.Stop() {
			select {
			case <-m.scheduleTimer.C:
			default:
			}
		}
		m.scheduleTimer = nil
	}

	indexes := m.SelectRunables()
	if len(indexes) == 0 {
		return
	}

	sentCount := 0
	for _, index := range indexes {
		if index >= len(m.blockBuffer) || !m.blockBuffer[index].valid {
			continue
		}
		bm := m.blockBuffer[index]
		sent, _ := m.preBatcher.SendAsync(bm.msg.Context(), bm.msg)
		if !sent {
			m.scheduleTimer = time.AfterFunc(DefaultScheduleBackoff, func() {})
			m.metrics.IncScheduleRejected("batcher_full")
			break
		}

		m.blockBuffer[index].valid = false
		m.mark(bm.msg.(batchqueue.Identifier))
		sentCount++
		m.metrics.IncMessagesScheduled()

		// 记录调度等待耗时
		if bornTime, ok := m.msgBornTime[bm.msg.EntryID()]; ok {
			m.metrics.ObserveScheduleWaitDuration(time.Since(bornTime))
		}

		if m.Debug {
			log.INFO.Printf("Send message(%s) to pre-batcher, elapsed: %s", bm.msg.EntryID(), bm.msg.Elapsed())
		}
	}

	m.pendingCompact += sentCount
	if m.pendingCompact > len(m.blockBuffer)/4 {
		m.compactBuffer()
	}
}

func (m *Runner) compactBuffer() {
	if m.pendingCompact == 0 {
		return
	}
	newBuf := make([]bufferedMsg, 0, len(m.blockBuffer))
	for _, bm := range m.blockBuffer {
		if bm.valid {
			newBuf = append(newBuf, bm)
		}
	}
	m.blockBuffer = newBuf
	m.pendingCompact = 0
}

func (m *Runner) Process(ctx context.Context, msgCtx MessageContext) error {
	defer func() {
		if r := recover(); r != nil {
			log.ERROR.Printf("An exception occurs in Runner(%s), %v", m.Name, r)
		}
	}()

	select {
	case <-ctx.Done():
		log.INFO.Printf("push message failed, %v", ctx.Err())
	case m.recvChan <- msgCtx:
		m.metrics.IncMessagesReceived()
		m.msgBornTime[msgCtx.EntryID()] = time.Now()
		if m.Debug {
			log.INFO.Printf("worker consume one message, %v", ctx)
		}
	}
	return nil
}

func (m *Runner) handleEventLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.ERROR.Printf("An exception occurs in Runner(%s), %v", m.Name, r)
		}
	}()
	defer m.workerWg.Done()

	ctx, cancel := context.WithCancel(m.ctx)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			// log.DEBUG.Printf("%s slot stop graceful.", m.Name)
			return
		case msg := <-m.msgChan:
			// 处理消息
			atomic.AddInt64(&m.workerProcessing, 1)
			start := time.Now()
			m.ProcessFn(msg)
			m.metrics.ObserveProcessDuration(time.Since(start))
			m.metrics.IncMessagesProcessed()
			atomic.AddInt64(&m.workerProcessing, -1)

			// 消息处理完成后，发送到 post-batcher
			m.postBatcher.Send(msg.Context(), msg)
		}
	}
}

func (m *Runner) doPostFlushed(iders []batchqueue.Identifier) {
	for _, ider := range iders {
		m.unmark(false, ider)

		// 记录端到端耗时并清理 bornTime
		if bornTime, ok := m.msgBornTime[ider.EntryID()]; ok {
			m.metrics.ObserveEndToEndDuration(time.Since(bornTime))
			delete(m.msgBornTime, ider.EntryID())
		}
		m.metrics.IncMessagesPostFlushed()

		if m.Debug {
			log.INFO.Printf("Remove message(%s) from reference counter.", ider.EntryID())
		}

		// callback Flush handler if exists.
		if m.OnPostFlushFn != nil {
			m.OnPostFlushFn(ider)
		}
	}
}

func (m *Runner) SelectRunables() []int {
	var (
		indexes    = make([]int, 0)
		references = map[string]int{}
	)
	// m.showRunnerMetrics()
	for index, bm := range m.blockBuffer {
		if !bm.valid {
			continue
		}
		ider := bm.msg.(batchqueue.Identifier)

		var ready bool
		references, ready = m.runable(ider, references)
		if ready {
			indexes = append(indexes, index)
		}
	}

	for k, v := range m.references {
		references[k] += v
	}

	return indexes
}

func (m *Runner) runable(ider batchqueue.Identifier, references map[string]int) (map[string]int, bool) {
	defer func() {
		if r := recover(); r != nil {
			log.ERROR.Printf("panic in runable: %v", r)
		}
	}()

	// 同一主体不同时执行约束
	if m.UniqueEntryRunning {
		_, exist := m.references[ider.EntryID()]
		if exist {
			return references, false
		}
		_, exist = references[ider.EntryID()]
		if exist {
			return references, false
		}
	}

	// 检查资源限制约束
	labels := ider.Labels()
	for _, label := range labels {
		resource := Resource(label.Name)
		limit, exist := m.ResourceLimits[resource]
		if !exist {
			// log.DEBUG.Printf("resource limit: resource[%s] constraint not definition.", resource)
			continue
		}

		tempCnt := references[label.Value]
		count := m.references[label.Value]
		if count+tempCnt >= limit {
			return references, false
		}
	}

	for _, label := range labels {
		references[label.Value] += 1
	}
	references[ider.EntryID()] += 1

	return references, true
}

func (m *Runner) mark(iders ...batchqueue.Identifier) {
	if len(iders) == 0 {
		return
	}

	for _, ider := range iders {
		m.references[ider.EntryID()] = 1
		for _, label := range ider.Labels() {
			m.references[label.Value] += 1
		}
	}
}

func (m *Runner) unmark(once bool, iders ...batchqueue.Identifier) {
	if len(iders) == 0 {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.ERROR.Printf("panic in unmark: %v", r)
		}
	}()

	for _, ider := range iders {
		_, exist := m.references[ider.EntryID()]
		if once && !exist {
			continue
		}

		delete(m.references, ider.EntryID())

		for _, label := range ider.Labels() {
			count, exist := m.references[label.Value]
			if exist {
				if count <= 1 {
					delete(m.references, label.Value)
				} else {
					m.references[label.Value] = count - 1
				}
			}
		}
	}
}

func (m *Runner) onPostFlushed(iders []batchqueue.Identifier) {
	if len(iders) > 0 {
		m.refFlushChan <- iders
	}
}

func (r *Runner) wrapPreBatchFn(processFn batchqueue.ProcessFn) batchqueue.ProcessFn {
	return func(msgs []interface{}) ([]batchqueue.Identifier, error) {
		start := time.Now()
		iders, err := processFn(msgs)
		r.metrics.ObservePreBatchDuration(time.Since(start))
		for _, msg := range msgs {
			r.msgChan <- msg.(MessageContext)
		}
		return iders, err
	}
}

func (r *Runner) wrapPostBatchFn(processFn batchqueue.ProcessFn) batchqueue.ProcessFn {
	return func(msgs []interface{}) ([]batchqueue.Identifier, error) {
		start := time.Now()
		iders, err := processFn(msgs)
		r.metrics.ObservePostBatchDuration(time.Since(start))

		flushIders := make([]batchqueue.Identifier, 0, len(msgs))
		for _, msg := range msgs {
			ider := msg.(batchqueue.Identifier)
			flushIders = append(flushIders, ider.Duplicate())
		}
		r.onPostFlushed(flushIders)

		return iders, err
	}
}

const (
	// runner configuration default definitions.

	// DefaultRecvSize 定义接收消息的缓冲通道大小。
	// 该通道仅用于解耦外部生产者与 Runner 内部的事件循环，避免写入阻塞。
	// 由于消息会立即被转移至 blockBuffer，此处无需过大，固定较小值即可。
	// 影响仅为慢启动阶段达到最大吞吐的速率，对稳态性能无影响。
	DefaultRecvSize = 8

	// DefaultBlockSize 定义本地阻塞缓冲队列的最大容量。
	// 该缓冲区用于解决队头阻塞（HOL）问题：当队头消息因资源冲突无法执行时，
	// Runner 可以向后扫描，挑选就绪消息优先调度，从而提升并发效率。
	// 容量需根据业务消息的资源冲突密度设定，与 Concurrency 无强制关联。
	DefaultBlockSize = 256

	// DefaultConcurrency 定义 worker 的最大并发数。
	// 这是 Runner 的核心性能参数，决定了同时处理消息的最大数量。
	// 该值应根据实际业务负载、下游依赖（如数据库连接池）的承载能力设定。
	DefaultConcurrency = 512

	// DefaultMsgChanSize 定义 preBatcher 与 worker 之间的消息通道缓冲大小。
	// 该通道仅用于解耦批处理回调与 worker 消费，避免批次处理完成后阻塞。
	// worker 处理消息需要时间，但通道写入极快，固定小值即可满足需求。
	DefaultMsgChanSize = 16

	// DefaultReferenceSize 定义引用计数刷新通道的缓冲大小。
	// 用于接收 postBatcher 完成后的标识符，触发资源释放。
	DefaultReferenceSize = 128

	// DefaultPreMaxBatching 定义 preBatcher 单批次最大消息数。
	// preBatcher 用于批量执行预处理逻辑（如数据库批操作），
	// 其作用是补充 worker 的消耗，而非消息的主要处理链路。
	// 因此采用小批量快速处理策略，减少中间状态积压。
	DefaultPreMaxBatching = 32

	// DefaultPostMaxBatching 定义 postBatcher 单批次最大消息数。
	// postBatcher 用于批量执行后置处理逻辑（如状态更新、通知发送）。
	// 与 preBatcher 类似，采用小批量策略以降低延迟和内存占用。
	DefaultPostMaxBatching = 64

	// DefaultPreMaxPendingMessages 定义 preBatcher 允许的最大 pending 批次数。
	// 该值直接控制预处理阶段的并发度，防止对下游依赖（如数据库）造成过大压力。
	// 保持较小值（如 2）以确保预处理不会成为系统瓶颈。
	DefaultPreMaxPendingMessages = 2

	// DefaultPostMaxPendingMessages 定义 postBatcher 允许的最大 pending 批次数。
	// 控制后置处理阶段的并发度，避免资源耗尽。
	DefaultPostMaxPendingMessages = 2

	// DefaultPreBatchingMaxFlushDelay 定义 preBatcher 的最大刷新延迟。
	// 即使批次未满，超过该延迟也会强制 flush，确保消息不会长时间等待。
	// 较小的延迟有助于降低端到端处理时延。
	DefaultPreBatchingMaxFlushDelay = 100 * time.Millisecond

	// DefaultPostBatchingMaxFlushDelay 定义 postBatcher 的最大刷新延迟。
	// 控制后置处理的响应速度，避免消息处理完成后长时间未确认。
	DefaultPostBatchingMaxFlushDelay = 200 * time.Millisecond

	// DefaultScheduleBackoff 定义调度失败后的退避重试间隔。
	// 当 preBatcher 满或资源不足导致调度失败时，Runner 会在该间隔后重试。
	// 较小的间隔有助于快速恢复调度，减少消息等待时间。
	DefaultScheduleBackoff = 10 * time.Millisecond
)

type MessageContext interface {
	batchqueue.Identifier

	// AppendLogs append log to context.
	AppendLogs(...string)
	// Elapsed return elapsed milliseconds from context born.
	Elapsed() time.Duration
	// Context returns context.Context
	Context() context.Context
	// IsRetry returns true if retry.
	IsRetry() bool
}

type FlushHandler func(batchqueue.Identifier)

type Processor func(v MessageContext)

type Resource string

func (res Resource) S() string { return string(res) }

type RunnerConfig struct {
	// Debug 标识服务是否以调试模式启动。
	// 开启后会输出详细的调度日志和指标信息，便于排查问题。
	Debug bool

	// Name 定义 Runner 实例的名称，用于日志标识和监控区分。
	Name string

	// BlockSize 定义本地阻塞缓冲队列的最大容量。
	// 该缓冲区是 Runner 的核心组件，用于解决队头阻塞（Head-of-Line Blocking）问题：
	// 当队头消息因资源冲突（如 UniqueEntryRunning 或 ResourceLimits）无法执行时，
	// Runner 可以向后扫描 blockBuffer，挑选不冲突的就绪消息优先调度。
	// 容量需根据业务消息的资源冲突密度设定：
	//   - 冲突密度低（消息资源独立）：较小值即可（如 64）
	//   - 冲突密度高（消息争抢相同资源）：较大值（如 256）以提供足够的"跳过"空间
	// 与 Concurrency 无强制关联，独立配置。
	BlockSize int

	// RecvSize 定义接收消息的缓冲通道大小。
	// 该通道仅用于解耦外部生产者（如 MQ Consumer）与 Runner 内部的事件循环。
	// 消息进入 recvChan 后会立即被转移至 blockBuffer，因此该通道无需过大。
	// 固定较小值（如 8）即可，影响仅为慢启动阶段达到最大吞吐的速率。
	RecvSize int

	// Concurrency 定义 worker 的最大并发数。
	// 这是 Runner 的核心性能参数，决定了同时处理消息的最大数量。
	// 该值应根据以下因素综合设定：
	//   - CPU 核心数（避免过度上下文切换）
	//   - 下游依赖承载能力（如数据库连接池大小）
	//   - 消息处理耗时（长耗时任务需要更低并发以避免资源耗尽）
	// 消息链路中其他节点的容量均围绕此值设计，但 blockBuffer 除外。
	Concurrency int

	// PreMaxBatching 定义 preBatcher 单批次最大消息数。
	// preBatcher 用于批量执行预处理逻辑（如数据库批操作、缓存预热等）。
	// 其作用是补充 worker 的消耗，而非消息的主要处理链路，因此采用小批量策略：
	//   - 较小的批次可以降低端到端延迟
	//   - 快速 flush 有助于减少中间状态积压
	// 建议值：32 或 64，远小于 Concurrency。
	PreMaxBatching int

	// PreMaxPendingMessages 定义 preBatcher 允许的最大 pending 批次数。
	// 该值直接控制预处理阶段的并发度，防止对下游依赖（如数据库）造成过大压力。
	// 保持较小值（如 2）以确保预处理不会成为系统瓶颈，同时避免资源耗尽。
	PreMaxPendingMessages uint

	// PreBatchingMaxFlushDelay 定义 preBatcher 的最大刷新延迟。
	// 即使批次未满，超过该延迟也会强制 flush，确保消息不会长时间等待预处理。
	// 较小的延迟（如 100ms）有助于降低端到端处理时延，但会增加批次数。
	PreBatchingMaxFlushDelay time.Duration

	// PostMaxBatching 定义 postBatcher 单批次最大消息数。
	// postBatcher 用于批量执行后置处理逻辑（如状态更新、结果通知、日志归档等）。
	// 与 preBatcher 类似，采用小批量策略以降低延迟和内存占用。
	// 建议值：64 或 128，根据后置处理的开销调整。
	PostMaxBatching int

	// PostMaxPendingMessages 定义 postBatcher 允许的最大 pending 批次数。
	// 控制后置处理阶段的并发度，避免资源耗尽。
	// 保持较小值（如 2）即可，因为后置处理通常是轻量级操作。
	PostMaxPendingMessages uint

	// PostBatchingMaxFlushDelay 定义 postBatcher 的最大刷新延迟。
	// 控制后置处理的响应速度，避免消息处理完成后长时间未确认。
	// 建议值略大于 preBatcher（如 200ms），因为后置处理通常在 worker 完成后执行。
	PostBatchingMaxFlushDelay time.Duration

	// ProcessFn 定义消息处理函数，是 Runner 的核心业务逻辑入口。
	// 该函数在 worker goroutine 中并发执行，执行耗时直接影响整体吞吐量。
	// 函数执行完成后，消息会自动进入 postBatcher 进行后置处理。
	ProcessFn Processor

	// PreBatchFn 定义预处理批处理函数。
	// 在消息进入 worker 之前执行，用于批量预处理（如数据库查询、权限校验等）。
	// 该函数在 batcher 的内部 goroutine 中执行，不应阻塞过长时间。
	PreBatchFn batchqueue.ProcessFn

	// PostBatchFn 定义后置处理批处理函数。
	// 在消息处理完成后执行，用于批量后置处理（如状态更新、通知发送等）。
	// 该函数在 batcher 的内部 goroutine 中执行，不应阻塞过长时间。
	PostBatchFn batchqueue.ProcessFn

	// OnPostFlushFn 定义后置处理完成后的回调函数。
	// 当 postBatcher 完成一批消息的后置处理并释放引用计数后触发。
	// 可用于精确控制 MQ 的 ack 时机，确保消息处理完成后再确认。
	OnPostFlushFn FlushHandler

	// UniqueEntryRunning 启用同一主体唯一执行约束。
	// 当设置为 true 时，具有相同 EntryID 的消息不会同时执行，
	// 而是排队等待前一个消息处理完成。适用于需要串行执行的业务场景。
	UniqueEntryRunning bool

	// ResourceLimits 定义资源限制约束。
	// 键为资源名称，值为该资源允许的最大并发数。
	// Runner 会确保具有相同资源标签的消息不会超过该限制。
	// 例如：{"db_conn": 10} 表示最多 10 个消息同时使用数据库连接。
	ResourceLimits map[Resource]int
}

func (c *RunnerConfig) Default() {
	if c.Name == "" {
		c.Name = "RUNNER"
	}

	if c.BlockSize == 0 {
		c.BlockSize = DefaultBlockSize
	}

	if c.Concurrency == 0 {
		c.Concurrency = DefaultConcurrency
	}

	if c.PreMaxBatching == 0 {
		c.PreMaxBatching = DefaultPreMaxBatching
	}

	if c.PreMaxPendingMessages == 0 {
		c.PreMaxPendingMessages = DefaultPreMaxPendingMessages
	}

	if c.PreBatchingMaxFlushDelay == 0 {
		c.PreBatchingMaxFlushDelay = DefaultPreBatchingMaxFlushDelay
	}

	if c.PostMaxBatching == 0 {
		c.PostMaxBatching = DefaultPostMaxBatching
	}

	if c.PostMaxPendingMessages == 0 {
		c.PostMaxPendingMessages = DefaultPostMaxPendingMessages
	}

	if c.PostBatchingMaxFlushDelay == 0 {
		c.PostBatchingMaxFlushDelay = DefaultPostBatchingMaxFlushDelay
	}

	onExceptionFn := func(err error) {
		if err != nil {
			log.ERROR.Printf("An exception occurs in Runner[%s].", c.Name)
		}
	}

	if c.ProcessFn == nil {
		c.ProcessFn = DefaultProcessFn
	} else {
		c.ProcessFn = WrapProcessFn(c.ProcessFn, onExceptionFn)
	}

	if c.PreBatchFn == nil {
		c.PreBatchFn = DefaultPreBatchFn
	} else {
		c.PreBatchFn = WrapBatchProcessFn(c.PreBatchFn, onExceptionFn)
	}

	if c.PostBatchFn == nil {
		c.PostBatchFn = DefaultPostBatchFn
	} else {
		c.PostBatchFn = WrapBatchProcessFn(c.PostBatchFn, onExceptionFn)
	}

	if c.OnPostFlushFn == nil {
		c.OnPostFlushFn = DefaultPostFlushFn
	} else {
		c.OnPostFlushFn = WrapFlushHandleFn(c.OnPostFlushFn, onExceptionFn)
	}

	if c.ResourceLimits == nil {
		c.ResourceLimits = map[Resource]int{}
	}
}

func DefaultProcessFn(msgCtx MessageContext) {}

func DefaultPreBatchFn(msgs []interface{}) ([]batchqueue.Identifier, error) {
	return []batchqueue.Identifier{}, nil
}

func DefaultPostBatchFn(msgs []interface{}) ([]batchqueue.Identifier, error) {
	resultIders := []batchqueue.Identifier{}

	for _, msg := range msgs {
		ider, ok := msg.(batchqueue.Identifier)
		if ok {
			resultIders = append(resultIders, ider)
		}
	}

	return resultIders, nil
}

func DefaultPostFlushFn(batchqueue.Identifier) {}

type panicHandler struct {
	onError func(err error)
}

func newPanicHandler(onError func(err error)) *panicHandler {
	return &panicHandler{onError: onError}
}

func (p *panicHandler) handle(name string) {
	if r := recover(); r != nil {
		log.ERROR.Printf("panic in %s: %v", name, r)
		if p.onError != nil {
			p.onError(fmt.Errorf("%v", r))
		}
	}
}

func WrapProcessFn(processor Processor, fn func(err error)) Processor {
	ph := newPanicHandler(fn)
	return func(msgCtx MessageContext) {
		defer ph.handle("process")
		processor(msgCtx)
	}
}

func WrapBatchProcessFn(processor batchqueue.ProcessFn, fn func(err error)) batchqueue.ProcessFn {
	ph := newPanicHandler(fn)
	return func(msgs []interface{}) ([]batchqueue.Identifier, error) {
		defer ph.handle("batch process")
		return processor(msgs)
	}
}

func WrapFlushHandleFn(flushHandler FlushHandler, fn func(err error)) FlushHandler {
	ph := newPanicHandler(fn)
	return func(i batchqueue.Identifier) {
		defer ph.handle("flush handler")
		flushHandler(i)
	}
}

type RunnerMetrics struct {
	BlockBufferSize  int
	RecvChanSize     int
	RefFlushChanSize int
	ReferencesCount  int
	PendingCompact   int
	PreBatcherSize   int64
	PostBatcherSize  int64
}

func (m *Runner) Metrics() RunnerMetrics {
	return RunnerMetrics{
		BlockBufferSize:  len(m.blockBuffer),
		RecvChanSize:     len(m.recvChan),
		RefFlushChanSize: len(m.refFlushChan),
		ReferencesCount:  len(m.references),
		PendingCompact:   m.pendingCompact,
		PreBatcherSize:   m.preBatcher.Size(),
		PostBatcherSize:  m.postBatcher.Size(),
	}
}

func (m *Runner) showRunnerMetrics() {
	// if !m.Debug {
	// 	return
	// }
	m.metrics.updateAllGauges(m)
	log.INFO.Printf("runner[%s] metrics: %s", m.Name, m.metrics.String(m))
}
