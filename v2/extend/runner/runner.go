package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/RichardKnop/machinery/v2/extend/batchqueue"
	"github.com/RichardKnop/machinery/v2/log"
)

type Runner struct {
	*RunnerConfig

	// recv 消息信道
	recvChan chan MessageContext

	// 阻塞内存消息缓存队列
	//	1. 缓存队列消息，提供消息peek
	//	2. 缓冲重冲突的消息，解决对头阻塞
	blockBuffer []MessageContext

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

	// 重试事件channel
	retryChan chan retryMessage

	// 用于接受外部事件通知
	wakeEventChan chan WakeEvent

	// cc     *collector
	ctx    context.Context
	cancel context.CancelFunc
}

func NewRunner(config *RunnerConfig) *Runner {
	ctx, cancel := context.WithCancel(context.Background())

	// fill default configurations.
	config.Default()

	runner := &Runner{
		references:  make(map[string]int),
		blockBuffer: make([]MessageContext, 0, config.BlockSize),

		recvChan:      make(chan MessageContext, DefaultRecvSize),
		msgChan:       make(chan MessageContext, DefaultMsgChanSize),
		refFlushChan:  make(chan []batchqueue.Identifier, DefaultReferenceSize),
		retryChan:     make(chan retryMessage, DefaultMsgChanSize),
		wakeEventChan: make(chan WakeEvent, DefaultWakeChanSize),

		RunnerConfig: config,
		ctx:          ctx,
		cancel:       cancel,
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

func (m *Runner) runEventLoop() {
	// 将所有的槽位都执行起来
	for i := 0; i < m.Concurrency; i++ {
		go m.handleEventLoop()
	}

	log.INFO.Printf("%d %s slot start graceful.", m.Concurrency, m.Name)
	go m.handleReceiveLoop()
}

func (m *Runner) handleReceiveLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.ERROR.Printf("An exception occurs in Runner(%s), %v", m.Name, r)
		}
	}()

	ticker := time.NewTicker(500 * time.Millisecond)
	recvEventChan := make(chan struct{}, 2)
	flushEventChan := make(chan struct{}, 2)
	retryEventChan := make(chan struct{}, 2)
	feedbackEventChan := make(chan struct{}, 2)
	defer ticker.Stop()

	attemptToNotify := func(notifyCh chan<- struct{}) {
		select {
		case notifyCh <- struct{}{}:
		default:
		}
	}

	for {
		select {
		case <-m.ctx.Done():
			log.INFO.Printf("%s receive loop stop graceful.", m.Name)
		case <-ticker.C:
			m.scheduleBlockMessage()
			// 通知尚有消费消息
			attemptToNotify(flushEventChan)
			attemptToNotify(retryEventChan)
			attemptToNotify(recvEventChan)

		case <-recvEventChan:
			emptySize := m.BlockSize - len(m.blockBuffer)
			blockedFull := emptySize == 0
			tryWaitTimes := 5
			addition := 0
			for {
				if emptySize <= 0 {
					break
				}

				if tryWaitTimes == 0 {
					break
				}

				select {
				case msg := <-m.recvChan:
					m.blockBuffer = append(m.blockBuffer, msg)
					emptySize--
					addition++
				default:
					tryWaitTimes--
				}
			}

			if !blockedFull {
				// 当worker处于饥饿时，向生产者反馈
				attemptToNotify(feedbackEventChan)
			}

		case <-retryEventChan:
			retryMsgs := []retryMessage{}

			continuse := true
			for continuse {
				select {
				case msg := <-m.retryChan:
					if msg.msgCtx == nil {
						break
					}
					retryMsgs = append(retryMsgs, msg)
					log.INFO.Printf("Received retry message(%s), the current time has taken %dms to process the task.", msg.msgCtx.EntryID(), msg.msgCtx.Elapsed().Milliseconds())
				default:
					continuse = false
				}

				if !continuse {
					break
				}
			}

			retryEndIndex := 0
			// unmark constraint labels
			msgCtxs := []MessageContext{}
			for index, rmsg := range retryMsgs {
				m.unmark(false, rmsg.previous)
				msgCtxs = append(msgCtxs, rmsg.msgCtx)
				if rmsg.msgCtx.IsRetry() {
					retryEndIndex = index
				}
			}

			// 保持retry执行的原有顺序，避免因为重试引起太多的running状态的任务
			blockBuffer := append(m.blockBuffer[:retryEndIndex], msgCtxs...)
			blockBuffer = append(blockBuffer, m.blockBuffer[retryEndIndex:]...)
			// 将需要重试的消息放到blockBuffer的前端
			m.blockBuffer = blockBuffer

		case <-flushEventChan:
			select {
			default:
			case iders := <-m.refFlushChan:
				m.doPostFlushed(iders)
			}

		case <-feedbackEventChan:
			// NOTE:
			//	1. 在缓冲区消息剩余10个以内，使用 HungerFeedbacker.5 反馈生产者
			//	2. 在缓冲区消息为空后，反馈逻辑为退避算法，避免产生过多的反馈消息，引起消息积压
			curBlockedSize := len(m.blockBuffer)
			m.HungerFeedbacker(m.ctx, curBlockedSize)

		case we := <-m.wakeEventChan:
			eventSet := newStringSet()
			eventSet.Add(string(we))

			continues := true
			for continues {
				select {
				case we := <-m.wakeEventChan:
					eventSet.Add(string(we))
				default:
					continues = false
				}

				if !continues {
					break
				}
			}

			for _, we := range eventSet.Slice() {
				switch WakeEvent(we) {
				default:
					attemptToNotify(flushEventChan)
					attemptToNotify(retryEventChan)
				case WakeFlush:
					attemptToNotify(flushEventChan)
				case WakeRetry:
					attemptToNotify(retryEventChan)
				}
			}
		}
	}
}

func (m *Runner) scheduleBlockMessage() {
	sendMessage := func(msgCtx MessageContext) bool {
		sended, _ := m.preBatcher.SendAsync(msgCtx.Context(), msgCtx)
		if !sended {
			return false
		}

		log.INFO.Printf("Send message(%s) to pre-batcher, the current time has taken %s to process the task.", msgCtx.EntryID(), msgCtx.Elapsed())
		message := fmt.Sprintf("Send message to pre-batcher, the current time has taken %s to process the task.", msgCtx.Elapsed())
		msgCtx.AppendLogs(message)
		return true
	}

	sendedIndexes := []int{}
	indexes := m.SelectRunables()

	if len(indexes) == 0 {
		return
	}

	for _, index := range indexes {
		sended := sendMessage(m.blockBuffer[index])
		if !sended {
			break
		}

		sendedIndexes = append(sendedIndexes, index)
		m.mark(m.blockBuffer[index].(batchqueue.Identifier))
	}

	// restore retry buffer elements.
	m.restoreBlockBuffer(sendedIndexes)
}

func (m *Runner) restoreBlockBuffer(indexes []int) {
	m.blockBuffer = m.restoreBuffer(m.blockBuffer, indexes)
}

func (m *Runner) restoreBuffer(buffer []MessageContext, indexes []int) []MessageContext {
	if len(indexes) == len(buffer) {
		return []MessageContext{}
	}

	selected := newIntSet(indexes...)
	newBuffer := make([]MessageContext, 0, len(buffer)-len(indexes))

	for index, msg := range buffer {
		if selected.Has(index) {
			continue
		}

		newBuffer = append(newBuffer, msg)
	}

	return newBuffer
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
		log.INFO.Printf("worker consume one message, %v", ctx)
	}
	return nil
}

func (m *Runner) handleEventLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.ERROR.Printf("An exception occurs in Runner(%s), %v", m.Name, r)
		}
	}()

	ctx, cancel := context.WithCancel(m.ctx)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			log.INFO.Printf("%s slot stop graceful.", m.Name)
			return
		case msg := <-m.msgChan:
			// 处理消息
			m.ProcessFn(msg)

			previous := msg.Duplicate()
			// 尝试对消息进行重试处理
			if m.RetryUpdateFn(msg) {
				m.wakeEvent(WakeRetry)
				m.retryChan <- retryMessage{msgCtx: msg, previous: previous}
				m.wakeEvent(WakeRetry)
			} else {
				// 如果消息没有进行重试，那么作失败处理
				m.postBatcher.Send(msg.Context(), msg)
			}
		}
	}
}

func (m *Runner) doPostFlushed(iders []batchqueue.Identifier) {
	for _, ider := range iders {
		m.unmark(false, ider)

		log.INFO.Printf("Remove message(%s) from reference counter.", ider.EntryID())

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
	m.showRunnerMetrics()
	for index, msg := range m.blockBuffer {
		ider := msg.(batchqueue.Identifier)

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
			log.INFO.Printf("resource limit: resource[%s] constraint not definition.", resource)
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

func (m *Runner) wakeEvent(ev WakeEvent) {
	select {
	case m.wakeEventChan <- ev:
	default:
	}
}

func (m *Runner) onPostFlushed(iders []batchqueue.Identifier) {
	if len(iders) > 0 {
		m.wakeEvent(WakeFlush)
		m.refFlushChan <- iders
		m.wakeEvent(WakeFlush)
	}
}

func (r *Runner) wrapPreBatchFn(processFn batchqueue.ProcessFn) batchqueue.ProcessFn {
	return func(msgs []interface{}) ([]batchqueue.Identifier, error) {
		iders, err := processFn(msgs)
		for _, msg := range msgs {
			r.msgChan <- msg.(MessageContext)
		}
		return iders, err
	}
}

func (r *Runner) wrapPostBatchFn(processFn batchqueue.ProcessFn) batchqueue.ProcessFn {
	return func(msgs []interface{}) ([]batchqueue.Identifier, error) {
		iders, err := processFn(msgs)

		flushIders := make([]batchqueue.Identifier, 0, len(msgs))
		for _, msg := range msgs {
			ider := msg.(batchqueue.Identifier)
			flushIders = append(flushIders, ider.Duplicate())
		}
		r.onPostFlushed(flushIders)

		return iders, err
	}
}

var testingFeedback = false

func TestingSetup() { testingFeedback = true }

const (
	// runner configuration default definitions.
	DefaultRecvSize                  = 8
	DefaultBlockSize                 = 128
	DefaultConcurrency               = 128
	DefaultMsgChanSize               = 128
	DefaultWakeChanSize              = 128
	DefaultReferenceSize             = 128
	DefaultPreMaxBatching            = 100
	DefaultPostMaxBatching           = 200
	DefaultPreMaxPendingMessages     = 2
	DefaultPostMaxPendingMessages    = 5
	DefaultPreBatchingMaxFlushDelay  = 300 * time.Millisecond
	DefaultPostBatchingMaxFlushDelay = 800 * time.Millisecond
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

type retryMessage struct {
	msgCtx   MessageContext
	previous batchqueue.Identifier
}

type FlushHandler func(batchqueue.Identifier)

// 定义对失败的消息进行更新处理，返回是否继续更新
type RetryUpdater func(MessageContext) bool

type HungerFeedbacker func(ctx context.Context, sequence int)

type Processor func(v MessageContext)

type Resource string

func (res Resource) S() string { return string(res) }

type RunnerConfig struct {
	// Debug 标识服务是否以调试模式启动
	Debug bool

	// 名称
	Name string

	// 本地缓存队列大小，为解决对头阻塞而设计，默认值128
	BlockSize int

	RecvSize int

	// 改密节点最大并发，默认值为128
	Concurrency int

	// BatchingMaxMessages set the maximum number of messages permitted in a batch. (default: 100)
	PreMaxBatching int

	// MaxPendingMessages set the max size of the queue.
	PreMaxPendingMessages uint

	// BatchingMaxFlushDelay set the time period within which the messages sent will be batched (default: 300ms)
	PreBatchingMaxFlushDelay time.Duration

	// BatchingMaxMessages set the maximum number of messages permitted in a batch. (default: 200)
	PostMaxBatching int

	// MaxPendingMessages set the max size of the queue.
	PostMaxPendingMessages uint

	// BatchingMaxFlushDelay set the time period within which the messages sent will be batched (default: 800ms)
	PostBatchingMaxFlushDelay time.Duration

	// 消息处理过程，默认DefaultProcessFn
	ProcessFn Processor

	// ProcessFn执行前，批处理消息回调过程，默认DefaultPreBatchFn
	PreBatchFn batchqueue.ProcessFn

	// ProcessFn执行后，批处理消息回调过程，默认DefaultPostBatchFn
	PostBatchFn batchqueue.ProcessFn

	// 消息处理完成后，需要处理的通知等回调过程，默认DefaultPostFlushFn
	OnPostFlushFn FlushHandler

	// 消息处理失败后
	// 对消息进行处理，重置状态等，返回值决定是否重试，默认DefaultRetryUpdateFn
	RetryUpdateFn RetryUpdater

	// 消费者饥饿反馈回调
	// 当队列中不能消费出消息时，向生产者（调度器）反馈
	HungerFeedbacker HungerFeedbacker

	// 同一主体唯一执行约束
	UniqueEntryRunning bool

	// 资源限制约束
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

	onExeptionFn := func(err error) {
		if err != nil {
			log.ERROR.Printf("An exception ocurrs in Runner[%s].", c.Name)
		}
	}

	if c.ProcessFn == nil {
		c.ProcessFn = DefaultProcessFn
	} else {
		c.ProcessFn = WrapProcessFn(c.ProcessFn, onExeptionFn)
	}

	if c.PreBatchFn == nil {
		c.PreBatchFn = DefaultPreBatchFn
	} else {
		c.PreBatchFn = WrapBatchProcessFn(c.PreBatchFn, onExeptionFn)
	}

	if c.PostBatchFn == nil {
		c.PostBatchFn = DefaultPostBatchFn
	} else {
		c.PostBatchFn = WrapBatchProcessFn(c.PostBatchFn, onExeptionFn)
	}

	if c.OnPostFlushFn == nil {
		c.OnPostFlushFn = DefaultPostFlushFn
	} else {
		c.OnPostFlushFn = WrapFlushHandleFn(c.OnPostFlushFn, onExeptionFn)
	}

	if c.RetryUpdateFn == nil {
		c.RetryUpdateFn = DefaultRetryUpdateFn
	} else {
		c.RetryUpdateFn = WrapRetryUpdateFn(c.RetryUpdateFn, onExeptionFn)
	}

	if c.HungerFeedbacker == nil {
		c.HungerFeedbacker = DefaultHungerFeedbackerFn
	} else {
		c.HungerFeedbacker = WrapHungerFeedbackerFn(c.HungerFeedbacker, onExeptionFn)
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

func DefaultRetryUpdateFn(MessageContext) bool { return false }

func DefaultHungerFeedbackerFn(context.Context, int) {}

func WrapProcessFn(processor Processor, fn func(err error)) Processor {
	return func(msgCtx MessageContext) {
		defer func() {
			if r := recover(); r != nil {
				log.ERROR.Printf("panic in process: %v", r)
				if fn != nil {
					fn(fmt.Errorf("%v", r))
				}
			}
		}()
		processor(msgCtx)
	}
}

func WrapBatchProcessFn(processor batchqueue.ProcessFn, fn func(err error)) batchqueue.ProcessFn {
	return func(msgs []interface{}) ([]batchqueue.Identifier, error) {
		defer func() {
			if r := recover(); r != nil {
				log.ERROR.Printf("panic in batch process: %v", r)
				if fn != nil {
					fn(fmt.Errorf("%v", r))
				}
			}
		}()
		return processor(msgs)
	}
}

func WrapFlushHandleFn(flushHandler FlushHandler, fn func(err error)) FlushHandler {
	return func(i batchqueue.Identifier) {
		defer func() {
			if r := recover(); r != nil {
				log.ERROR.Printf("panic in flush handler: %v", r)
				if fn != nil {
					fn(fmt.Errorf("%v", r))
				}
			}
		}()
		flushHandler(i)
	}
}

func WrapRetryUpdateFn(flushHandler RetryUpdater, fn func(err error)) RetryUpdater {
	return func(msgCtx MessageContext) bool {
		defer func() {
			if r := recover(); r != nil {
				log.ERROR.Printf("panic in retry update: %v", r)
				if fn != nil {
					fn(fmt.Errorf("%v", r))
				}
			}
		}()
		return flushHandler(msgCtx)
	}
}

func WrapHungerFeedbackerFn(feedbacker HungerFeedbacker, fn func(err error)) HungerFeedbacker {
	return func(ctx context.Context, sequence int) {
		if testingFeedback {
			ctx = context.WithValue(ctx, "testing", struct{}{})
		}
		defer func() {
			if r := recover(); r != nil {
				log.ERROR.Printf("panic in feedbacker: %v", r)
				if fn != nil {
					fn(fmt.Errorf("%v", r))
				}
			}
		}()
		feedbacker(ctx, sequence)
	}
}

type WakeEvent string

func (e WakeEvent) String() string {
	return string(e)
}

const (
	WakeAll   WakeEvent = "ALL"
	WakeFlush WakeEvent = "FLUSH"
	WakeRetry WakeEvent = "RETRY"
)

func (m *Runner) showRunnerMetrics() {
	// m.logger.Info("================= Show runner metrics =================")
	// m.logger.Infof("block buffer size: %d", len(m.blockBuffer))
	// m.logger.Infof("recv channel size: %d", len(m.recvChan))
	// m.logger.Infof("flush channel size: %d", len(m.refFlushChan))
	// m.logger.Infof("retry channel size: %d", len(m.retryChan))
	// m.logger.Infof("wake channel size: %d", len(m.wakeEventChan))
	// m.logger.Infof("show references: %+v", m.references)
	// m.logger.Infof("show runner config: %+v", m.RunnerConfig)
}
