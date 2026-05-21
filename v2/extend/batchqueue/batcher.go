package batchqueue

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type batcherBatchState int

const (
	batcherBatchInit batcherBatchState = iota
	batcherBatchReady
	batcherBatchClosing
	batcherBatchClosed
)

type processState int

const (
	processIdle processState = iota
	processInProgress
)

type Batcher interface {
	Size() int64
	Send(ctx context.Context, msg interface{}) error
	SendAsync(ctx context.Context, msg interface{}) (bool, error)
	Flush(ctx context.Context) error
	AsyncResult(ctx context.Context, id Identifier, timeout time.Duration) (Identifier, error)
	Close()
}

type sendRequest struct {
	ctx context.Context
	msg interface{}
}

type closeRequest struct {
	waitGroup *sync.WaitGroup
}

type flushRequest struct {
	waitGroup *sync.WaitGroup
	err       error
}

type pendingItem struct {
	sync.Mutex
	sequenceID uint64
	batchData  []interface{}
	callback   []CallbackFn
	status     processState
	err        error
}

func (pending *pendingItem) GetSequenceID() uint64 {
	return pending.sequenceID
}

func (pending *pendingItem) Callback() {
	// lock the pending item
	pending.Lock()
	defer pending.Unlock()
	for _, fn := range pending.callback {
		fn(pending.sequenceID, pending.err)
	}
}

func (pending *pendingItem) Release() {
	pending.batchData = nil
}

type CallbackFn func(sequenceID uint64, e error)

type ProcessFn func(msgs []interface{}) ([]Identifier, error)

type FlushFn func(iders []Identifier)

type batcher struct {
	batchBuilder     *BatchBuilder
	batchFlushTicker *time.Ticker

	// Channel where app is posting messages to be published
	eventsChan chan interface{}

	pendingQueue BlockingQueue

	processFn   ProcessFn
	postFlushFn FlushFn
	state       batcherBatchState
	batcherName string
	//	lock      sync.Mutex

	conf *Config

	sendCnt      int64
	processedCnt int64
	spinlock     int32

	ctx context.Context
}

type Config struct {
	Name        string
	DoBatchFn   ProcessFn
	PostFlushFn FlushFn
	// BatchingMaxMessages set the maximum number of messages permitted in a batch. (default: 1000)
	MaxBatching int
	// MaxPendingMessages set the max size of the queue.
	MaxPendingMessages uint
	// BatchingMaxFlushDelay set the time period within which the messages sent will be batched (default: 10ms)
	BatchingMaxFlushDelay time.Duration
}

func (c *Config) GetBatchingMaxFlushDelay() time.Duration {
	if c.BatchingMaxFlushDelay == 0 {
		c.BatchingMaxFlushDelay = defaultBatchingMaxFlushDelay
	}
	return c.BatchingMaxFlushDelay
}

func (c *Config) GetMaxPendingMessages() int {
	if c.MaxPendingMessages == 0 {
		c.MaxPendingMessages = defaultMaxPendingMessages
	}
	return int(c.MaxPendingMessages)
}

func (c *Config) GetMaxBatching() uint {
	if c.MaxBatching == 0 {
		c.MaxBatching = defaultMaxBatching
	}
	return uint(c.MaxBatching)
}

const (
	defaultMaxBatching           = 1000
	defaultMaxPendingMessages    = 5
	defaultBatchingMaxFlushDelay = 10 * time.Millisecond
)

func NewBatcher(ctx context.Context, conf *Config) Batcher {
	batcher := &batcher{
		ctx:              ctx,
		conf:             conf,
		batcherName:      conf.Name,
		processFn:        conf.DoBatchFn,
		postFlushFn:      conf.PostFlushFn,
		state:            batcherBatchInit,
		eventsChan:       make(chan interface{}, 1),
		batchBuilder:     NewBatchBuilder(conf.GetMaxBatching()),
		pendingQueue:     NewBlockingQueue(conf.GetMaxPendingMessages()),
		batchFlushTicker: time.NewTicker(conf.GetBatchingMaxFlushDelay()),
	}
	batcher.state = batcherBatchReady

	go batcher.runEventsLoop()

	return batcher
}

func (p *batcher) runEventsLoop() {
	for {
		select {
		case i := <-p.eventsChan:
			switch v := i.(type) {
			case *sendRequest:
				p.internalSend(v)
			case *flushRequest:
				p.internalFlush(v)
			case *closeRequest:
				p.internalClose(v)
				return
			}

		case <-p.batchFlushTicker.C:
			p.internalFlushCurrentBatch()
		}
	}
}

func (p *batcher) internalSend(request *sendRequest) {
	msg := request.msg

	isFull := p.batchBuilder.Add(msg)
	if isFull {
		// The current batch is full then flush it.
		p.internalFlushCurrentBatch()
	}
	p.sendCnt++
}

func (p *batcher) internalFlushCurrentBatch() {
	batchData, sequenceID := p.batchBuilder.Flush()
	if len(batchData) == 0 {
		return
	}

	item := pendingItem{
		batchData:  batchData,
		sequenceID: sequenceID,
		callback:   []CallbackFn{},
		status:     processInProgress}
	p.pendingQueue.Put(&item)

	go func(item *pendingItem) {
		iders, err := p.processFn(item.batchData)
		if len(iders) > 0 {
			if p.postFlushFn != nil {
				p.postFlushFn(iders)
			}
		}

		atomic.AddInt64(&p.processedCnt, int64(len(item.batchData)))
		p.callbackReceipt(item, err)
	}(&item)
}

func (p *batcher) internalFlush(fr *flushRequest) {
	p.internalFlushCurrentBatch()

	pi, ok := p.pendingQueue.PeekLast().(*pendingItem)
	if !ok {
		fr.waitGroup.Done()
		return
	}

	// lock the pending request while adding requests
	// since the ReceivedSendReceipt func iterates over this list
	pi.Lock()
	pi.callback = append(pi.callback, func(sequenceID uint64, e error) {
		fr.err = e
		fr.waitGroup.Done()
	})
	pi.Unlock()
}

func (p *batcher) internalClose(req *closeRequest) {
	defer req.waitGroup.Done()
	if p.state != batcherBatchReady {
		return
	}

	p.state = batcherBatchClosing

	p.state = batcherBatchClosed
	p.batchFlushTicker.Stop()

	wg := sync.WaitGroup{}
	wg.Add(1)
	fr := &flushRequest{&wg, nil}
	p.internalFlush(fr)
	wg.Wait()
}

func (p *batcher) callbackReceipt(item *pendingItem, err error) {
	item.status = processIdle
	item.err = err
	p.sendCnt -= int64(len(item.batchData))

	for {
		pi, ok := p.pendingQueue.Peek().(*pendingItem)

		if !ok {
			break
		}
		if pi.status == processInProgress {
			break
		}

		// We can remove the item which is done
		p.pendingQueue.Poll()

		// Trigger the callback and release item
		pi.Callback()
		pi.Release()
	}
}

func (p *batcher) Size() int64 {
	return p.sendCnt - atomic.LoadInt64(&p.processedCnt)
}

func (p *batcher) Send(ctx context.Context, msg interface{}) error {
	var err error
	sr := &sendRequest{
		ctx: ctx,
		msg: msg,
	}
	p.internalSend(sr)

	return err
}

func (p *batcher) SendAsync(ctx context.Context, msg interface{}) (bool, error) {
	var err error
	sr := &sendRequest{
		ctx: ctx,
		msg: msg,
	}

	// lock spin lock.
	for atomic.CompareAndSwapInt32(&p.spinlock, 0, 1) {
		runtime.Gosched()
	}
	defer atomic.StoreInt32(&p.spinlock, 0)

	if p.pendingQueue.Busy() {
		if p.batchBuilder.IsFull() {
			return false, nil
		}
	}

	p.internalSend(sr)

	return true, err
}

func (p *batcher) Flush(ctx context.Context) error {
	wg := sync.WaitGroup{}
	wg.Add(1)

	fr := &flushRequest{&wg, nil}
	p.eventsChan <- fr

	wg.Wait()
	return fr.err
}

func (p *batcher) AsyncResult(ctx context.Context, id Identifier, expiration time.Duration) (Identifier, error) {
	return nil, errors.New("implement me")
}

func (p *batcher) Close() {
	if p.state != batcherBatchReady {
		// BatcherBench is closing
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	cp := &closeRequest{&wg}
	p.eventsChan <- cp

	wg.Wait()
}
