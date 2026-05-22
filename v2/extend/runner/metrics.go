package runner

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// messageCounters 消息链路各阶段计数器
	messageReceivedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "runner_messages_received_total",
			Help: "Total number of messages received by the runner",
		},
		[]string{"runner"},
	)
	messageScheduledTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "runner_messages_scheduled_total",
			Help: "Total number of messages successfully scheduled to pre-batcher",
		},
		[]string{"runner"},
	)
	messageProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "runner_messages_processed_total",
			Help: "Total number of messages processed by workers",
		},
		[]string{"runner"},
	)
	messagePostFlushedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "runner_messages_postflushed_total",
			Help: "Total number of messages completed post-batcher processing",
		},
		[]string{"runner"},
	)
	scheduleRejectedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "runner_schedule_rejected_total",
			Help: "Total number of schedule attempts rejected (batcher full or resource limit)",
		},
		[]string{"runner", "reason"},
	)

	// queueGauges 各队列/缓冲区的当前大小
	blockBufferSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "runner_block_buffer_size",
			Help: "Current number of messages in block buffer",
		},
		[]string{"runner"},
	)
	recvChanSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "runner_recv_chan_size",
			Help: "Current number of messages in recv channel",
		},
		[]string{"runner"},
	)
	msgChanSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "runner_msg_chan_size",
			Help: "Current number of messages in msg channel (pre-batcher to worker)",
		},
		[]string{"runner"},
	)
	refFlushChanSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "runner_ref_flush_chan_size",
			Help: "Current number of messages in ref flush channel",
		},
		[]string{"runner"},
	)
	referencesCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "runner_references_count",
			Help: "Current number of messages being processed (reference count)",
		},
		[]string{"runner"},
	)
	preBatcherSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "runner_prebatcher_size",
			Help: "Current number of in-flight messages in pre-batcher",
		},
		[]string{"runner"},
	)
	postBatcherSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "runner_postbatcher_size",
			Help: "Current number of in-flight messages in post-batcher",
		},
		[]string{"runner"},
	)
	workerProcessingSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "runner_worker_processing_size",
			Help: "Current number of messages being processed by workers",
		},
		[]string{"runner"},
	)

	// resourceUsageGauges 资源限制使用情况
	resourceUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "runner_resource_usage",
			Help: "Current usage count of each resource limit",
		},
		[]string{"runner", "resource", "value"},
	)
	resourceLimit = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "runner_resource_limit",
			Help: "Configured limit for each resource",
		},
		[]string{"runner", "resource"},
	)

	// durationHistograms 各阶段处理耗时
	processDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "runner_process_duration_seconds",
			Help:    "Duration of ProcessFn execution",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"runner"},
	)
	preBatchDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "runner_prebatch_duration_seconds",
			Help:    "Duration of PreBatchFn execution",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"runner"},
	)
	postBatchDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "runner_postbatch_duration_seconds",
			Help:    "Duration of PostBatchFn execution",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"runner"},
	)
	scheduleWaitDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "runner_schedule_wait_duration_seconds",
			Help:    "Duration from message received to scheduled",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"runner"},
	)
	endToEndDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "runner_end_to_end_duration_seconds",
			Help:    "End-to-end duration from message received to post-flush completed",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"runner"},
	)
)

// runnerMetrics 封装单个 Runner 实例的指标操作
type runnerMetrics struct {
	runnerName string
}

func newRunnerMetrics(name string) *runnerMetrics {
	return &runnerMetrics{runnerName: name}
}

func (rm *runnerMetrics) label() prometheus.Labels {
	return prometheus.Labels{"runner": rm.runnerName}
}

func (rm *runnerMetrics) labelWithReason(reason string) prometheus.Labels {
	return prometheus.Labels{"runner": rm.runnerName, "reason": reason}
}

func (rm *runnerMetrics) labelWithResource(resource, value string) prometheus.Labels {
	return prometheus.Labels{"runner": rm.runnerName, "resource": resource, "value": value}
}

func (rm *runnerMetrics) labelWithResourceName(resource string) prometheus.Labels {
	return prometheus.Labels{"runner": rm.runnerName, "resource": resource}
}

// IncMessagesReceived 增加接收消息计数
func (rm *runnerMetrics) IncMessagesReceived() {
	messageReceivedTotal.With(rm.label()).Inc()
}

// IncMessagesScheduled 增加调度成功计数
func (rm *runnerMetrics) IncMessagesScheduled() {
	messageScheduledTotal.With(rm.label()).Inc()
}

// IncMessagesProcessed 增加处理完成计数
func (rm *runnerMetrics) IncMessagesProcessed() {
	messageProcessedTotal.With(rm.label()).Inc()
}

// IncMessagesPostFlushed 增加 post-flush 完成计数
func (rm *runnerMetrics) IncMessagesPostFlushed() {
	messagePostFlushedTotal.With(rm.label()).Inc()
}

// IncScheduleRejected 增加调度拒绝计数
func (rm *runnerMetrics) IncScheduleRejected(reason string) {
	scheduleRejectedTotal.With(rm.labelWithReason(reason)).Inc()
}

// SetBlockBufferSize 设置 block buffer 大小
func (rm *runnerMetrics) SetBlockBufferSize(size float64) {
	blockBufferSize.With(rm.label()).Set(size)
}

// SetRecvChanSize 设置 recv channel 大小
func (rm *runnerMetrics) SetRecvChanSize(size float64) {
	recvChanSize.With(rm.label()).Set(size)
}

// SetMsgChanSize 设置 msg channel 大小
func (rm *runnerMetrics) SetMsgChanSize(size float64) {
	msgChanSize.With(rm.label()).Set(size)
}

// SetRefFlushChanSize 设置 ref flush channel 大小
func (rm *runnerMetrics) SetRefFlushChanSize(size float64) {
	refFlushChanSize.With(rm.label()).Set(size)
}

// SetReferencesCount 设置引用计数
func (rm *runnerMetrics) SetReferencesCount(count float64) {
	referencesCount.With(rm.label()).Set(count)
}

// SetPreBatcherSize 设置 pre-batcher 在途数
func (rm *runnerMetrics) SetPreBatcherSize(size float64) {
	preBatcherSize.With(rm.label()).Set(size)
}

// SetPostBatcherSize 设置 post-batcher 在途数
func (rm *runnerMetrics) SetPostBatcherSize(size float64) {
	postBatcherSize.With(rm.label()).Set(size)
}

// SetWorkerProcessingSize 设置 worker 处理中消息数
func (rm *runnerMetrics) SetWorkerProcessingSize(size float64) {
	workerProcessingSize.With(rm.label()).Set(size)
}

// SetResourceUsage 设置资源使用量
func (rm *runnerMetrics) SetResourceUsage(resource, value string, count float64) {
	resourceUsage.With(rm.labelWithResource(resource, value)).Set(count)
}

// SetResourceLimit 设置资源限制
func (rm *runnerMetrics) SetResourceLimit(resource string, limit float64) {
	resourceLimit.With(rm.labelWithResourceName(resource)).Set(limit)
}

// ObserveProcessDuration 记录 ProcessFn 耗时
func (rm *runnerMetrics) ObserveProcessDuration(d time.Duration) {
	processDuration.With(rm.label()).Observe(d.Seconds())
}

// ObservePreBatchDuration 记录 PreBatchFn 耗时
func (rm *runnerMetrics) ObservePreBatchDuration(d time.Duration) {
	preBatchDuration.With(rm.label()).Observe(d.Seconds())
}

// ObservePostBatchDuration 记录 PostBatchFn 耗时
func (rm *runnerMetrics) ObservePostBatchDuration(d time.Duration) {
	postBatchDuration.With(rm.label()).Observe(d.Seconds())
}

// ObserveScheduleWaitDuration 记录调度等待耗时
func (rm *runnerMetrics) ObserveScheduleWaitDuration(d time.Duration) {
	scheduleWaitDuration.With(rm.label()).Observe(d.Seconds())
}

// ObserveEndToEndDuration 记录端到端耗时
func (rm *runnerMetrics) ObserveEndToEndDuration(d time.Duration) {
	endToEndDuration.With(rm.label()).Observe(d.Seconds())
}

// updateAllGauges 一次性更新所有 Gauge 指标
func (rm *runnerMetrics) updateAllGauges(r *Runner) {
	// blockBuffer 统计有效消息数（valid=true）
	var blockBufferValid int
	for _, bm := range r.blockBuffer {
		if bm.valid {
			blockBufferValid++
		}
	}
	rm.SetBlockBufferSize(float64(blockBufferValid))
	rm.SetRecvChanSize(float64(len(r.recvChan)))
	rm.SetMsgChanSize(float64(len(r.msgChan)))
	rm.SetRefFlushChanSize(float64(len(r.refFlushChan)))
	rm.SetReferencesCount(float64(len(r.references)))
	rm.SetPreBatcherSize(float64(r.preBatcher.Size()))
	rm.SetPostBatcherSize(float64(r.postBatcher.Size()))
	rm.SetWorkerProcessingSize(float64(r.workerProcessing))

	// 更新资源限制指标
	for resource, limit := range r.ResourceLimits {
		rm.SetResourceLimit(string(resource), float64(limit))
	}

	// 更新资源使用量
	for key := range r.references {
		// 跳过 EntryID（它们不是资源标签）
		// 资源标签的 key 对应 ResourceLimits 中的 key
		for resource := range r.ResourceLimits {
			if key == string(resource) {
				// 这里无法直接知道 label value，所以只更新总量
				// 更细粒度的资源使用在 mark/unmark 时更新
				break
			}
		}
	}
}

// String 返回格式化的指标文本（用于日志打印）
func (rm *runnerMetrics) String(r *Runner) string {
	// blockBuffer 统计有效消息数（valid=true）
	var blockBufferValid int
	for _, bm := range r.blockBuffer {
		if bm.valid {
			blockBufferValid++
		}
	}
	return fmt.Sprintf(
		"recvChan=%d blockBuffer=%d preBatcher=%d msgChan=%d worker=%d postBatcher=%d refFlushChan=%d references=%d",
		len(r.recvChan),
		blockBufferValid,
		r.preBatcher.Size(),
		len(r.msgChan),
		r.workerProcessing,
		r.postBatcher.Size(),
		len(r.refFlushChan),
		len(r.references),
	)
}
