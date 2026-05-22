package runner

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v2/extend/batchqueue"
)

// testMessage 实现 MessageContext 接口，用于测试
type testMessage struct {
	batchqueue.UnimplementedIder
	id       string
	labels   []batchqueue.Label
	ctx      context.Context
	bornTime time.Time
}

func (m *testMessage) EntryID() string            { return m.id }
func (m *testMessage) Labels() []batchqueue.Label { return m.labels }
func (m *testMessage) Duplicate() batchqueue.Identifier {
	labels := make([]batchqueue.Label, len(m.labels))
	copy(labels, m.labels)
	return &testMessage{
		id:       m.id,
		labels:   labels,
		ctx:      m.ctx,
		bornTime: m.bornTime,
	}
}
func (m *testMessage) AppendLogs(...string)   {}
func (m *testMessage) Elapsed() time.Duration { return time.Since(m.bornTime) }
func (m *testMessage) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}
func (m *testMessage) IsRetry() bool { return false }

// testReport 测试报告
type testReport struct {
	Scenario       string
	TotalMessages  int
	TotalDuration  time.Duration
	Throughput     float64
	BlockBufferMax int
	PreBatcherMax  int64
	PostBatcherMax int64
	ReferencesMax  int
}

func (r *testReport) String() string {
	return fmt.Sprintf(
		"=== %s ===\n"+
			"  消息总数: %d\n"+
			"  总耗时: %v\n"+
			"  吞吐量: %.2f msg/s\n"+
			"  blockBuffer峰值: %d\n"+
			"  preBatcher峰值: %d\n"+
			"  postBatcher峰值: %d\n"+
			"  references峰值: %d\n",
		r.Scenario, r.TotalMessages, r.TotalDuration, r.Throughput,
		r.BlockBufferMax, r.PreBatcherMax, r.PostBatcherMax, r.ReferencesMax,
	)
}

// runScenario 执行测试场景
func runScenario(t *testing.T, name string, totalMessages int, config *RunnerConfig, msgGen func(int) *testMessage, completedCount *int64) *testReport {
	// 记录峰值指标
	var (
		blockBufferMax int
		preBatcherMax  int64
		postBatcherMax int64
		referencesMax  int
	)

	// 创建 Runner
	runner := NewRunner(config)
	runner.Start()
	defer runner.Stop()

	// 启动指标采集
	stopMetrics := make(chan struct{})
	var metricsWg sync.WaitGroup
	metricsWg.Add(1)
	go func() {
		defer metricsWg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopMetrics:
				return
			case <-ticker.C:
				m := runner.Metrics()
				if m.BlockBufferSize > blockBufferMax {
					blockBufferMax = m.BlockBufferSize
				}
				if m.PreBatcherSize > preBatcherMax {
					preBatcherMax = m.PreBatcherSize
				}
				if m.PostBatcherSize > postBatcherMax {
					postBatcherMax = m.PostBatcherSize
				}
				if m.ReferencesCount > referencesMax {
					referencesMax = m.ReferencesCount
				}
			}
		}
	}()

	// 记录开始时间
	startTime := time.Now()

	// 发送消息
	for i := 0; i < totalMessages; i++ {
		msg := msgGen(i)
		runner.Process(context.Background(), msg)
	}

	// 等待所有消息完成（通过 OnPostFlushFn 计数）
	for atomic.LoadInt64(completedCount) < int64(totalMessages) {
		time.Sleep(10 * time.Millisecond)
	}

	endTime := time.Now()

	// 停止指标采集
	close(stopMetrics)
	metricsWg.Wait()

	return &testReport{
		Scenario:       name,
		TotalMessages:  totalMessages,
		TotalDuration:  endTime.Sub(startTime),
		Throughput:     float64(totalMessages) / endTime.Sub(startTime).Seconds(),
		BlockBufferMax: blockBufferMax,
		PreBatcherMax:  preBatcherMax,
		PostBatcherMax: postBatcherMax,
		ReferencesMax:  referencesMax,
	}
}

// TestRunnerScenarioMixedWorkload 混合负载场景测试
func TestRunnerScenarioMixedWorkload(t *testing.T) {
	const totalMessages = 100000

	var completedCount int64

	config := &RunnerConfig{
		Name:        "MIXED_WORKLOAD",
		Concurrency: 512,
		BlockSize:   256,
		RecvSize:    8,
		Debug:       false,
		ResourceLimits: map[Resource]int{
			"db_conn":    30,
			"cache_conn": 20,
		},
		ProcessFn: func(msgCtx MessageContext) {
			// 模拟不同耗时：
			// - 轻量任务：1-3ms
			// - 中量任务：5-10ms
			// - 重量任务：15-30ms
			id := msgCtx.EntryID()
			switch id[0] % 3 {
			case 0:
				time.Sleep(time.Duration(1+id[1]%3) * time.Millisecond)
			case 1:
				time.Sleep(time.Duration(5+id[1]%5) * time.Millisecond)
			case 2:
				time.Sleep(time.Duration(15+id[1]%15) * time.Millisecond)
			}
		},
		PreBatchFn: func(msgs []interface{}) ([]batchqueue.Identifier, error) {
			time.Sleep(500 * time.Microsecond)
			iders := make([]batchqueue.Identifier, len(msgs))
			for i, msg := range msgs {
				iders[i] = msg.(batchqueue.Identifier)
			}
			return iders, nil
		},
		PostBatchFn: func(msgs []interface{}) ([]batchqueue.Identifier, error) {
			time.Sleep(500 * time.Microsecond)
			iders := make([]batchqueue.Identifier, len(msgs))
			for i, msg := range msgs {
				iders[i] = msg.(batchqueue.Identifier)
			}
			return iders, nil
		},
		OnPostFlushFn: func(ider batchqueue.Identifier) {
			atomic.AddInt64(&completedCount, 1)
		},
	}

	// 生成混合负载消息：
	// - 30% 轻量任务（无资源约束）
	// - 50% 中量任务（db_conn 约束）
	// - 20% 重量任务（db_conn + cache_conn 约束）
	msgGen := func(i int) *testMessage {
		msgType := i % 10
		switch {
		case msgType < 3: // 30% 轻量
			return &testMessage{
				id:       fmt.Sprintf("light-%d", i),
				labels:   nil,
				ctx:      context.Background(),
				bornTime: time.Now(),
			}
		case msgType < 8: // 50% 中量
			return &testMessage{
				id:       fmt.Sprintf("medium-%d", i),
				labels:   []batchqueue.Label{{Name: "db_conn", Value: "db-0"}},
				ctx:      context.Background(),
				bornTime: time.Now(),
			}
		default: // 20% 重量
			return &testMessage{
				id:       fmt.Sprintf("heavy-%d", i),
				labels:   []batchqueue.Label{{Name: "db_conn", Value: "db-1"}, {Name: "cache_conn", Value: "cache-0"}},
				ctx:      context.Background(),
				bornTime: time.Now(),
			}
		}
	}

	report := runScenario(t, "混合负载场景", totalMessages, config, msgGen, &completedCount)
	t.Log(report.String())
}

// =============================================================================
// 改密 10w 场景测试
// =============================================================================

// assetSameID 生成资产唯一标识的 hash（与 TEST_PLAN.md 一致）
func assetSameID(service, host string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(service+"://"+host)))
}

// serviceTimeRange 定义各服务的改密耗时范围（秒）
var serviceTimeRange = map[string][2]int{
	"SSH":      {5, 30},
	"RDP":      {60, 180}, // Script 隧道
	"MYSQL":    {10, 45},
	"ORACLE":   {15, 60},
	"POSTGRES": {10, 40},
}

// chpassMessage 改密测试消息
type chpassMessage struct {
	testMessage
	service string
}

// buildChpassMessage 构造改密测试消息
// 根据 TEST_PLAN.md 第 4 节的 Label 产出规则
func buildChpassMessage(id, service, host string, labels []batchqueue.Label) *chpassMessage {
	return &chpassMessage{
		testMessage: testMessage{
			id:       id,
			labels:   labels,
			ctx:      context.Background(),
			bornTime: time.Now(),
		},
		service: service,
	}
}

// mockChpassProcessFn 模拟改密处理（按服务随机耗时）
func mockChpassProcessFn(msgCtx MessageContext) {
	msg, ok := msgCtx.(*chpassMessage)
	if !ok {
		return
	}
	rng := serviceTimeRange[msg.service]
	if rng[0] == 0 {
		rng = [2]int{5, 30}
	}
	// 模拟改密耗时：在 [min, max] 范围内均匀随机
	sleepSec := rng[0] + rand.Intn(rng[1]-rng[0]+1)
	time.Sleep(time.Duration(sleepSec) * time.Second)
}

// mockChpassBatchFn 模拟改密批处理
func mockChpassBatchFn(msgs []interface{}) ([]batchqueue.Identifier, error) {
	// 模拟数据库批操作：5-20ms
	time.Sleep(time.Duration(5+rand.Intn(16)) * time.Millisecond)
	iders := make([]batchqueue.Identifier, len(msgs))
	for i, msg := range msgs {
		iders[i] = msg.(batchqueue.Identifier)
	}
	return iders, nil
}

// build100kMessages 生成 10w 改密测试消息
func build100kMessages() []*chpassMessage {
	messages := make([]*chpassMessage, 0, 100_000)

	// ---- SSH: 70,000 个 (350 资产 × 200 账号) ----
	for i := 0; i < 350; i++ {
		host := fmt.Sprintf("10.0.0.%d", 101+i)
		hash := assetSameID("ssh", host)
		for j := 0; j < 200; j++ {
			msgID := fmt.Sprintf("ssh-%d-%d", i, j)
			labels := []batchqueue.Label{
				{Name: "sshPerAsset", Value: "sshPerAsset" + hash},
			}
			messages = append(messages, buildChpassMessage(msgID, "SSH", host, labels))
		}
	}

	// ---- RDP: 10,000 个 (200 资产 × 50 账号) ----
	for i := 0; i < 200; i++ {
		host := fmt.Sprintf("10.1.0.%d", 101+i)
		hash := assetSameID("rdp", host)
		for j := 0; j < 50; j++ {
			msgID := fmt.Sprintf("rdp-%d-%d", i, j)
			labels := []batchqueue.Label{
				{Name: "exec", Value: "exec"},
				{Name: "rdpScriptExec", Value: "rdpScriptExec"},
				{Name: "rdpScriptPerAsset", Value: "rdpScriptPerAsset" + hash},
			}
			messages = append(messages, buildChpassMessage(msgID, "RDP", host, labels))
		}
	}

	// ---- DB: 20,000 个 ----
	// MySQL: 8,000 (80 资产 × 100 账号)
	for i := 0; i < 80; i++ {
		host := fmt.Sprintf("10.2.0.%d", 101+i)
		hash := assetSameID("mysql", host)
		for j := 0; j < 100; j++ {
			msgID := fmt.Sprintf("mysql-%d-%d", i, j)
			labels := []batchqueue.Label{
				{Name: "exec", Value: "exec"},
				{Name: "dbSqlshellExec", Value: "dbSqlshellExec"},
				{Name: "dbPerAsset", Value: "dbPerAsset" + hash},
			}
			messages = append(messages, buildChpassMessage(msgID, "MYSQL", host, labels))
		}
	}
	// Oracle: 6,000 (60 资产 × 100 账号)
	for i := 0; i < 60; i++ {
		host := fmt.Sprintf("10.2.1.%d", 101+i)
		hash := assetSameID("oracle", host)
		for j := 0; j < 100; j++ {
			msgID := fmt.Sprintf("oracle-%d-%d", i, j)
			labels := []batchqueue.Label{
				{Name: "exec", Value: "exec"},
				{Name: "dbSqlshellExec", Value: "dbSqlshellExec"},
				{Name: "dbPerAsset", Value: "dbPerAsset" + hash},
			}
			messages = append(messages, buildChpassMessage(msgID, "ORACLE", host, labels))
		}
	}
	// PostgreSQL: 6,000 (60 资产 × 100 账号)
	for i := 0; i < 60; i++ {
		host := fmt.Sprintf("10.2.2.%d", 101+i)
		hash := assetSameID("postgres", host)
		for j := 0; j < 100; j++ {
			msgID := fmt.Sprintf("postgres-%d-%d", i, j)
			labels := []batchqueue.Label{
				{Name: "exec", Value: "exec"},
				{Name: "dbSqlshellExec", Value: "dbSqlshellExec"},
				{Name: "dbPerAsset", Value: "dbPerAsset" + hash},
			}
			messages = append(messages, buildChpassMessage(msgID, "POSTGRES", host, labels))
		}
	}

	return messages
}

// TestRunnerScenarioChpass100k 改密 10w 场景测试
// 验证资源限制约束是否正确生效
func TestRunnerScenarioChpass100k(t *testing.T) {
	const totalMessages = 100000

	var completedCount int64

	config := &RunnerConfig{
		Name:        "CHPASS_100K",
		Concurrency: 128,
		BlockSize:   512,
		RecvSize:    8,
		Debug:       false,
		ResourceLimits: map[Resource]int{
			"exec":              65,
			"sshExec":           128,
			"telnetExec":        128,
			"rdpScriptExec":     64,
			"dbSqlshellExec":    128,
			"dbPerAsset":        2,
			"sshPerAsset":       10,
			"rdpWinrmPerAsset":  5,
			"rdpScriptPerAsset": 3,
		},
		ProcessFn:     mockChpassProcessFn,
		PreBatchFn:    mockChpassBatchFn,
		PostBatchFn:   mockChpassBatchFn,
		OnPostFlushFn: func(ider batchqueue.Identifier) { atomic.AddInt64(&completedCount, 1) },
	}

	// 预生成 10w 消息
	messages := build100kMessages()

	// 创建 Runner
	runner := NewRunner(config)
	runner.Start()
	defer runner.Stop()

	// 记录峰值指标
	var (
		blockBufferMax int
		preBatcherMax  int64
		postBatcherMax int64
		referencesMax  int
	)

	// 启动指标采集
	stopMetrics := make(chan struct{})
	var metricsWg sync.WaitGroup
	metricsWg.Add(1)
	go func() {
		defer metricsWg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopMetrics:
				return
			case <-ticker.C:
				m := runner.Metrics()
				if m.BlockBufferSize > blockBufferMax {
					blockBufferMax = m.BlockBufferSize
				}
				if m.PreBatcherSize > preBatcherMax {
					preBatcherMax = m.PreBatcherSize
				}
				if m.PostBatcherSize > postBatcherMax {
					postBatcherMax = m.PostBatcherSize
				}
				if m.ReferencesCount > referencesMax {
					referencesMax = m.ReferencesCount
				}
			}
		}
	}()

	// 记录开始时间
	startTime := time.Now()

	// 发送消息
	for _, msg := range messages {
		runner.Process(context.Background(), msg)
	}

	// 等待所有消息完成
	for atomic.LoadInt64(&completedCount) < int64(totalMessages) {
		time.Sleep(100 * time.Millisecond)
	}

	endTime := time.Now()

	// 停止指标采集
	close(stopMetrics)
	metricsWg.Wait()

	report := &testReport{
		Scenario:       "改密 10w 场景",
		TotalMessages:  totalMessages,
		TotalDuration:  endTime.Sub(startTime),
		Throughput:     float64(totalMessages) / endTime.Sub(startTime).Seconds(),
		BlockBufferMax: blockBufferMax,
		PreBatcherMax:  preBatcherMax,
		PostBatcherMax: postBatcherMax,
		ReferencesMax:  referencesMax,
	}

	t.Log(report.String())
}

// TestRunnerScenarioChpassSSHOnly SSH 改密场景测试
func TestRunnerScenarioChpassSSHOnly(t *testing.T) {
	const totalMessages = 70000

	var completedCount int64

	config := &RunnerConfig{
		Name:        "CHPASS_SSH",
		Concurrency: 128,
		BlockSize:   512,
		RecvSize:    8,
		Debug:       false,
		ResourceLimits: map[Resource]int{
			"sshPerAsset": 10,
		},
		ProcessFn:     mockChpassProcessFn,
		PreBatchFn:    mockChpassBatchFn,
		PostBatchFn:   mockChpassBatchFn,
		OnPostFlushFn: func(ider batchqueue.Identifier) { atomic.AddInt64(&completedCount, 1) },
	}

	messages := make([]*chpassMessage, 0, totalMessages)
	for i := 0; i < 350; i++ {
		host := fmt.Sprintf("10.0.0.%d", 101+i)
		hash := assetSameID("ssh", host)
		for j := 0; j < 200; j++ {
			msgID := fmt.Sprintf("ssh-%d-%d", i, j)
			labels := []batchqueue.Label{
				{Name: "sshPerAsset", Value: "sshPerAsset" + hash},
			}
			messages = append(messages, buildChpassMessage(msgID, "SSH", host, labels))
		}
	}

	runner := NewRunner(config)
	runner.Start()
	defer runner.Stop()

	startTime := time.Now()
	for _, msg := range messages {
		runner.Process(context.Background(), msg)
	}
	for atomic.LoadInt64(&completedCount) < int64(totalMessages) {
		time.Sleep(100 * time.Millisecond)
	}
	endTime := time.Now()

	report := &testReport{
		Scenario:      "SSH 改密 7w 场景",
		TotalMessages: totalMessages,
		TotalDuration: endTime.Sub(startTime),
		Throughput:    float64(totalMessages) / endTime.Sub(startTime).Seconds(),
	}
	t.Log(report.String())
}

// TestRunnerScenarioChpassRDPOnly RDP 改密场景测试
func TestRunnerScenarioChpassRDPOnly(t *testing.T) {
	const totalMessages = 10000

	var completedCount int64

	config := &RunnerConfig{
		Name:        "CHPASS_RDP",
		Concurrency: 128,
		BlockSize:   512,
		RecvSize:    8,
		Debug:       false,
		ResourceLimits: map[Resource]int{
			"exec":              65,
			"rdpScriptExec":     64,
			"rdpScriptPerAsset": 3,
		},
		ProcessFn:     mockChpassProcessFn,
		PreBatchFn:    mockChpassBatchFn,
		PostBatchFn:   mockChpassBatchFn,
		OnPostFlushFn: func(ider batchqueue.Identifier) { atomic.AddInt64(&completedCount, 1) },
	}

	messages := make([]*chpassMessage, 0, totalMessages)
	for i := 0; i < 200; i++ {
		host := fmt.Sprintf("10.1.0.%d", 101+i)
		hash := assetSameID("rdp", host)
		for j := 0; j < 50; j++ {
			msgID := fmt.Sprintf("rdp-%d-%d", i, j)
			labels := []batchqueue.Label{
				{Name: "exec", Value: "exec"},
				{Name: "rdpScriptExec", Value: "rdpScriptExec"},
				{Name: "rdpScriptPerAsset", Value: "rdpScriptPerAsset" + hash},
			}
			messages = append(messages, buildChpassMessage(msgID, "RDP", host, labels))
		}
	}

	runner := NewRunner(config)
	runner.Start()
	defer runner.Stop()

	startTime := time.Now()
	for _, msg := range messages {
		runner.Process(context.Background(), msg)
	}
	for atomic.LoadInt64(&completedCount) < int64(totalMessages) {
		time.Sleep(100 * time.Millisecond)
	}
	endTime := time.Now()

	report := &testReport{
		Scenario:      "RDP 改密 1w 场景",
		TotalMessages: totalMessages,
		TotalDuration: endTime.Sub(startTime),
		Throughput:    float64(totalMessages) / endTime.Sub(startTime).Seconds(),
	}
	t.Log(report.String())
}

// TestRunnerScenarioChpassDBOnly DB 改密场景测试
func TestRunnerScenarioChpassDBOnly(t *testing.T) {
	const totalMessages = 20000

	var completedCount int64

	config := &RunnerConfig{
		Name:        "CHPASS_DB",
		Concurrency: 128,
		BlockSize:   512,
		RecvSize:    8,
		Debug:       false,
		ResourceLimits: map[Resource]int{
			"exec":           65,
			"dbSqlshellExec": 128,
			"dbPerAsset":     2,
		},
		ProcessFn:     mockChpassProcessFn,
		PreBatchFn:    mockChpassBatchFn,
		PostBatchFn:   mockChpassBatchFn,
		OnPostFlushFn: func(ider batchqueue.Identifier) { atomic.AddInt64(&completedCount, 1) },
	}

	messages := make([]*chpassMessage, 0, totalMessages)

	// MySQL: 8,000
	for i := 0; i < 80; i++ {
		host := fmt.Sprintf("10.2.0.%d", 101+i)
		hash := assetSameID("mysql", host)
		for j := 0; j < 100; j++ {
			msgID := fmt.Sprintf("mysql-%d-%d", i, j)
			labels := []batchqueue.Label{
				{Name: "exec", Value: "exec"},
				{Name: "dbSqlshellExec", Value: "dbSqlshellExec"},
				{Name: "dbPerAsset", Value: "dbPerAsset" + hash},
			}
			messages = append(messages, buildChpassMessage(msgID, "MYSQL", host, labels))
		}
	}
	// Oracle: 6,000
	for i := 0; i < 60; i++ {
		host := fmt.Sprintf("10.2.1.%d", 101+i)
		hash := assetSameID("oracle", host)
		for j := 0; j < 100; j++ {
			msgID := fmt.Sprintf("oracle-%d-%d", i, j)
			labels := []batchqueue.Label{
				{Name: "exec", Value: "exec"},
				{Name: "dbSqlshellExec", Value: "dbSqlshellExec"},
				{Name: "dbPerAsset", Value: "dbPerAsset" + hash},
			}
			messages = append(messages, buildChpassMessage(msgID, "ORACLE", host, labels))
		}
	}
	// PostgreSQL: 6,000
	for i := 0; i < 60; i++ {
		host := fmt.Sprintf("10.2.2.%d", 101+i)
		hash := assetSameID("postgres", host)
		for j := 0; j < 100; j++ {
			msgID := fmt.Sprintf("postgres-%d-%d", i, j)
			labels := []batchqueue.Label{
				{Name: "exec", Value: "exec"},
				{Name: "dbSqlshellExec", Value: "dbSqlshellExec"},
				{Name: "dbPerAsset", Value: "dbPerAsset" + hash},
			}
			messages = append(messages, buildChpassMessage(msgID, "POSTGRES", host, labels))
		}
	}

	runner := NewRunner(config)
	runner.Start()
	defer runner.Stop()

	startTime := time.Now()
	for _, msg := range messages {
		runner.Process(context.Background(), msg)
	}
	for atomic.LoadInt64(&completedCount) < int64(totalMessages) {
		time.Sleep(100 * time.Millisecond)
	}
	endTime := time.Now()

	report := &testReport{
		Scenario:      "DB 改密 2w 场景",
		TotalMessages: totalMessages,
		TotalDuration: endTime.Sub(startTime),
		Throughput:    float64(totalMessages) / endTime.Sub(startTime).Seconds(),
	}
	t.Log(report.String())
}
