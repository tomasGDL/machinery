package batchqueue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkBatcher_Send measures the throughput of Send under various batch sizes.
func BenchmarkBatcher_Send(b *testing.B) {
	ctx := context.Background()
	conf := &Config{
		Name:        "bench-send",
		DoBatchFn:   func(msgs []interface{}) ([]Identifier, error) { return nil, nil },
		MaxBatching: 1000,
		BatchingMaxFlushDelay: 10 * time.Millisecond,
	}

	batcher := NewBatcher(ctx, conf)
	defer batcher.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = batcher.Send(ctx, i)
	}
}

// BenchmarkBatcher_SendAsync measures the throughput of SendAsync.
func BenchmarkBatcher_SendAsync(b *testing.B) {
	ctx := context.Background()
	conf := &Config{
		Name:        "bench-async",
		DoBatchFn:   func(msgs []interface{}) ([]Identifier, error) { return nil, nil },
		MaxBatching: 1000,
		BatchingMaxFlushDelay: 10 * time.Millisecond,
	}

	batcher := NewBatcher(ctx, conf)
	defer batcher.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = batcher.SendAsync(ctx, i)
	}
}

// BenchmarkBatcher_Flush measures the latency of Flush with pending messages.
func BenchmarkBatcher_Flush(b *testing.B) {
	ctx := context.Background()
	conf := &Config{
		Name:        "bench-flush",
		DoBatchFn:   func(msgs []interface{}) ([]Identifier, error) { return nil, nil },
		MaxBatching: 1000,
		BatchingMaxFlushDelay: 1 * time.Hour,
	}

	batcher := NewBatcher(ctx, conf)
	defer batcher.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batcher.Send(ctx, i)
		_ = batcher.Flush(ctx)
	}
}

// BenchmarkBatcher_ConcurrentSend measures throughput under concurrent load.
func BenchmarkBatcher_ConcurrentSend(b *testing.B) {
	ctx := context.Background()
	var processed int64
	conf := &Config{
		Name: "bench-concurrent",
		DoBatchFn: func(msgs []interface{}) ([]Identifier, error) {
			atomic.AddInt64(&processed, int64(len(msgs)))
			return nil, nil
		},
		MaxBatching:           1000,
		BatchingMaxFlushDelay: 10 * time.Millisecond,
	}

	batcher := NewBatcher(ctx, conf)
	defer batcher.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = batcher.Send(ctx, i)
			i++
		}
	})
}

// BenchmarkBatcher_HeavyProcessing simulates a slow DoBatchFn to measure queue behavior.
func BenchmarkBatcher_HeavyProcessing(b *testing.B) {
	ctx := context.Background()
	conf := &Config{
		Name: "bench-heavy",
		DoBatchFn: func(msgs []interface{}) ([]Identifier, error) {
			time.Sleep(1 * time.Millisecond)
			return nil, nil
		},
		MaxBatching:           100,
		BatchingMaxFlushDelay: 5 * time.Millisecond,
		MaxPendingMessages:    100,
	}

	batcher := NewBatcher(ctx, conf)
	defer batcher.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = batcher.Send(ctx, i)
	}
}
