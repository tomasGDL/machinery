package batchqueue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockIdentifier is a simple Identifier implementation for testing.
type mockIdentifier struct {
	id string
}

func (m *mockIdentifier) EntryID() string   { return m.id }
func (m *mockIdentifier) Labels() []Label   { return nil }
func (m *mockIdentifier) Duplicate() Identifier { return &mockIdentifier{id: m.id} }

// TestNewBatcher_BasicFunctionality verifies that a Batcher can be created and closed.
func TestNewBatcher_BasicFunctionality(t *testing.T) {
	ctx := context.Background()
	conf := &Config{
		Name:        "test-batcher",
		DoBatchFn:   func(msgs []interface{}) ([]Identifier, error) { return nil, nil },
		MaxBatching: 10,
	}

	b := NewBatcher(ctx, conf)
	if b == nil {
		t.Fatal("NewBatcher returned nil")
	}

	b.Close()
}

// TestBatcher_Send verifies that Send accepts messages without error.
func TestBatcher_Send(t *testing.T) {
	ctx := context.Background()
	processed := make(chan []interface{}, 1)
	conf := &Config{
		Name:      "test-send",
		DoBatchFn: func(msgs []interface{}) ([]Identifier, error) {
			processed <- msgs
			return nil, nil
		},
		MaxBatching:           2,
		BatchingMaxFlushDelay: 50 * time.Millisecond,
	}

	b := NewBatcher(ctx, conf)
	defer b.Close()

	if err := b.Send(ctx, "msg1"); err != nil {
		t.Fatalf("Send error: %v", err)
	}

	select {
	case msgs := <-processed:
		if len(msgs) != 1 {
			t.Fatalf("expected 1 message, got %d", len(msgs))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for batch processing")
	}
}

// TestBatcher_Send_MultipleMessages verifies batching of multiple messages.
func TestBatcher_Send_MultipleMessages(t *testing.T) {
	ctx := context.Background()
	processed := make(chan []interface{}, 1)
	conf := &Config{
		Name:      "test-multiple",
		DoBatchFn: func(msgs []interface{}) ([]Identifier, error) {
			processed <- msgs
			return nil, nil
		},
		MaxBatching:           3,
		BatchingMaxFlushDelay: 500 * time.Millisecond,
	}

	b := NewBatcher(ctx, conf)
	defer b.Close()

	for i := 0; i < 3; i++ {
		if err := b.Send(ctx, i); err != nil {
			t.Fatalf("Send error: %v", err)
		}
	}

	select {
	case msgs := <-processed:
		if len(msgs) != 3 {
			t.Fatalf("expected 3 messages, got %d", len(msgs))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for batch processing")
	}
}

// TestBatcher_SendAsync verifies SendAsync behavior.
func TestBatcher_SendAsync(t *testing.T) {
	ctx := context.Background()
	processed := make(chan []interface{}, 1)
	conf := &Config{
		Name:      "test-async",
		DoBatchFn: func(msgs []interface{}) ([]Identifier, error) {
			processed <- msgs
			return nil, nil
		},
		MaxBatching:           2,
		BatchingMaxFlushDelay: 50 * time.Millisecond,
	}

	b := NewBatcher(ctx, conf)
	defer b.Close()

	ok, err := b.SendAsync(ctx, "msg1")
	if err != nil {
		t.Fatalf("SendAsync error: %v", err)
	}
	if !ok {
		t.Fatal("SendAsync returned false")
	}

	select {
	case msgs := <-processed:
		if len(msgs) != 1 {
			t.Fatalf("expected 1 message, got %d", len(msgs))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for batch processing")
	}
}

// TestBatcher_Flush verifies Flush waits for pending batches.
func TestBatcher_Flush(t *testing.T) {
	ctx := context.Background()
	var processed int32
	conf := &Config{
		Name:      "test-flush",
		DoBatchFn: func(msgs []interface{}) ([]Identifier, error) {
			atomic.AddInt32(&processed, 1)
			return nil, nil
		},
		MaxBatching:           100,
		BatchingMaxFlushDelay: 10 * time.Second,
	}

	b := NewBatcher(ctx, conf)

	if err := b.Send(ctx, "msg1"); err != nil {
		t.Fatalf("Send error: %v", err)
	}

	if err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush error: %v", err)
	}

	if atomic.LoadInt32(&processed) != 1 {
		t.Fatalf("expected 1 batch processed, got %d", atomic.LoadInt32(&processed))
	}

	b.Close()
}

// TestBatcher_Flush_ErrorPropagation verifies Flush propagates errors from DoBatchFn.
func TestBatcher_Flush_ErrorPropagation(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("batch processing failed")
	conf := &Config{
		Name:      "test-flush-error",
		DoBatchFn: func(msgs []interface{}) ([]Identifier, error) {
			return nil, expectedErr
		},
		MaxBatching:           100,
		BatchingMaxFlushDelay: 10 * time.Second,
	}

	b := NewBatcher(ctx, conf)

	if err := b.Send(ctx, "msg1"); err != nil {
		t.Fatalf("Send error: %v", err)
	}

	if err := b.Flush(ctx); err == nil {
		t.Fatal("expected error from Flush, got nil")
	} else if err != expectedErr {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}

	b.Close()
}

// TestBatcher_Size verifies Size reflects the number of pending messages.
func TestBatcher_Size(t *testing.T) {
	ctx := context.Background()
	blocker := make(chan struct{})
	conf := &Config{
		Name:      "test-size",
		DoBatchFn: func(msgs []interface{}) ([]Identifier, error) {
			<-blocker
			return nil, nil
		},
		MaxBatching:           100,
		BatchingMaxFlushDelay: 10 * time.Second,
	}

	b := NewBatcher(ctx, conf)
	defer b.Close()

	if err := b.Send(ctx, "msg1"); err != nil {
		t.Fatalf("Send error: %v", err)
	}
	if err := b.Send(ctx, "msg2"); err != nil {
		t.Fatalf("Send error: %v", err)
	}

	// Allow time for the batch to be queued
	time.Sleep(50 * time.Millisecond)

	size := b.Size()
	if size != 2 {
		t.Fatalf("expected size 2, got %d", size)
	}

	close(blocker)
}

// TestBatcher_Close_Idempotent verifies Close can be called multiple times safely.
func TestBatcher_Close_Idempotent(t *testing.T) {
	ctx := context.Background()
	conf := &Config{
		Name:        "test-close",
		DoBatchFn:   func(msgs []interface{}) ([]Identifier, error) { return nil, nil },
		MaxBatching: 10,
	}

	b := NewBatcher(ctx, conf)
	b.Close()
	b.Close()
}

// TestBatcher_PostFlushFn verifies PostFlushFn is called with identifiers.
func TestBatcher_PostFlushFn(t *testing.T) {
	ctx := context.Background()
	flushed := make(chan []Identifier, 1)
	conf := &Config{
		Name: "test-postflush",
		DoBatchFn: func(msgs []interface{}) ([]Identifier, error) {
			return []Identifier{&mockIdentifier{id: "id1"}}, nil
		},
		PostFlushFn: func(iders []Identifier) {
			flushed <- iders
		},
		MaxBatching:           1,
		BatchingMaxFlushDelay: 50 * time.Millisecond,
	}

	b := NewBatcher(ctx, conf)
	defer b.Close()

	if err := b.Send(ctx, "msg1"); err != nil {
		t.Fatalf("Send error: %v", err)
	}

	select {
	case iders := <-flushed:
		if len(iders) != 1 {
			t.Fatalf("expected 1 identifier, got %d", len(iders))
		}
		if iders[0].EntryID() != "id1" {
			t.Fatalf("expected id1, got %s", iders[0].EntryID())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for PostFlushFn")
	}
}

// TestBatcher_ConcurrentSend verifies concurrent Send operations.
func TestBatcher_ConcurrentSend(t *testing.T) {
	ctx := context.Background()
	var processedCount int32
	conf := &Config{
		Name:      "test-concurrent",
		DoBatchFn: func(msgs []interface{}) ([]Identifier, error) {
			atomic.AddInt32(&processedCount, int32(len(msgs)))
			return nil, nil
		},
		MaxBatching:           10,
		BatchingMaxFlushDelay: 100 * time.Millisecond,
	}

	b := NewBatcher(ctx, conf)
	defer b.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := b.Send(ctx, i); err != nil {
				t.Errorf("Send error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Allow time for processing
	time.Sleep(300 * time.Millisecond)

	if atomic.LoadInt32(&processedCount) != 100 {
		t.Fatalf("expected 100 processed messages, got %d", atomic.LoadInt32(&processedCount))
	}
}

// TestBatcher_AsyncResult_NotImplemented verifies AsyncResult returns an error.
func TestBatcher_AsyncResult_NotImplemented(t *testing.T) {
	ctx := context.Background()
	conf := &Config{
		Name:        "test-asyncresult",
		DoBatchFn:   func(msgs []interface{}) ([]Identifier, error) { return nil, nil },
		MaxBatching: 10,
	}

	b := NewBatcher(ctx, conf)
	defer b.Close()

	_, err := b.AsyncResult(ctx, &mockIdentifier{id: "test"}, time.Second)
	if err == nil {
		t.Fatal("expected error from AsyncResult, got nil")
	}
}

// TestBatcher_ContextCancellation verifies behavior when context is cancelled.
func TestBatcher_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	conf := &Config{
		Name:        "test-ctx-cancel",
		DoBatchFn:   func(msgs []interface{}) ([]Identifier, error) { return nil, nil },
		MaxBatching: 10,
	}

	b := NewBatcher(ctx, conf)

	cancel()

	// After context cancellation, Send may or may not succeed depending on implementation.
	// The interface doesn't specify behavior here, so we just verify it doesn't panic.
	_ = b.Send(ctx, "msg1")

	b.Close()
}
