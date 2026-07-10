package sqs

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBatcherFlushBySize(t *testing.T) {
	var mu sync.Mutex
	var calls [][]string

	b := newBatcher(2, time.Hour, func(_ context.Context, entries []batchEntry) {
		mu.Lock()
		var ids []string
		for _, e := range entries {
			ids = append(ids, e.receiptHandle)
		}
		calls = append(calls, ids)
		mu.Unlock()

		for _, e := range entries {
			e.result <- nil
		}
	})

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(2)
	var err1, err2 error
	go func() {
		defer wg.Done()
		err1 = b.enqueue(ctx, "handle-1", nil)
	}()
	go func() {
		defer wg.Done()
		err2 = b.enqueue(ctx, "handle-2", nil)
	}()
	wg.Wait()

	assert.NoError(t, err1)
	assert.NoError(t, err2)
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, calls, 1)
	assert.ElementsMatch(t, []string{"handle-1", "handle-2"}, calls[0])
}

func TestBatcherFlushByInterval(t *testing.T) {
	flushed := make(chan []string, 1)

	b := newBatcher(10, 50*time.Millisecond, func(_ context.Context, entries []batchEntry) {
		var ids []string
		for _, e := range entries {
			ids = append(ids, e.receiptHandle)
		}
		flushed <- ids
		for _, e := range entries {
			e.result <- nil
		}
	})

	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		done <- b.enqueue(ctx, "handle-1", nil)
	}()

	select {
	case ids := <-flushed:
		assert.Equal(t, []string{"handle-1"}, ids)
	case <-time.After(2 * time.Second):
		t.Fatal("expected flush to be triggered by interval timeout")
	}

	assert.NoError(t, <-done)
}

func TestBatcherPartialFailure(t *testing.T) {
	b := newBatcher(2, time.Hour, func(_ context.Context, entries []batchEntry) {
		for _, e := range entries {
			if e.receiptHandle == "bad-handle" {
				e.result <- fmt.Errorf("boom")
				continue
			}
			e.result <- nil
		}
	})

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(2)
	var okErr, badErr error
	go func() {
		defer wg.Done()
		okErr = b.enqueue(ctx, "good-handle", nil)
	}()
	go func() {
		defer wg.Done()
		badErr = b.enqueue(ctx, "bad-handle", nil)
	}()
	wg.Wait()

	assert.NoError(t, okErr)
	assert.EqualError(t, badErr, "boom")
}

func TestBatcherCloseAndFlush(t *testing.T) {
	flushed := make(chan []string, 1)

	b := newBatcher(10, time.Hour, func(_ context.Context, entries []batchEntry) {
		var ids []string
		for _, e := range entries {
			ids = append(ids, e.receiptHandle)
		}
		flushed <- ids
		for _, e := range entries {
			e.result <- nil
		}
	})

	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		done <- b.enqueue(ctx, "handle-1", nil)
	}()

	// give enqueue time to register the pending entry before we force the flush
	time.Sleep(20 * time.Millisecond)
	b.closeAndFlush(ctx)

	select {
	case ids := <-flushed:
		assert.Equal(t, []string{"handle-1"}, ids)
	case <-time.After(2 * time.Second):
		t.Fatal("expected closeAndFlush to flush pending entries")
	}
	assert.NoError(t, <-done)
}
