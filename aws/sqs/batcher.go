package sqs

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// batchEntry represents a single unit of work waiting to be flushed as part of a batch request.
type batchEntry struct {
	result        chan error
	timeout       *int32
	id            string
	receiptHandle string
}

// batchFlushFunc sends a group of entries (up to 10, the SQS hard limit) and reports
// the outcome of each one back through its result channel.
type batchFlushFunc func(ctx context.Context, entries []batchEntry)

// batcher accumulates entries from concurrent callers and flushes them together,
// either when batchSize is reached or when interval elapses since the first pending entry.
type batcher struct {
	timer     *time.Timer
	flushFn   batchFlushFunc
	pending   []batchEntry
	mu        sync.Mutex
	nextID    uint64
	batchSize int
	interval  time.Duration
}

func newBatcher(batchSize int, interval time.Duration, flushFn batchFlushFunc) *batcher {
	if batchSize <= 0 || batchSize > 10 {
		batchSize = 10
	}

	return &batcher{
		batchSize: batchSize,
		interval:  interval,
		flushFn:   flushFn,
	}
}

// enqueue adds a request to the current batch and blocks until it has been flushed,
// returning the error reported for this specific entry (nil on success).
func (b *batcher) enqueue(ctx context.Context, receiptHandle string, timeout *int32) error {
	entry := batchEntry{
		id:            b.newID(),
		receiptHandle: receiptHandle,
		timeout:       timeout,
		result:        make(chan error, 1),
	}

	b.mu.Lock()
	b.pending = append(b.pending, entry)
	flushNow := len(b.pending) >= b.batchSize
	if len(b.pending) == 1 && !flushNow {
		if b.timer == nil {
			b.timer = time.AfterFunc(b.interval, func() { b.flush(ctx) })
		} else {
			b.timer.Reset(b.interval)
		}
	}
	b.mu.Unlock()

	if flushNow {
		b.flush(ctx)
	}

	select {
	case err := <-entry.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flush sends all pending entries in groups of up to 10, splitting into multiple
// requests if more than 10 entries accumulated between flush triggers.
func (b *batcher) flush(ctx context.Context) {
	b.mu.Lock()
	if b.timer != nil {
		b.timer.Stop()
	}
	entries := b.pending
	b.pending = nil
	b.mu.Unlock()

	for len(entries) > 0 {
		n := min(len(entries), 10)
		batch := entries[:n]
		entries = entries[n:]
		b.flushFn(ctx, batch)
	}
}

// closeAndFlush flushes any remaining pending entries; used on graceful shutdown.
func (b *batcher) closeAndFlush(ctx context.Context) {
	b.flush(ctx)
}

func (b *batcher) newID() string {
	n := atomic.AddUint64(&b.nextID, 1)
	return strconv.FormatUint(n, 10)
}

// resultsFromError applies err to every entry, used when the batch request itself failed
// (as opposed to individual entries failing).
func resultsFromError(entries []batchEntry, err error) {
	for _, e := range entries {
		e.result <- err
	}
}

func batchEntryError(code, message string) error {
	return fmt.Errorf("%s: %s", code, message)
}
