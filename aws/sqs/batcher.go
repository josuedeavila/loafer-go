package sqs

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	loafergo "github.com/justcodes/loafer-go/v2"
)

const (
	// maxDeleteBatchSize is the AWS hard limit of entries accepted by DeleteMessageBatch.
	// It matches the ReceiveMessage MaxNumberOfMessages ceiling, so a full receive batch
	// collapses into a single delete call.
	maxDeleteBatchSize = 10
	// defaultDeleteLinger is the linger applied when RouteWithDeleteBatch gets a non positive value.
	defaultDeleteLinger = 50 * time.Millisecond
	// minDeleteLinger and maxDeleteLinger bound the configured linger.
	minDeleteLinger = 10 * time.Millisecond
	maxDeleteLinger = 5 * time.Second
	// deleteRetryLimit is how many extra attempts a batch gets after a server side failure.
	deleteRetryLimit = 2
	// deleteRetryBackoff is the base delay between delete attempts.
	deleteRetryBackoff = 50 * time.Millisecond
	// shutdownFlushTimeout bounds the final flush performed when the route context is canceled.
	shutdownFlushTimeout = 5 * time.Second
	// messageGroupIDAttr is the FIFO group system attribute, used only for logging.
	messageGroupIDAttr = "MessageGroupId"
)

// deleteItem carries one message into the batcher. flush marks the message as the last
// in-flight one of a receive cycle, which triggers an immediate flush instead of waiting
// for the linger to expire.
type deleteItem struct {
	msg   loafergo.Message
	flush bool
}

// deleteBatcher groups DeleteMessage calls into DeleteMessageBatch requests.
//
// A single goroutine owns the pending batch, so no locking is needed around it. The mutex
// only guards the closed flag, which makes enqueue safe against a concurrent shutdown.
type deleteBatcher struct {
	sqs      loafergo.SQSClient
	logger   loafergo.Logger
	in       chan deleteItem
	pokeCh   chan struct{}
	done     chan struct{}
	queueURL string
	linger   time.Duration
	mu       sync.RWMutex
	closed   bool
}

func newDeleteBatcher(
	client loafergo.SQSClient,
	logger loafergo.Logger,
	queueURL string,
	linger time.Duration,
	capacity int,
) *deleteBatcher {
	return &deleteBatcher{
		sqs:      client,
		logger:   logger,
		queueURL: queueURL,
		linger:   linger,
		in:       make(chan deleteItem, capacity),
		pokeCh:   make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

// enqueue hands a message to the batcher. It never blocks: it reports false when the
// batcher is already shut down or its buffer is saturated, and the caller is then
// expected to fall back to a single DeleteMessage call.
func (b *deleteBatcher) enqueue(it deleteItem) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return false
	}

	select {
	case b.in <- it:
		return true
	default:
		return false
	}
}

// poke asks for an immediate flush of whatever is pending. It is used when a message
// leaves the route without being committed (handler error or backoff) and happens to be
// the last one in flight.
func (b *deleteBatcher) poke() {
	select {
	case b.pokeCh <- struct{}{}:
	default:
	}
}

// run owns the pending batch until ctx is canceled. It must be started exactly once.
func (b *deleteBatcher) run(ctx context.Context) {
	defer close(b.done)

	batch := make([]loafergo.Message, 0, maxDeleteBatchSize)
	timer := time.NewTimer(b.linger)
	stopTimer(timer)
	armed := false

	for {
		select {
		case <-ctx.Done():
			b.shutdown(ctx, batch)
			return
		case it := <-b.in:
			batch = append(batch, it.msg)
			if it.flush || len(batch) >= maxDeleteBatchSize {
				b.flushPending(ctx, batch)
				batch = batch[:0]
			}
		case <-b.pokeCh:
			b.flushPending(ctx, batch)
			batch = batch[:0]
		case <-timer.C:
			armed = false
			b.flushPending(ctx, batch)
			batch = batch[:0]
		}

		switch {
		case len(batch) > 0 && !armed:
			timer.Reset(b.linger)
			armed = true
		case len(batch) == 0 && armed:
			stopTimer(timer)
			armed = false
		}
	}
}

// shutdown drains whatever is still in flight and performs a last flush on a context
// detached from the canceled one, so pending deletes are not silently lost.
func (b *deleteBatcher) shutdown(ctx context.Context, batch []loafergo.Message) {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()

	// Every enqueue either observed closed and fell back to a single delete, or already
	// landed in the buffer and is picked up here. There is no third case.
	for draining := true; draining; {
		select {
		case it := <-b.in:
			batch = append(batch, it.msg)
		default:
			draining = false
		}
	}

	if len(batch) == 0 {
		return
	}

	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownFlushTimeout)
	defer cancel()
	b.flushPending(flushCtx, batch)
}

// flushPending deletes every pending message, splitting it into requests of at most
// maxDeleteBatchSize entries.
func (b *deleteBatcher) flushPending(ctx context.Context, batch []loafergo.Message) {
	for len(batch) > 0 {
		n := len(batch)
		if n > maxDeleteBatchSize {
			n = maxDeleteBatchSize
		}
		b.flush(ctx, batch[:n])
		batch = batch[n:]
	}
}

// flush issues a single DeleteMessageBatch, retrying the entries that failed for server
// side reasons. Every message reaches a terminal state - and is dispatched - before it
// returns, so the visibility watchdog is never left running.
func (b *deleteBatcher) flush(ctx context.Context, batch []loafergo.Message) {
	pending := b.discardInvalid(batch)

	for attempt := 0; len(pending) > 0; attempt++ {
		if attempt > 0 {
			b.backoff(ctx, attempt)
		}
		pending = b.deleteOnce(ctx, pending, attempt >= deleteRetryLimit)
	}
}

// deleteOnce performs one delete round and returns the messages worth retrying.
// When last is set nothing is returned: every message is resolved and dispatched.
func (b *deleteBatcher) deleteOnce(ctx context.Context, pending []loafergo.Message, last bool) []loafergo.Message {
	out, err := b.sqs.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: &b.queueURL,
		Entries:  buildDeleteEntries(pending),
	})
	if err != nil {
		if !last {
			return pending
		}
		b.logger.Log(fmt.Sprintf(
			"delete_message_batch_error: %v; queueUrl: %s; count: %d",
			err, b.queueURL, len(pending),
		))
		dispatchAll(pending)
		return nil
	}

	return b.resolve(out, pending, last)
}

// resolve dispatches the messages the batch settled and returns the retryable ones.
func (b *deleteBatcher) resolve(
	out *sqs.DeleteMessageBatchOutput,
	pending []loafergo.Message,
	last bool,
) []loafergo.Message {
	failed := make(map[int]types.BatchResultErrorEntry, len(out.Failed))
	for _, f := range out.Failed {
		if f.Id == nil {
			continue
		}
		i, err := strconv.Atoi(*f.Id)
		if err != nil || i < 0 || i >= len(pending) {
			continue
		}
		failed[i] = f
	}

	var retry []loafergo.Message
	for i, msg := range pending {
		f, bad := failed[i]
		switch {
		case !bad:
			msg.Dispatch()
		case f.SenderFault || last:
			// A sender fault (an invalid receipt handle, for instance) will not heal on a
			// retry. The message is left to the queue visibility timeout, as it already is
			// today when DeleteMessage fails.
			b.logEntryError(msg, f)
			msg.Dispatch()
		default:
			retry = append(retry, msg)
		}
	}

	return retry
}

// discardInvalid drops messages without a receipt handle, which AWS would reject for the
// whole request, taking the valid entries down with them.
func (b *deleteBatcher) discardInvalid(batch []loafergo.Message) []loafergo.Message {
	valid := make([]loafergo.Message, 0, len(batch))
	for _, msg := range batch {
		if msg.Identifier() == "" {
			b.logger.Log(fmt.Sprintf(
				"delete_message_batch_skipped: empty receipt handle; queueUrl: %s; group_id: %s",
				b.queueURL, msg.SystemAttributeByKey(messageGroupIDAttr),
			))
			msg.Dispatch()
			continue
		}
		valid = append(valid, msg)
	}
	return valid
}

func (b *deleteBatcher) backoff(ctx context.Context, attempt int) {
	timer := time.NewTimer(time.Duration(attempt) * deleteRetryBackoff)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// logEntryError mirrors the commit_message_error log emitted by the manager when Commit
// is synchronous, so switching to batch deletes does not lose observability.
func (b *deleteBatcher) logEntryError(msg loafergo.Message, f types.BatchResultErrorEntry) {
	b.logger.Log(fmt.Sprintf(
		"commit_message_error: %s %s; sender_fault: %t; queueUrl: %s; message: %s; group_id: %s; identifier: %s",
		deref(f.Code), deref(f.Message), f.SenderFault, b.queueURL,
		msg.Body(), msg.SystemAttributeByKey(messageGroupIDAttr), msg.Identifier(),
	))
}

func buildDeleteEntries(pending []loafergo.Message) []types.DeleteMessageBatchRequestEntry {
	entries := make([]types.DeleteMessageBatchRequestEntry, 0, len(pending))
	for i, msg := range pending {
		// Ids only need to be unique within the request, and the index is what maps a
		// result entry back to its message - sqs.Message does not expose the MessageId.
		id := strconv.Itoa(i)
		handle := msg.Identifier()
		entries = append(entries, types.DeleteMessageBatchRequestEntry{
			Id:            &id,
			ReceiptHandle: &handle,
		})
	}
	return entries
}

func dispatchAll(msgs []loafergo.Message) {
	for _, msg := range msgs {
		msg.Dispatch()
	}
}

func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
