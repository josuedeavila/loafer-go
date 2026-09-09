package sqs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	loafergo "github.com/justcodes/loafer-go/v2"
	"github.com/justcodes/loafer-go/v2/fake"
)

const testQueueURL = "http://localhost:4566/000000000000/test-queue"

// responder decides what the fake client answers for a given call, 0 indexed.
type responder func(call int, in *awsSqs.DeleteMessageBatchInput) (*awsSqs.DeleteMessageBatchOutput, error)

type harness struct {
	t       *testing.T
	client  *fake.SQSClient
	batcher *deleteBatcher
	cancel  context.CancelFunc
	entries [][]types.DeleteMessageBatchRequestEntry
	liveCtx []bool
	mu      sync.Mutex
}

func newHarness(t *testing.T, linger time.Duration, capacity int, respond responder) *harness {
	t.Helper()

	h := &harness{t: t, client: new(fake.SQSClient)}
	h.client.On("DeleteMessageBatch", mock.Anything, mock.Anything).Return(
		func(ctx context.Context, in *awsSqs.DeleteMessageBatchInput,
			_ ...func(*awsSqs.Options)) (*awsSqs.DeleteMessageBatchOutput, error) {
			h.mu.Lock()
			call := len(h.entries)
			h.entries = append(h.entries, in.Entries)
			h.liveCtx = append(h.liveCtx, ctx.Err() == nil)
			h.mu.Unlock()
			return respond(call, in)
		})

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.batcher = newDeleteBatcher(h.client, loafergo.NoOpLogger{}, testQueueURL, linger, capacity)
	go h.batcher.run(ctx)
	t.Cleanup(func() {
		cancel()
		<-h.batcher.done
	})
	return h
}

func (h *harness) calls() [][]types.DeleteMessageBatchRequestEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([][]types.DeleteMessageBatchRequestEntry, len(h.entries))
	copy(out, h.entries)
	return out
}

func (h *harness) waitForCalls(n int) {
	h.t.Helper()
	require.Eventually(h.t, func() bool { return len(h.calls()) >= n }, time.Second, 2*time.Millisecond)
}

// enqueue pushes n messages, marking the last one as the end of the receive cycle when
// flushLast is set.
func (h *harness) enqueue(n int, flushLast bool) []*message {
	h.t.Helper()
	msgs := make([]*message, 0, n)
	for i := 0; i < n; i++ {
		m := testMessage("handle-" + string(rune('a'+i)))
		msgs = append(msgs, m)
		require.True(h.t, h.batcher.enqueue(deleteItem{msg: m, flush: flushLast && i == n-1}))
	}
	return msgs
}

func testMessage(handle string) *message {
	m := types.Message{Body: aws.String(`{"Message":"body"}`)}
	if handle != "" {
		m.ReceiptHandle = aws.String(handle)
	}
	return newMessage(m)
}

// dispatched reports whether Dispatch was already called. It consumes the signal, so it
// must be called at most once per message.
func dispatched(m *message) bool {
	select {
	case <-m.dispatched:
		return true
	default:
		return false
	}
}

func allSuccessful(in *awsSqs.DeleteMessageBatchInput) *awsSqs.DeleteMessageBatchOutput {
	out := &awsSqs.DeleteMessageBatchOutput{}
	for _, e := range in.Entries {
		out.Successful = append(out.Successful, types.DeleteMessageBatchResultEntry{Id: e.Id})
	}
	return out
}

func succeedAll(_ int, in *awsSqs.DeleteMessageBatchInput) (*awsSqs.DeleteMessageBatchOutput, error) {
	return allSuccessful(in), nil
}

func entryIDs(entries []types.DeleteMessageBatchRequestEntry) []string {
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, *e.Id)
	}
	return ids
}

func entryHandles(entries []types.DeleteMessageBatchRequestEntry) []string {
	handles := make([]string, 0, len(entries))
	for _, e := range entries {
		handles = append(handles, *e.ReceiptHandle)
	}
	return handles
}

func TestDeleteBatcher_FlushesWhenBatchIsFull(t *testing.T) {
	h := newHarness(t, time.Hour, 32, succeedAll)

	msgs := h.enqueue(maxDeleteBatchSize, false)
	h.waitForCalls(1)

	calls := h.calls()
	require.Len(t, calls, 1)
	assert.Len(t, calls[0], maxDeleteBatchSize)
	// Ids only need to be unique within the request.
	assert.Equal(t, []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}, entryIDs(calls[0]))
	assert.Equal(t, testQueueURL, h.batcher.queueURL)

	for _, m := range msgs {
		assert.True(t, dispatched(m))
	}
}

func TestDeleteBatcher_FlushesWhenLingerExpires(t *testing.T) {
	h := newHarness(t, 20*time.Millisecond, 32, succeedAll)

	msgs := h.enqueue(3, false)
	h.waitForCalls(1)

	calls := h.calls()
	require.Len(t, calls, 1)
	assert.Len(t, calls[0], 3)
	for _, m := range msgs {
		assert.True(t, dispatched(m))
	}
}

// The end of a receive cycle flushes right away, so the common case pays no linger at all.
func TestDeleteBatcher_FlushesOnLastInFlightMessage(t *testing.T) {
	h := newHarness(t, time.Hour, 32, succeedAll)

	h.enqueue(3, true)
	h.waitForCalls(1)

	calls := h.calls()
	require.Len(t, calls, 1)
	assert.Len(t, calls[0], 3)
}

// Dispatch stops the visibility watchdog, so it must not happen while the delete is still
// only queued - otherwise a pending message could become visible again.
func TestDeleteBatcher_DispatchIsDeferredUntilTheDeleteResolves(t *testing.T) {
	release := make(chan struct{})
	h := newHarness(t, time.Hour, 32, func(_ int, in *awsSqs.DeleteMessageBatchInput) (
		*awsSqs.DeleteMessageBatchOutput, error) {
		<-release
		return allSuccessful(in), nil
	})

	msgs := h.enqueue(maxDeleteBatchSize, false)
	h.waitForCalls(1)

	for _, m := range msgs {
		select {
		case <-m.dispatched:
			t.Fatal("message was dispatched before the delete resolved")
		default:
		}
	}

	close(release)
	for _, m := range msgs {
		select {
		case <-m.dispatched:
		case <-time.After(time.Second):
			t.Fatal("message was never dispatched after the delete resolved")
		}
	}
}

func TestDeleteBatcher_SplitsIntoRequestsOfTen(t *testing.T) {
	h := newHarness(t, 20*time.Millisecond, 64, succeedAll)

	// 23 messages with a huge buffer: two full batches flush on size, the remainder on linger.
	h.enqueue(23, false)
	h.waitForCalls(3)

	calls := h.calls()
	require.Len(t, calls, 3)
	assert.Len(t, calls[0], maxDeleteBatchSize)
	assert.Len(t, calls[1], maxDeleteBatchSize)
	assert.Len(t, calls[2], 3)
}

func TestDeleteBatcher_SenderFaultIsNotRetried(t *testing.T) {
	h := newHarness(t, time.Hour, 32, func(_ int, in *awsSqs.DeleteMessageBatchInput) (
		*awsSqs.DeleteMessageBatchOutput, error) {
		out := allSuccessful(in)
		out.Successful = out.Successful[1:]
		out.Failed = []types.BatchResultErrorEntry{{
			Id:          in.Entries[0].Id,
			Code:        aws.String("ReceiptHandleIsInvalid"),
			Message:     aws.String("invalid handle"),
			SenderFault: true,
		}}
		return out, nil
	})

	msgs := h.enqueue(maxDeleteBatchSize, false)
	h.waitForCalls(1)

	time.Sleep(50 * time.Millisecond)
	assert.Len(t, h.calls(), 1, "a sender fault must not be retried")
	// Every message reaches a terminal state, including the failed one.
	for _, m := range msgs {
		assert.True(t, dispatched(m))
	}
}

func TestDeleteBatcher_RetriesServerSideFailures(t *testing.T) {
	h := newHarness(t, time.Hour, 32, func(call int, in *awsSqs.DeleteMessageBatchInput) (
		*awsSqs.DeleteMessageBatchOutput, error) {
		if call > 0 {
			return allSuccessful(in), nil
		}
		out := allSuccessful(in)
		out.Successful = out.Successful[1:]
		out.Failed = []types.BatchResultErrorEntry{{
			Id:          in.Entries[0].Id,
			Code:        aws.String("InternalError"),
			SenderFault: false,
		}}
		return out, nil
	})

	msgs := h.enqueue(maxDeleteBatchSize, false)
	h.waitForCalls(2)

	calls := h.calls()
	require.Len(t, calls, 2)
	// only the failed entry is retried, re-indexed from zero
	require.Len(t, calls[1], 1)
	assert.Equal(t, []string{"0"}, entryIDs(calls[1]))
	assert.Equal(t, entryHandles(calls[0])[:1], entryHandles(calls[1]))

	for _, m := range msgs {
		assert.True(t, dispatched(m))
	}
}

func TestDeleteBatcher_StopsRetryingAfterTheLimit(t *testing.T) {
	h := newHarness(t, time.Hour, 32, func(_ int, in *awsSqs.DeleteMessageBatchInput) (
		*awsSqs.DeleteMessageBatchOutput, error) {
		out := &awsSqs.DeleteMessageBatchOutput{}
		for _, e := range in.Entries {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{
				Id: e.Id, Code: aws.String("InternalError"), SenderFault: false,
			})
		}
		return out, nil
	})

	msgs := h.enqueue(2, true)
	h.waitForCalls(1 + deleteRetryLimit)

	time.Sleep(100 * time.Millisecond)
	assert.Len(t, h.calls(), 1+deleteRetryLimit, "retries must be bounded")
	// Not deleted, but dispatched: the messages come back through the visibility timeout.
	for _, m := range msgs {
		assert.True(t, dispatched(m))
	}
}

func TestDeleteBatcher_RetriesWholeRequestErrors(t *testing.T) {
	h := newHarness(t, time.Hour, 32, func(call int, in *awsSqs.DeleteMessageBatchInput) (
		*awsSqs.DeleteMessageBatchOutput, error) {
		if call == 0 {
			return nil, errors.New("throttled")
		}
		return allSuccessful(in), nil
	})

	msgs := h.enqueue(2, true)
	h.waitForCalls(2)

	calls := h.calls()
	require.Len(t, calls, 2)
	assert.Len(t, calls[1], 2, "the whole request is retried")
	for _, m := range msgs {
		assert.True(t, dispatched(m))
	}
}

// Pending deletes must survive shutdown, otherwise the messages are redelivered.
func TestDeleteBatcher_FlushesPendingOnShutdown(t *testing.T) {
	h := newHarness(t, time.Hour, 32, succeedAll)

	msgs := h.enqueue(3, false)
	assert.Empty(t, h.calls())

	h.cancel()
	<-h.batcher.done

	calls := h.calls()
	require.Len(t, calls, 1)
	assert.Len(t, calls[0], 3)

	h.mu.Lock()
	live := h.liveCtx
	h.mu.Unlock()
	assert.Equal(t, []bool{true}, live, "the final flush must run on a context detached from the canceled one")

	for _, m := range msgs {
		assert.True(t, dispatched(m))
	}
}

func TestDeleteBatcher_EnqueueRefusesAfterShutdown(t *testing.T) {
	h := newHarness(t, time.Hour, 32, succeedAll)

	h.cancel()
	<-h.batcher.done

	assert.False(t, h.batcher.enqueue(deleteItem{msg: testMessage("late")}),
		"a closed batcher must refuse the message so the caller falls back to a single delete")
}

func TestDeleteBatcher_EnqueueRefusesWhenSaturatedWithoutBlocking(t *testing.T) {
	// no run goroutine: nothing drains the channel
	b := newDeleteBatcher(new(fake.SQSClient), loafergo.NoOpLogger{}, testQueueURL, time.Hour, 1)

	done := make(chan bool, 1)
	go func() {
		b.enqueue(deleteItem{msg: testMessage("first")})
		done <- b.enqueue(deleteItem{msg: testMessage("second")})
	}()

	select {
	case ok := <-done:
		assert.False(t, ok, "a saturated batcher must refuse rather than block the worker")
	case <-time.After(time.Second):
		t.Fatal("enqueue blocked on a saturated batcher")
	}
}

func TestDeleteBatcher_PokeWithNothingPendingIsANoop(t *testing.T) {
	h := newHarness(t, time.Hour, 32, succeedAll)

	h.batcher.poke()
	time.Sleep(50 * time.Millisecond)

	assert.Empty(t, h.calls())
	h.client.AssertNotCalled(t, "DeleteMessageBatch", mock.Anything, mock.Anything)
}

// A nil receipt handle would make AWS reject the whole request, taking the valid entries
// with it.
func TestDeleteBatcher_SkipsMessagesWithoutReceiptHandle(t *testing.T) {
	h := newHarness(t, time.Hour, 32, succeedAll)

	bad := testMessage("")
	good := testMessage("handle-ok")
	require.True(t, h.batcher.enqueue(deleteItem{msg: bad}))
	require.True(t, h.batcher.enqueue(deleteItem{msg: good, flush: true}))
	h.waitForCalls(1)

	calls := h.calls()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{"handle-ok"}, entryHandles(calls[0]))
	assert.True(t, dispatched(bad), "a message that cannot be deleted must still be dispatched")
	assert.True(t, dispatched(good))
}
