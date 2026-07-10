package sqs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"

	loafergo "github.com/justcodes/loafer-go/v2"
)

func newTestMessage(receiptHandle string) *message {
	return &message{
		backoffChannel: make(chan time.Duration, 1),
		dispatched:     make(chan bool, 1),
		originalMessage: types.Message{
			Body:          aws.String("body"),
			ReceiptHandle: aws.String(receiptHandle),
		},
	}
}

func noopLogger() loafergo.Logger {
	return loafergo.LoggerFunc(func(args ...interface{}) {})
}

func TestVisibilitySchedulerInitialAndDoublingExtension(t *testing.T) {
	var mu sync.Mutex
	var timeouts []int32

	s := newVisibilityScheduler(50*time.Millisecond, 10, func(_ context.Context, _ loafergo.Logger, dues []visibilityDue) {
		mu.Lock()
		for _, d := range dues {
			timeouts = append(timeouts, d.timeout)
		}
		mu.Unlock()
	})

	m := newTestMessage("receipt-1")
	// visibilityTimeout=11 -> sleep = 11-10 = 1s; extensionLimit=1 -> one doubling extension after the initial call
	s.register(m, 11, 1)

	ctx := context.Background()
	go s.run(ctx, noopLogger())

	// initial extension should fire on the first tick (within ~50ms), well before the 1s ticker
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(timeouts) >= 1
	}, time.Second, 10*time.Millisecond)

	// the doubled extension should fire ~1s after registration, then the task is dropped
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(timeouts) >= 2
	}, 3*time.Second, 20*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []int32{11, 22}, timeouts)

	s.mu.Lock()
	defer s.mu.Unlock()
	assert.Empty(t, s.tasks, "task should be removed once extensionLimit is reached")
}

func TestVisibilitySchedulerCancel(t *testing.T) {
	var mu sync.Mutex
	var calls int

	s := newVisibilityScheduler(20*time.Millisecond, 10, func(_ context.Context, _ loafergo.Logger, dues []visibilityDue) {
		mu.Lock()
		calls += len(dues)
		mu.Unlock()
	})

	m := newTestMessage("receipt-1")
	s.register(m, 30, 2)
	s.cancel(m)

	s.tick(context.Background(), noopLogger())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 0, calls, "a cancelled message should never be flushed")
}

func TestVisibilitySchedulerSplitsLargeGroupsIntoBatches(t *testing.T) {
	var mu sync.Mutex
	var flushCalls int
	var totalEntries int

	s := newVisibilityScheduler(time.Hour, 2, func(_ context.Context, _ loafergo.Logger, dues []visibilityDue) {
		mu.Lock()
		flushCalls++
		totalEntries += len(dues)
		mu.Unlock()
	})

	for i := 0; i < 5; i++ {
		m := newTestMessage("receipt-" + string(rune('a'+i)))
		s.register(m, 30, 2)
	}

	s.tick(context.Background(), noopLogger())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 5, totalEntries)
	assert.Equal(t, 3, flushCalls, "5 entries with batchSize=2 should split into 3 requests")
}
