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

// visibilityTask tracks the extension state of a single in-flight message.
type visibilityTask struct {
	m              *message
	nextDue        time.Time
	sleep          time.Duration
	base           int32
	extension      int32
	count          int
	extensionLimit int
	initialSent    bool
}

// visibilityDue represents an extension that is ready to be sent, detached from its
// task so the flush call can happen without holding the scheduler lock.
type visibilityDue struct {
	receiptHandle string
	timeout       int32
}

// visibilityScheduler replaces one ticker-per-message with a single per-route ticker
// that periodically groups every message whose extension is due into
// ChangeMessageVisibilityBatch requests (up to batchSize entries each), instead of
// issuing one ChangeMessageVisibility call per message.
type visibilityScheduler struct {
	tasks     map[string]*visibilityTask
	flushFn   func(ctx context.Context, logger loafergo.Logger, dues []visibilityDue)
	interval  time.Duration
	batchSize int
	mu        sync.Mutex
}

func newVisibilityScheduler(
	interval time.Duration,
	batchSize int,
	flushFn func(ctx context.Context, logger loafergo.Logger, dues []visibilityDue),
) *visibilityScheduler {
	if batchSize <= 0 || batchSize > maxBatchSize {
		batchSize = maxBatchSize
	}

	return &visibilityScheduler{
		tasks:     make(map[string]*visibilityTask),
		interval:  interval,
		batchSize: batchSize,
		flushFn:   flushFn,
	}
}

// run starts the periodic scan; it blocks until ctx is done.
func (s *visibilityScheduler) run(ctx context.Context, logger loafergo.Logger) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx, logger)
		}
	}
}

// register schedules the initial visibility extension for m, to be sent on the next tick.
func (s *visibilityScheduler) register(m *message, visibilityTimeout int32, extensionLimit int) {
	sleep := time.Duration(visibilityTimeout-defaultVisibilityTimeoutControl) * time.Second

	s.mu.Lock()
	s.tasks[m.Identifier()] = &visibilityTask{
		m:              m,
		base:           visibilityTimeout,
		extension:      visibilityTimeout,
		extensionLimit: extensionLimit,
		sleep:          sleep,
		nextDue:        time.Now(),
	}
	s.mu.Unlock()
}

// cancel stops tracking m, e.g. once it has been dispatched or backed off.
func (s *visibilityScheduler) cancel(m *message) {
	s.mu.Lock()
	delete(s.tasks, m.Identifier())
	s.mu.Unlock()
}

func (s *visibilityScheduler) tick(ctx context.Context, logger loafergo.Logger) {
	now := time.Now()

	s.mu.Lock()
	var due []visibilityDue
	for key, task := range s.tasks {
		if task.nextDue.After(now) {
			continue
		}

		timeout := task.extension
		if !task.initialSent {
			task.initialSent = true
		} else {
			task.count++
			task.extension += task.base
			timeout = task.extension
		}

		due = append(due, visibilityDue{receiptHandle: task.m.Identifier(), timeout: timeout})

		if task.count >= task.extensionLimit {
			delete(s.tasks, key)
		} else {
			task.nextDue = now.Add(task.sleep)
		}
	}
	s.mu.Unlock()

	for len(due) > 0 {
		n := min(len(due), s.batchSize)
		s.flushFn(ctx, logger, due[:n])
		due = due[n:]
	}
}

// flushVisibilityBatch sends a group of pending visibility extensions as a single
// ChangeMessageVisibilityBatch request, logging the outcome of each entry the same way
// doChangeVisibilityTimeout does for the unbatched path.
func (r *route) flushVisibilityBatch(ctx context.Context, logger loafergo.Logger, dues []visibilityDue) {
	byID := make(map[string]visibilityDue, len(dues))
	reqEntries := make([]types.ChangeMessageVisibilityBatchRequestEntry, 0, len(dues))
	for i, d := range dues {
		id := strconv.Itoa(i)
		byID[id] = d
		reqEntries = append(reqEntries, types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                &id,
			ReceiptHandle:     &dues[i].receiptHandle,
			VisibilityTimeout: d.timeout,
		})
		logger.Log(fmt.Sprintf(
			"change_visibility_timeout; queueUrl: %s; receipt_hendler: %s; timeout: %ds",
			r.queueURL, d.receiptHandle, d.timeout,
		))
	}

	signalDone := func() {
		if done, ok := ctx.Value(DoneCtxKey{}).(chan bool); ok {
			done <- true
		}
	}

	out, err := r.sqs.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: &r.queueURL,
		Entries:  reqEntries,
	})
	if err != nil {
		for _, d := range dues {
			logger.Log(fmt.Sprintf(
				"change_visibility_timeout_error: %v; queueUrl: %s; receipt_hendler: %s; timeout: %ds",
				err, r.queueURL, d.receiptHandle, d.timeout,
			))
			signalDone()
		}
		return
	}

	for range out.Successful {
		signalDone()
	}
	for _, f := range out.Failed {
		d := byID[*f.Id]
		logger.Log(fmt.Sprintf(
			"change_visibility_timeout_error: %v; queueUrl: %s; receipt_hendler: %s; timeout: %ds",
			batchEntryError(*f.Code, *f.Message), r.queueURL, d.receiptHandle, d.timeout,
		))
		signalDone()
	}
}
