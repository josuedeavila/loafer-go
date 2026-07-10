package sqs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	loafergo "github.com/justcodes/loafer-go/v2"
)

const (
	all                             = "All"
	defaultVisibilityTimeoutControl = 10
)

type route struct {
	sqs                     loafergo.SQSClient
	handler                 loafergo.Handler
	deleteBatcher           *batcher
	visibilityScheduler     *visibilityScheduler
	queueName               string
	queueURL                string
	customGroupFields       []string
	deleteBatchSize         int
	extensionLimit          int
	runMode                 loafergo.Mode
	visibilityBatchInterval time.Duration
	visibilityBatchSize     int
	deleteBatchInterval     time.Duration
	visibilityTimeout       int32
	workerPoolSize          int32
	waitTimeSeconds         int32
	maxMessages             int32
	batchDeleteEnabled      bool
	batchVisibilityEnabled  bool
	visibilitySchedulerOnce sync.Once
}

// DoneCtxKey is the context key for the done channel that is optionally passed to the router
// and used inside the doChangeVisibilityTimeout function to signal that the method finished its execution
type DoneCtxKey struct{}

// NewRoute creates a new Route
// By default, the new route will set the followed values:
//
// Visibility timeout: 30 seconds
// Max message: 10 unit
// Wait time: 10 seconds
//
// Use the Route method to modify these values.
// Example:
//
// sqs.NewRoute(
//
//		&sqs.Config{
//			SQSClient: sqsClient,
//			Handler:   handler1,
//			QueueName: "example-1",
//		},
//		sqs.RouteWithVisibilityTimeout(25),
//		sqs.RouteWithMaxMessages(5),
//		sqs.RouteWithWaitTimeSeconds(8),
//	)
func NewRoute(config *Config, optFns ...func(config *RouteConfig)) loafergo.Router {
	cfg := loadDefaultRouteConfig()
	for _, optFn := range optFns {
		optFn(cfg)
	}

	return &route{
		sqs:                     config.SQSClient,
		handler:                 config.Handler,
		queueName:               config.QueueName,
		extensionLimit:          cfg.extensionLimit,
		visibilityTimeout:       cfg.visibilityTimeout,
		maxMessages:             cfg.maxMessages,
		waitTimeSeconds:         cfg.waitTimeSeconds,
		workerPoolSize:          cfg.workerPoolSize,
		runMode:                 cfg.runMode,
		customGroupFields:       cfg.customGroupFields,
		batchDeleteEnabled:      cfg.batchDeleteEnabled,
		deleteBatchSize:         cfg.deleteBatchSize,
		deleteBatchInterval:     cfg.deleteBatchInterval,
		batchVisibilityEnabled:  cfg.batchVisibilityEnabled,
		visibilityBatchSize:     cfg.visibilityBatchSize,
		visibilityBatchInterval: cfg.visibilityBatchInterval,
	}
}

// Configure sets the queue url to route
func (r *route) Configure(ctx context.Context) error {
	err := r.checkRequiredFields()
	if err != nil {
		return err
	}

	o, err := r.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &r.queueName})
	if err != nil {
		return err
	}

	r.queueURL = *o.QueueUrl

	if r.batchDeleteEnabled {
		r.deleteBatcher = newBatcher(r.deleteBatchSize, r.deleteBatchInterval, r.flushDeleteBatch)
	}

	if r.batchVisibilityEnabled {
		r.visibilityScheduler = newVisibilityScheduler(r.visibilityBatchInterval, r.visibilityBatchSize, r.flushVisibilityBatch)
	}

	return nil
}

// Close flushes any pending batched deletes so a graceful shutdown does not hold
// unnecessary acks; safe to call even when batching is disabled.
func (r *route) Close(ctx context.Context) error {
	if r.deleteBatcher != nil {
		r.deleteBatcher.closeAndFlush(ctx)
	}
	return nil
}

// GetMessages gets messages from queue
func (r *route) GetMessages(ctx context.Context, logger loafergo.Logger) (messages []loafergo.Message, err error) {
	output, err := r.sqs.ReceiveMessage(
		ctx,
		&sqs.ReceiveMessageInput{
			QueueUrl:                    &r.queueURL,
			WaitTimeSeconds:             r.waitTimeSeconds,
			MaxNumberOfMessages:         r.maxMessages,
			MessageAttributeNames:       []string{all},
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		},
	)
	if err != nil {
		return
	}

	if r.batchVisibilityEnabled {
		r.visibilitySchedulerOnce.Do(func() {
			go r.visibilityScheduler.run(ctx, logger)
		})
	}

	for _, m := range output.Messages {
		msg := newMessage(m)
		messages = append(messages, msg)

		if r.batchVisibilityEnabled {
			r.visibilityScheduler.register(msg, r.visibilityTimeout, r.extensionLimit)
			go r.watchVisibilityLifecycle(ctx, msg, logger)
			continue
		}

		// change the message visibility
		go r.changeMessageVisibility(ctx, msg, logger)
	}

	return
}

// watchVisibilityLifecycle stops the visibility scheduler from tracking m once it is
// dispatched (committed) or reacts immediately to a Backoff call, mirroring the
// behavior of changeMessageVisibility's select loop without needing a per-message ticker.
func (r *route) watchVisibilityLifecycle(ctx context.Context, m *message, logger loafergo.Logger) {
	select {
	case d := <-m.backoffChannel:
		r.visibilityScheduler.cancel(m)
		r.doChangeVisibilityTimeout(ctx, m, int32(d.Seconds()), logger)
	case <-m.dispatched:
		r.visibilityScheduler.cancel(m)
	case <-ctx.Done():
		r.visibilityScheduler.cancel(m)
	}
}

// Commit deletes the message from the queue
func (r *route) Commit(ctx context.Context, m loafergo.Message) error {
	// if the handler backed off the message, we should not delete it
	if m.BackedOff() {
		return nil
	}

	defer m.Dispatch()
	identifier := m.Identifier()

	if r.deleteBatcher != nil {
		return r.deleteBatcher.enqueue(ctx, identifier, nil)
	}

	_, err := r.sqs.DeleteMessage(
		ctx,
		&sqs.DeleteMessageInput{QueueUrl: &r.queueURL, ReceiptHandle: &identifier},
	)
	if err != nil {
		return err
	}
	return err
}

// flushDeleteBatch sends a group of pending deletes as a single DeleteMessageBatch request
// and reports the per-entry outcome back to each caller blocked on Commit.
func (r *route) flushDeleteBatch(ctx context.Context, entries []batchEntry) {
	byID := make(map[string]batchEntry, len(entries))
	reqEntries := make([]types.DeleteMessageBatchRequestEntry, 0, len(entries))
	for _, e := range entries {
		byID[e.id] = e
		reqEntries = append(reqEntries, types.DeleteMessageBatchRequestEntry{
			Id:            &e.id,
			ReceiptHandle: &e.receiptHandle,
		})
	}

	out, err := r.sqs.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: &r.queueURL,
		Entries:  reqEntries,
	})
	if err != nil {
		resultsFromError(entries, err)
		return
	}

	for _, s := range out.Successful {
		if e, ok := byID[*s.Id]; ok {
			e.result <- nil
			delete(byID, *s.Id)
		}
	}
	for _, f := range out.Failed {
		if e, ok := byID[*f.Id]; ok {
			e.result <- batchEntryError(*f.Code, *f.Message)
			delete(byID, *f.Id)
		}
	}
	// defensive: any entry left unaccounted for gets an error so its caller never blocks forever
	for _, e := range byID {
		e.result <- fmt.Errorf("delete_message_batch: no result returned for id %s", e.id)
	}
}

// HandlerMessage consumes the message from the queue
func (r *route) HandlerMessage(ctx context.Context, msg loafergo.Message) error {
	err := r.handler(ctx, msg)
	if err != nil {
		msg.Dispatch()
		return err
	}
	return nil
}

// WorkerPoolSize returns the router worker pool size
func (r *route) WorkerPoolSize(ctx context.Context) int32 {
	return r.workerPoolSize
}

// VisibilityTimeout returns the router visibility timeout
func (r *route) VisibilityTimeout(ctx context.Context) int32 {
	return r.visibilityTimeout
}

// RunMode returns the router run mode
func (r *route) RunMode(ctx context.Context) loafergo.Mode {
	return r.runMode
}

// CustomGroupFields returns the router custom group fields
func (r *route) CustomGroupFields(ctx context.Context) []string {
	return r.customGroupFields
}

func (r *route) changeMessageVisibility(ctx context.Context, m *message, logger loafergo.Logger) {
	var count int
	extension := r.visibilityTimeout
	sleepTime := time.Duration(r.visibilityTimeout-defaultVisibilityTimeoutControl) * time.Second
	ticker := time.NewTicker(sleepTime)
	defer ticker.Stop()

	r.doChangeVisibilityTimeout(ctx, m, extension, logger)

	for {
		// only allow extensionLimit extension (Default 1m30s)
		if count >= r.extensionLimit {
			break
		}

		select {
		case d := <-m.backoffChannel:
			r.doChangeVisibilityTimeout(ctx, m, int32(d.Seconds()), logger)
			return
		case <-m.dispatched:
			return
		case <-ticker.C:
			count++
			// double the allowed processing time
			extension += r.visibilityTimeout
			r.doChangeVisibilityTimeout(ctx, m, extension, logger)
		}
	}
}

func (r *route) doChangeVisibilityTimeout(ctx context.Context, m *message, timeout int32, logger loafergo.Logger) {
	if timeout < 0 {
		timeout = 0
	}

	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html
	maxLimit := int32((12 * time.Hour).Seconds())
	if timeout > maxLimit {
		timeout = maxLimit
	}

	logMsg := fmt.Sprintf(
		"change_visibility_timeout; queueUrl: %s; receipt_hendler: %s; timeout: %ds",
		r.queueURL, *m.originalMessage.ReceiptHandle, timeout,
	)
	logger.Log(logMsg)
	_, err := r.sqs.ChangeMessageVisibility(
		ctx,
		&sqs.ChangeMessageVisibilityInput{
			QueueUrl:          &r.queueURL,
			ReceiptHandle:     m.originalMessage.ReceiptHandle,
			VisibilityTimeout: timeout,
		},
	)
	if err != nil {
		logMsg = fmt.Sprintf(
			"change_visibility_timeout_error: %v; queueUrl: %s; receipt_hendler: %s; timeout: %ds",
			err, r.queueURL, *m.originalMessage.ReceiptHandle, timeout,
		)
		logger.Log(logMsg)
	}

	done, ok := ctx.Value(DoneCtxKey{}).(chan bool)
	if ok {
		done <- true
	}
}

func (r *route) checkRequiredFields() error {
	if r.sqs == nil {
		return loafergo.ErrNoSQSClient
	}

	if r.handler == nil {
		return loafergo.ErrNoHandler
	}
	return nil
}
