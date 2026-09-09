package sqs

import (
	"context"
	"errors"
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

// newTestRoute builds a configured route backed by a fake client, plus a canceled-on-cleanup
// context so the batcher goroutine never outlives the test.
func newTestRoute(t *testing.T, queueName string, optFns ...func(*RouteConfig)) (*route, *fake.SQSClient) {
	t.Helper()

	client := new(fake.SQSClient)
	client.On("GetQueueUrl", mock.Anything, mock.Anything).
		Return(&awsSqs.GetQueueUrlOutput{QueueUrl: aws.String(testQueueURL)}, nil)

	r, ok := NewRoute(&Config{
		SQSClient: client,
		QueueName: queueName,
		Handler:   func(context.Context, loafergo.Message) error { return nil },
	}, optFns...).(*route)
	require.True(t, ok)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, r.Configure(ctx))
	t.Cleanup(func() {
		cancel()
		if r.batcher != nil {
			<-r.batcher.done
		}
	})

	return r, client
}

func receiveOutput(n int) *awsSqs.ReceiveMessageOutput {
	out := &awsSqs.ReceiveMessageOutput{}
	for i := 0; i < n; i++ {
		out.Messages = append(out.Messages, types.Message{
			ReceiptHandle: aws.String("handle-" + string(rune('a'+i))),
			Body:          aws.String(`{"Message":"body"}`),
			Attributes:    map[string]string{messageGroupIDAttr: "group-1"},
		})
	}
	return out
}

// Without the option the route must keep deleting one message per call, unchanged.
func TestRoute_DeleteBatchDisabledByDefault(t *testing.T) {
	r, client := newTestRoute(t, "test-queue")
	require.Nil(t, r.batcher)

	msg := testMessage("handle-a")
	client.On("DeleteMessage", mock.Anything, &awsSqs.DeleteMessageInput{
		QueueUrl:      aws.String(testQueueURL),
		ReceiptHandle: aws.String("handle-a"),
	}).Return(&awsSqs.DeleteMessageOutput{}, nil).Once()

	require.NoError(t, r.Commit(context.Background(), msg))
	client.AssertExpectations(t)
	client.AssertNotCalled(t, "DeleteMessageBatch", mock.Anything, mock.Anything)
	assert.True(t, dispatched(msg))
}

func TestRoute_DeleteBatchDisabledPropagatesTheDeleteError(t *testing.T) {
	r, client := newTestRoute(t, "test-queue")

	wantErr := errors.New("delete failed")
	client.On("DeleteMessage", mock.Anything, mock.Anything).
		Return(&awsSqs.DeleteMessageOutput{}, wantErr).Once()

	assert.ErrorIs(t, r.Commit(context.Background(), testMessage("handle-a")), wantErr)
}

// Commit must not block the worker and must not issue a single-message delete.
func TestRoute_CommitIsAsynchronousWhenBatchingIsEnabled(t *testing.T) {
	r, client := newTestRoute(t, "test-queue", RouteWithDeleteBatch(time.Hour))
	require.NotNil(t, r.batcher)

	client.On("DeleteMessageBatch", mock.Anything, mock.Anything).Return(
		func(_ context.Context, in *awsSqs.DeleteMessageBatchInput,
			_ ...func(*awsSqs.Options)) (*awsSqs.DeleteMessageBatchOutput, error) {
			return allSuccessful(in), nil
		})

	done := make(chan struct{})
	go func() {
		defer close(done)
		require.NoError(t, r.Commit(context.Background(), testMessage("handle-a")))
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Commit blocked while batching was enabled")
	}
	client.AssertNotCalled(t, "DeleteMessage", mock.Anything, mock.Anything)
}

// The whole point of the in-flight counter: a full receive cycle collapses into one call
// as soon as the last message is committed, without paying the linger.
func TestRoute_FlushesWhenTheReceiveCycleEnds(t *testing.T) {
	// an hour of linger proves the flush came from the in-flight counter, not the timer
	r, client := newTestRoute(t, "test-queue", RouteWithDeleteBatch(time.Hour))

	client.On("ReceiveMessage", mock.Anything, mock.Anything).Return(receiveOutput(3), nil).Once()

	batches := make(chan []types.DeleteMessageBatchRequestEntry, 4)
	client.On("DeleteMessageBatch", mock.Anything, mock.Anything).Return(
		func(_ context.Context, in *awsSqs.DeleteMessageBatchInput,
			_ ...func(*awsSqs.Options)) (*awsSqs.DeleteMessageBatchOutput, error) {
			batches <- in.Entries
			return allSuccessful(in), nil
		})

	ctx := context.Background()
	msgs, err := r.GetMessages(ctx, loafergo.NoOpLogger{})
	require.NoError(t, err)
	require.Len(t, msgs, 3)

	for _, m := range msgs {
		require.NoError(t, r.Commit(ctx, m))
	}

	select {
	case entries := <-batches:
		assert.Len(t, entries, 3)
	case <-time.After(time.Second):
		t.Fatal("the batch was not flushed when the receive cycle ended")
	}
	client.AssertNotCalled(t, "DeleteMessage", mock.Anything, mock.Anything)
}

// A handler error resolves the message too, so it must release the batch of its siblings.
func TestRoute_HandlerErrorReleasesThePendingBatch(t *testing.T) {
	r, client := newTestRoute(t, "test-queue", RouteWithDeleteBatch(time.Hour))
	wantErr := errors.New("handler failed")
	r.handler = func(context.Context, loafergo.Message) error { return wantErr }

	client.On("ReceiveMessage", mock.Anything, mock.Anything).Return(receiveOutput(2), nil).Once()

	batches := make(chan []types.DeleteMessageBatchRequestEntry, 4)
	client.On("DeleteMessageBatch", mock.Anything, mock.Anything).Return(
		func(_ context.Context, in *awsSqs.DeleteMessageBatchInput,
			_ ...func(*awsSqs.Options)) (*awsSqs.DeleteMessageBatchOutput, error) {
			batches <- in.Entries
			return allSuccessful(in), nil
		})

	ctx := context.Background()
	msgs, err := r.GetMessages(ctx, loafergo.NoOpLogger{})
	require.NoError(t, err)

	require.NoError(t, r.Commit(ctx, msgs[0]))
	assert.ErrorIs(t, r.HandlerMessage(ctx, msgs[1]), wantErr)

	select {
	case entries := <-batches:
		assert.Len(t, entries, 1, "only the committed message is deleted")
	case <-time.After(time.Second):
		t.Fatal("the pending batch was never flushed")
	}
}

func TestRoute_BackedOffMessageIsNeverDeleted(t *testing.T) {
	r, client := newTestRoute(t, "test-queue", RouteWithDeleteBatch(20*time.Millisecond))

	msg := testMessage("handle-a")
	msg.backedOff = true

	require.NoError(t, r.Commit(context.Background(), msg))
	time.Sleep(80 * time.Millisecond)

	client.AssertNotCalled(t, "DeleteMessage", mock.Anything, mock.Anything)
	client.AssertNotCalled(t, "DeleteMessageBatch", mock.Anything, mock.Anything)
}

// Once the batcher is gone Commit must still delete the message, synchronously.
func TestRoute_FallsBackToSingleDeleteAfterShutdown(t *testing.T) {
	client := new(fake.SQSClient)
	client.On("GetQueueUrl", mock.Anything, mock.Anything).
		Return(&awsSqs.GetQueueUrlOutput{QueueUrl: aws.String(testQueueURL)}, nil)
	client.On("DeleteMessage", mock.Anything, mock.Anything).
		Return(&awsSqs.DeleteMessageOutput{}, nil).Once()

	r, ok := NewRoute(&Config{
		SQSClient: client,
		QueueName: "test-queue",
		Handler:   func(context.Context, loafergo.Message) error { return nil },
	}, RouteWithDeleteBatch(time.Hour)).(*route)
	require.True(t, ok)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, r.Configure(ctx))
	cancel()
	<-r.batcher.done

	require.NoError(t, r.Commit(context.Background(), testMessage("handle-a")))
	client.AssertExpectations(t)
}

func TestRoute_ClampsTheDeleteLinger(t *testing.T) {
	tests := map[string]struct {
		linger            time.Duration
		visibilityTimeout int32
		want              time.Duration
	}{
		"zero falls back to the default":        {0, 300, defaultDeleteLinger},
		"negative falls back to the default":    {-time.Second, 300, defaultDeleteLinger},
		"below the minimum is raised":           {time.Millisecond, 300, minDeleteLinger},
		"above the maximum is capped":           {time.Hour, 300, maxDeleteLinger},
		"capped to a quarter of the timeout":    {time.Hour, 12, 3 * time.Second},
		"within the visibility budget it stays": {2 * time.Second, 12, 2 * time.Second},
		"a valid value is preserved":            {200 * time.Millisecond, 300, 200 * time.Millisecond},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := clampDeleteLinger(tc.linger, tc.visibilityTimeout)
			assert.LessOrEqual(t, got, maxDeleteLinger)
			assert.GreaterOrEqual(t, got, minDeleteLinger)
			assert.Equal(t, tc.want, got)
		})
	}
}

// Dispatch is called from the batcher, the fallback path and the handler error path; a
// second send on the buffered channel would deadlock a worker forever.
func TestMessage_DispatchIsIdempotent(t *testing.T) {
	m := testMessage("handle-a")

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.Dispatch()
		m.Dispatch()
		m.Dispatch()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a repeated Dispatch blocked")
	}

	assert.True(t, dispatched(m))
	assert.False(t, dispatched(m), "the watchdog is signalled exactly once")
}

func TestMessage_IdentifierWithoutReceiptHandle(t *testing.T) {
	assert.Empty(t, testMessage("").Identifier())
}
