//go:build integration

package sqs_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"

	loafergo "github.com/justcodes/loafer-go/v2"
	loafersqs "github.com/justcodes/loafer-go/v2/aws/sqs"
)

// countingSQSClient wraps a real SQSClient and records every ChangeMessageVisibility
// call by receipt handle, delegating everything else untouched.
type countingSQSClient struct {
	loafergo.SQSClient
	mu    sync.Mutex
	calls map[string]int
}

func newCountingSQSClient(c loafergo.SQSClient) *countingSQSClient {
	return &countingSQSClient{SQSClient: c, calls: make(map[string]int)}
}

func (c *countingSQSClient) ChangeMessageVisibility(
	ctx context.Context,
	params *sqs.ChangeMessageVisibilityInput,
	optFns ...func(*sqs.Options),
) (*sqs.ChangeMessageVisibilityOutput, error) {
	c.mu.Lock()
	c.calls[*params.ReceiptHandle]++
	c.mu.Unlock()
	return c.SQSClient.ChangeMessageVisibility(ctx, params, optFns...)
}

func (c *countingSQSClient) count(receiptHandle string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[receiptHandle]
}

// TestIntegrationVisibilityCallsAgainstLocalStack drives real messages through a route backed by
// a LocalStack SQS queue, using handlers with a random execution time, and reports how
// many real ChangeMessageVisibility calls each message triggered. It exists to validate
// end-to-end, against the real SQS API surface, that visibility is only extended when a
// message is still being processed - not for every message received.
//
// Requires LocalStack running locally:
//
//	docker compose -f docker-compose.integration.yml up -d
//	go test -tags=integration -race -run TestIntegrationVisibilityCallsAgainstLocalStack ./aws/sqs/... -v
func TestIntegrationVisibilityCallsAgainstLocalStack(t *testing.T) {
	const (
		messageCount      = 20
		maxMessages       = int32(10) // SQS caps ReceiveMessage at 10 per call
		visibilityTimeout = int32(11) // sleepTime = visibilityTimeout - 10 = 1s
		sleepTime         = 1 * time.Second
		maxHandlerSleep   = 2200 * time.Millisecond
		margin            = 400 * time.Millisecond // avoids flaky asserts right at tick boundaries
	)

	ctx := context.Background()
	endpoint := envOr("AWS_ENDPOINT", "http://localhost:4566")

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)
	awsCfg.BaseEndpoint = aws.String(endpoint)

	rawClient := sqs.NewFromConfig(awsCfg)

	queueName := fmt.Sprintf("visibility-load-test-%d", time.Now().UnixNano())
	createOut, err := rawClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queueName})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl
	t.Cleanup(func() {
		_, _ = rawClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: queueURL})
	})

	durationByBody := make(map[string]time.Duration, messageCount)
	for i := 0; i < messageCount; i++ {
		body := fmt.Sprintf("message-%d", i)
		durationByBody[body] = rand.N(maxHandlerSleep)

		_, err := rawClient.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    queueURL,
			MessageBody: aws.String(body),
		})
		require.NoError(t, err)
	}

	countingClient := newCountingSQSClient(rawClient)

	handler := func(ctx context.Context, m loafergo.Message) error {
		time.Sleep(durationByBody[string(m.Body())])
		return nil
	}

	route := loafersqs.NewRoute(&loafersqs.Config{
		SQSClient: countingClient,
		Handler:   handler,
		QueueName: queueName,
	},
		loafersqs.RouteWithVisibilityTimeout(visibilityTimeout),
		loafersqs.RouteWithMaxMessages(maxMessages),
		loafersqs.RouteWithWaitTimeSeconds(2),
	)
	require.NoError(t, route.Configure(ctx))

	allMessages := make([]loafergo.Message, 0, messageCount)
	deadline := time.Now().Add(20 * time.Second)
	for len(allMessages) < messageCount && time.Now().Before(deadline) {
		batch, err := route.GetMessages(ctx, loafergo.NoOpLogger{})
		require.NoError(t, err)
		allMessages = append(allMessages, batch...)
	}
	require.Len(t, allMessages, messageCount)

	var wg sync.WaitGroup
	for _, msg := range allMessages {
		msg := msg
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = route.HandlerMessage(ctx, msg)
			_ = route.Commit(ctx, msg)
		}()
	}
	wg.Wait()

	totalCalls := 0
	fmt.Println("\nbody           duration        visibility_calls")
	for _, msg := range allMessages {
		body := string(msg.Body())
		d := durationByBody[body]
		calls := countingClient.count(msg.Identifier())
		totalCalls += calls
		fmt.Printf("%-14s %-15s %d\n", body, d, calls)

		switch {
		case d < sleepTime-margin:
			if calls != 0 {
				t.Errorf("message %s: duration %s finished well before the first tick, expected 0 visibility calls, got %d", body, d, calls)
			}
		case d > sleepTime+margin && d < 2*sleepTime-margin:
			if calls < 1 {
				t.Errorf("message %s: duration %s crossed one tick, expected at least 1 visibility call, got %d", body, d, calls)
			}
		case d > 2*sleepTime+margin:
			if calls < 2 {
				t.Errorf("message %s: duration %s crossed two ticks, expected at least 2 visibility calls, got %d", body, d, calls)
			}
		}
	}

	fmt.Printf("\nmessages=%d totalVisibilityCalls=%d avgCallsPerMessage=%.2f\n",
		messageCount, totalCalls, float64(totalCalls)/float64(messageCount))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
