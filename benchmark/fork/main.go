// Command fork runs the SQS cost benchmark against this repository's fork of
// loafer-go (with opt-in delete/visibility batching enabled), so its SQS API
// call counts can be compared against the upstream module (see ../upstream).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/justcodes/loafer-go/benchmark/counting"
	loafergo "github.com/justcodes/loafer-go/v2"
	loafersqs "github.com/justcodes/loafer-go/v2/aws/sqs"
)

func main() {
	endpoint := envOr("AWS_ENDPOINT", "http://localhost:4566")
	queueName := envOr("QUEUE_NAME", "bench-fork")
	messageCount := envIntOr("MESSAGE_COUNT", 500)

	ctx := context.Background()
	rawClient := newSQSClient(ctx, endpoint)

	queueURL := mustGetQueueURL(ctx, rawClient, queueName)
	purgeQueue(ctx, rawClient, queueURL)
	seedMessages(ctx, rawClient, queueURL, messageCount)

	countingClient := counting.New(rawClient)

	var processed atomic.Int64
	runCtx, cancel := context.WithCancel(ctx)

	handler := func(_ context.Context, _ loafergo.Message) error {
		if processed.Add(1) >= int64(messageCount) {
			// grace period so pending batched deletes/visibility extensions can flush
			// before the manager is torn down.
			go func() {
				time.Sleep(3 * time.Second)
				cancel()
			}()
		}
		return nil
	}

	route := loafersqs.NewRoute(
		&loafersqs.Config{
			SQSClient: countingClient,
			Handler:   handler,
			QueueName: queueName,
		},
		loafersqs.RouteWithMaxMessages(10),
		loafersqs.RouteWithWaitTimeSeconds(2),
		loafersqs.RouteWithWorkerPoolSize(10),
		loafersqs.RouteWithDeleteBatching(true),
		loafersqs.RouteWithDeleteBatchSize(10),
		loafersqs.RouteWithDeleteBatchInterval(50*time.Millisecond),
		loafersqs.RouteWithVisibilityBatching(true),
		loafersqs.RouteWithVisibilityBatchSize(10),
		loafersqs.RouteWithVisibilityBatchInterval(500*time.Millisecond),
	)

	manager := loafergo.NewManager(&loafergo.Config{Logger: loafergo.NoOpLogger{}})
	manager.RegisterRoute(route)

	start := time.Now()
	if err := manager.Run(runCtx); err != nil {
		log.Fatalf("manager run failed: %v", err)
	}
	elapsed := time.Since(start)

	printReport("fork (batching enabled)", messageCount, elapsed, countingClient.Counts())
}

func newSQSClient(ctx context.Context, endpoint string) *sqs.Client {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}
	cfg.BaseEndpoint = &endpoint
	return sqs.NewFromConfig(cfg)
}

func mustGetQueueURL(ctx context.Context, client *sqs.Client, queueName string) string {
	out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		log.Fatalf("get queue url %q: %v", queueName, err)
	}
	return *out.QueueUrl
}

func purgeQueue(ctx context.Context, client *sqs.Client, queueURL string) {
	_, err := client.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: &queueURL})
	if err != nil {
		log.Printf("purge queue (ignoring, likely rate limited): %v", err)
	}
	time.Sleep(1 * time.Second)
}

func seedMessages(ctx context.Context, client *sqs.Client, queueURL string, count int) {
	for start := 0; start < count; start += 10 {
		n := min(10, count-start)
		entries := make([]types.SendMessageBatchRequestEntry, 0, n)
		for i := 0; i < n; i++ {
			id := strconv.Itoa(start + i)
			body := fmt.Sprintf(`{"message":"hello world","seq":%d}`, start+i)
			entries = append(entries, types.SendMessageBatchRequestEntry{Id: &id, MessageBody: &body})
		}
		if _, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: &queueURL, Entries: entries}); err != nil {
			log.Fatalf("seed messages: %v", err)
		}
	}
}

func printReport(label string, messageCount int, elapsed time.Duration, c counting.Counts) {
	fmt.Printf("\n=== %s ===\n", label)
	fmt.Printf("messages:                      %d\n", messageCount)
	fmt.Printf("elapsed:                       %s\n", elapsed)
	fmt.Printf("GetQueueUrl calls:             %d\n", c.GetQueueUrl)
	fmt.Printf("ReceiveMessage calls:          %d\n", c.ReceiveMessage)
	fmt.Printf("DeleteMessage calls:           %d\n", c.DeleteMessage)
	fmt.Printf("DeleteMessageBatch calls:      %d\n", c.DeleteMessageBatch)
	fmt.Printf("ChangeMessageVisibility calls: %d\n", c.ChangeMessageVisibility)
	fmt.Printf("ChangeMessageVisibilityBatch calls: %d\n", c.ChangeMessageVisibilityBatch)
	fmt.Printf("total billed SQS API calls:    %d\n", c.Total())
	fmt.Printf("API calls per message:         %.3f\n", float64(c.Total())/float64(messageCount))

	// machine-readable line so run.sh can build a consolidated comparison at the end
	fmt.Printf(
		"RESULT|%s|%d|%.3f|%d|%d|%d|%d|%d|%d|%d|%.3f\n",
		label, messageCount, elapsed.Seconds(),
		c.GetQueueUrl, c.ReceiveMessage, c.DeleteMessage, c.DeleteMessageBatch,
		c.ChangeMessageVisibility, c.ChangeMessageVisibilityBatch,
		c.Total(), float64(c.Total())/float64(messageCount),
	)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
