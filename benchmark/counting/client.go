// Package counting wraps a real *sqs.Client and counts how many times each API
// operation is invoked, so a benchmark can compare the number of billed SQS
// requests between two configurations (e.g. batched vs unbatched deletes).
//
// It has no dependency on loafer-go itself: it only implements methods matching
// the aws-sdk-go-v2 sqs client, so the same type satisfies the loafergo.SQSClient
// interface of both the fork and the upstream module under benchmark.
package counting

import (
	"context"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Counts is a snapshot of how many times each SQS API operation was invoked.
type Counts struct {
	ReceiveMessage               int64
	DeleteMessage                int64
	DeleteMessageBatch           int64
	ChangeMessageVisibility      int64
	ChangeMessageVisibilityBatch int64
	GetQueueUrl                  int64
}

// Total returns the sum of every operation count, i.e. the total number of
// billed SQS API requests made through the client.
func (c Counts) Total() int64 {
	return c.ReceiveMessage + c.DeleteMessage + c.DeleteMessageBatch +
		c.ChangeMessageVisibility + c.ChangeMessageVisibilityBatch + c.GetQueueUrl
}

// Client wraps a real *sqs.Client, counting calls per operation.
type Client struct {
	inner *sqs.Client

	receiveMessage               atomic.Int64
	deleteMessage                atomic.Int64
	deleteMessageBatch           atomic.Int64
	changeMessageVisibility      atomic.Int64
	changeMessageVisibilityBatch atomic.Int64
	getQueueUrl                  atomic.Int64
}

// New wraps inner with call counters.
func New(inner *sqs.Client) *Client {
	return &Client{inner: inner}
}

// Counts returns a snapshot of the current call counts.
func (c *Client) Counts() Counts {
	return Counts{
		ReceiveMessage:               c.receiveMessage.Load(),
		DeleteMessage:                c.deleteMessage.Load(),
		DeleteMessageBatch:           c.deleteMessageBatch.Load(),
		ChangeMessageVisibility:      c.changeMessageVisibility.Load(),
		ChangeMessageVisibilityBatch: c.changeMessageVisibilityBatch.Load(),
		GetQueueUrl:                  c.getQueueUrl.Load(),
	}
}

func (c *Client) GetQueueUrl(
	ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options),
) (*sqs.GetQueueUrlOutput, error) {
	c.getQueueUrl.Add(1)
	return c.inner.GetQueueUrl(ctx, params, optFns...)
}

func (c *Client) ReceiveMessage(
	ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options),
) (*sqs.ReceiveMessageOutput, error) {
	c.receiveMessage.Add(1)
	return c.inner.ReceiveMessage(ctx, params, optFns...)
}

func (c *Client) DeleteMessage(
	ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options),
) (*sqs.DeleteMessageOutput, error) {
	c.deleteMessage.Add(1)
	return c.inner.DeleteMessage(ctx, params, optFns...)
}

func (c *Client) DeleteMessageBatch(
	ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options),
) (*sqs.DeleteMessageBatchOutput, error) {
	c.deleteMessageBatch.Add(1)
	return c.inner.DeleteMessageBatch(ctx, params, optFns...)
}

func (c *Client) ChangeMessageVisibility(
	ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options),
) (*sqs.ChangeMessageVisibilityOutput, error) {
	c.changeMessageVisibility.Add(1)
	return c.inner.ChangeMessageVisibility(ctx, params, optFns...)
}

func (c *Client) ChangeMessageVisibilityBatch(
	ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options),
) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	c.changeMessageVisibilityBatch.Add(1)
	return c.inner.ChangeMessageVisibilityBatch(ctx, params, optFns...)
}
