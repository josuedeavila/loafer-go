package sqs

import (
	"strconv"
	"time"

	loafergo "github.com/justcodes/loafer-go/v2"
)

// A Config provides service configuration for SQS routes.
type Config struct {
	SQSClient loafergo.SQSClient
	Handler   loafergo.Handler
	QueueName string
}

const (
	defaultExtensionLimit    = 2
	defaultVisibilityTimeout = int32(30)
	defaultMaxMessages       = int32(10)
	defaultWaitTimeSeconds   = int32(10)
	defaultWorkerPoolSize    = int32(5)
	// deleteLingerVisibilityDivisor keeps the delete linger to a fraction of the route
	// visibility timeout, so a pending batch never eats the watchdog renewal budget.
	deleteLingerVisibilityDivisor = 4
)

// RouteConfig are a discrete set of route options that are valid for loading the route configuration
type RouteConfig struct {
	logger             loafergo.Logger
	customGroupFields  []string
	extensionLimit     int
	deleteLinger       time.Duration
	runMode            loafergo.Mode
	visibilityTimeout  int32
	maxMessages        int32
	waitTimeSeconds    int32
	workerPoolSize     int32
	deleteBatchEnabled bool
}

func loadDefaultRouteConfig() *RouteConfig {
	return &RouteConfig{
		visibilityTimeout: defaultVisibilityTimeout,
		maxMessages:       defaultMaxMessages,
		extensionLimit:    defaultExtensionLimit,
		waitTimeSeconds:   defaultWaitTimeSeconds,
		workerPoolSize:    defaultWorkerPoolSize,
		runMode:           loafergo.Parallel,
		logger:            loafergo.NoOpLogger{},
	}
}

// clampDeleteLinger bounds the configured linger to a safe window: never so short that it
// defeats the batching, never long enough to compromise the visibility timeout budget the
// watchdog works with, nor to stall a FIFO message group for a noticeable time.
func clampDeleteLinger(linger time.Duration, visibilityTimeout int32) time.Duration {
	if linger <= 0 {
		linger = defaultDeleteLinger
	}
	if linger < minDeleteLinger {
		linger = minDeleteLinger
	}
	if linger > maxDeleteLinger {
		linger = maxDeleteLinger
	}
	budget := time.Duration(visibilityTimeout) * time.Second / deleteLingerVisibilityDivisor
	if linger > budget {
		linger = budget
	}
	return linger
}

// LoadRouteConfigFunc is a type alias for RouteConfig functional config
type LoadRouteConfigFunc func(config *RouteConfig)

// RouteWithVisibilityTimeout is a helper function to construct functional options that sets visibility Timeout value
// on config's Route. If multiple RouteWithVisibilityTimeout calls are made,
// the last call overrides the previous call values.
//
// The minimum value is 11 seconds (defaultVisibilityTimeoutControl + 1)
// This value is used to extend the visibility timeout of the message
//
//	to avoid other consumers from consuming this message while it is being processed.
//
// It will extend it periodically based on the visibility timeout value provided,
// and at each iteration the sleep time will be doubled.
//
// For example,
//
//   - queue visibility timeout = 60 seconds (value defined in aws)
//   - route visibility timeout = 30 seconds
//   - time to process the message = 70 seconds
//   - sleep time = 20 seconds (30 seconds - defaultVisibilityTimeoutControl)
//
// --------------------------------------
//   - sleep 20s
//
// 1st iteration:
//
//   - change visibility timeout = 30 seconds
//
//   - sleep 20s
//
// 2nd iteration:
//
//   - change visibility timeout = 60 seconds
//
//   - handler finishes processing the message
//
//   - error handling the message? Message does not get deleted, and the queue visibility timeout (60s) is used (default aws sqs behavior)
//
//   - success handling the message? Message gets deleted
//
// end
func RouteWithVisibilityTimeout(v int32) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		if v <= defaultVisibilityTimeoutControl {
			rc.visibilityTimeout = defaultVisibilityTimeoutControl + 1
			return
		}
		rc.visibilityTimeout = v
	}
}

// RouteWithMaxMessages is a helper function to construct functional options that sets Max Messages value
// on config's Route. If multiple RouteWithMaxMessages calls are made,
// the last call overrides the previous call values.
func RouteWithMaxMessages(v int32) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		rc.maxMessages = v
	}
}

// RouteWithWaitTimeSeconds is a helper function to construct functional options that sets Wait Time Seconds value
// on config's Route. If multiple RouteWithWaitTimeSeconds calls are made,
// the last call overrides the previous call values.
func RouteWithWaitTimeSeconds(v int32) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		rc.waitTimeSeconds = v
	}
}

// RouteWithWorkerPoolSize is a helper function to construct functional options that sets Worker Pool Size value
// on config's Route. If multiple RouteWithWorkerPoolSize calls are made,
// the last call overrides the previous call values.
func RouteWithWorkerPoolSize(v int32) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		rc.workerPoolSize = v
	}
}

// RouteWithRunMode sets the routing mode for message processing.
//
// It returns a LoadRouteConfigFunc that updates the RouteConfig with the given Mode.
// This controls how SQS messages are dispatched to workers—either fully in parallel (Parallel)
// or grouped by MessageGroupId and custom fields (PerGroupID).
//
// The default mode is Parallel.
func RouteWithRunMode(v loafergo.Mode) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		rc.runMode = v
	}
}

// RouteWithCustomGroupFields sets the custom group fields used for message routing
// when the run mode is set to PerGroupID.
// These fields are extracted from the message body and appended to the MessageGroupId
// to generate a unique group key.
// This allows finer control over how messages are partitioned across workers.
func RouteWithCustomGroupFields(v []string) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		rc.customGroupFields = v
	}
}

// RouteWithDeleteBatch enables grouping message deletions into DeleteMessageBatch calls,
// cutting the number of delete API calls by up to 10x.
//
// With it enabled Commit becomes asynchronous: it hands the message over to the batcher
// and returns nil right away, so workers never block waiting on a delete. The outcome of
// the delete is reported through the logger set with RouteWithLogger.
//
// A batch is flushed as soon as it holds 10 messages - the AWS limit, which matches the
// MaxNumberOfMessages ceiling - or as soon as the last message of a receive cycle is
// committed. In the common case that means one DeleteMessageBatch per ReceiveMessage with
// no added latency at all; linger is only the safety net for when a slow handler is
// holding the batch back.
//
// A non positive linger falls back to 50ms. The value is clamped to [10ms, 5s] and to a
// quarter of the route visibility timeout.
//
// On FIFO queues a delayed delete holds the whole MessageGroupId, since SQS does not
// deliver the next message of a group while one is still in flight. Keep the linger small
// there, and note that handlers must be idempotent: a delete pending at crash time means
// the message is redelivered.
func RouteWithDeleteBatch(linger time.Duration) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		rc.deleteBatchEnabled = true
		rc.deleteLinger = linger
	}
}

// RouteWithLogger sets the logger the route uses to report delete batch failures.
// It defaults to loafergo.NoOpLogger, and normally receives the same logger given to the
// loafergo.Config driving the manager. A nil logger is ignored.
func RouteWithLogger(l loafergo.Logger) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		if l == nil {
			return
		}
		rc.logger = l
	}
}

// AWSConfig defines the loafer aws configuration
type AWSConfig struct {
	// private key to access aws
	Key string
	// secret to access aws
	Secret string
	// region for aws and used for determining the region
	Region string
	// profile for aws and used for determining the profile
	Profile string
	// provided automatically by aws, but must be set for emulators or local testing
	Hostname string
	// used to determine how many attempts exponential backoff should use before logging an error

	// Add custom attributes to the message. This might be a correlationId or client meta-information
	// custom attributes will be viewable on the sqs dashboard as metadata
	Attributes []CustomAttribute
}

// ClientConfig defines the loafer aws configuration
type ClientConfig struct {
	AwsConfig *AWSConfig
	// used to determine how many attempts exponential backoff should use before logging an error
	RetryCount int
}

// CustomAttribute add custom attributes to SNS and SQS messages.
// This can include correlationIds, or any additional information you would like
// separate from the payload body. These attributes can be easily seen from the SQS console.
type CustomAttribute struct {
	Title string
	// Use sqs.DataTypeNumber or sqs.DataTypeString
	DataType string
	// Value represents the value
	Value string
}

// NewCustomAttribute adds a custom attribute to SNS and SQS messages.
// This can include correlationIds, logIds, or any additional information you would like
// separate from the payload body. These attributes can be easily seen from the SQS console.
//
// Must use sqs.DataTypeNumber of sqs.DataTypeString for the datatype, the value must match the type provided
func (c *AWSConfig) NewCustomAttribute(dataType DataType, title string, value interface{}) error {
	if dataType == DataTypeNumber {
		val, ok := value.(int)
		if !ok {
			return loafergo.ErrMarshal
		}

		c.Attributes = append(c.Attributes, CustomAttribute{title, dataType.String(), strconv.Itoa(val)})
		return nil
	}

	val, ok := value.(string)
	if !ok {
		return loafergo.ErrMarshal
	}
	c.Attributes = append(c.Attributes, CustomAttribute{title, dataType.String(), val})
	return nil
}

// DataType is an alias to string
type DataType string

// String returns DataType as a string
func (dt DataType) String() string {
	return string(dt)
}

// DataTypeNumber represents the Number datatype, use it when creating custom attributes
const DataTypeNumber = DataType("Number")

// DataTypeString represents the String datatype, use it when creating custom attributes
const DataTypeString = DataType("String")
