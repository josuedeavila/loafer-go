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
	defaultExtensionLimit          = 2
	defaultVisibilityTimeout       = int32(30)
	defaultMaxMessages             = int32(10)
	defaultWaitTimeSeconds         = int32(10)
	defaultWorkerPoolSize          = int32(5)
	defaultDeleteBatchSize         = 10
	defaultDeleteBatchInterval     = 200 * time.Millisecond
	defaultVisibilityBatchSize     = 10
	defaultVisibilityBatchInterval = 500 * time.Millisecond
	maxBatchSize                   = 10
)

// RouteConfig are a discrete set of route options that are valid for loading the route configuration
type RouteConfig struct {
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
}

func loadDefaultRouteConfig() *RouteConfig {
	return &RouteConfig{
		visibilityTimeout:       defaultVisibilityTimeout,
		maxMessages:             defaultMaxMessages,
		extensionLimit:          defaultExtensionLimit,
		waitTimeSeconds:         defaultWaitTimeSeconds,
		workerPoolSize:          defaultWorkerPoolSize,
		runMode:                 loafergo.Parallel,
		batchDeleteEnabled:      false,
		deleteBatchSize:         defaultDeleteBatchSize,
		deleteBatchInterval:     defaultDeleteBatchInterval,
		batchVisibilityEnabled:  false,
		visibilityBatchSize:     defaultVisibilityBatchSize,
		visibilityBatchInterval: defaultVisibilityBatchInterval,
	}
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

// RouteWithDeleteBatching enables batching of DeleteMessage calls (Commit) using
// DeleteMessageBatch, reducing the number of SQS API requests at high throughput.
// Disabled by default; when enabled, use RouteWithDeleteBatchSize and
// RouteWithDeleteBatchInterval to tune the batching window.
func RouteWithDeleteBatching(enabled bool) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		rc.batchDeleteEnabled = enabled
	}
}

// RouteWithDeleteBatchSize sets the maximum number of entries per DeleteMessageBatch
// request. Values above the SQS hard limit (10) are clamped to 10.
func RouteWithDeleteBatchSize(v int) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		if v <= 0 || v > maxBatchSize {
			v = maxBatchSize
		}
		rc.deleteBatchSize = v
	}
}

// RouteWithDeleteBatchInterval sets the maximum time a delete request waits for its
// batch to fill up before being flushed on its own.
func RouteWithDeleteBatchInterval(d time.Duration) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		rc.deleteBatchInterval = d
	}
}

// RouteWithVisibilityBatching enables batching of ChangeMessageVisibility calls using
// ChangeMessageVisibilityBatch, reducing the number of SQS API requests for routes
// with many long-running or backed-off messages in flight.
// Disabled by default; when enabled, use RouteWithVisibilityBatchSize and
// RouteWithVisibilityBatchInterval to tune the batching window.
func RouteWithVisibilityBatching(enabled bool) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		rc.batchVisibilityEnabled = enabled
	}
}

// RouteWithVisibilityBatchSize sets the maximum number of entries per
// ChangeMessageVisibilityBatch request. Values above the SQS hard limit (10) are
// clamped to 10.
func RouteWithVisibilityBatchSize(v int) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		if v <= 0 || v > maxBatchSize {
			v = maxBatchSize
		}
		rc.visibilityBatchSize = v
	}
}

// RouteWithVisibilityBatchInterval sets how often the visibility scheduler groups
// due extensions into a batch request.
func RouteWithVisibilityBatchInterval(d time.Duration) LoadRouteConfigFunc {
	return func(rc *RouteConfig) {
		rc.visibilityBatchInterval = d
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
