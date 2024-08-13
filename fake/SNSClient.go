// Code generated by mockery v2.41.0. DO NOT EDIT.

package fake

import (
	context "context"

	mock "github.com/stretchr/testify/mock"

	sns "github.com/aws/aws-sdk-go-v2/service/sns"
)

// SNSClient is an autogenerated mock type for the SNSClient type
type SNSClient struct {
	mock.Mock
}

// Publish provides a mock function with given fields: ctx, params, optFns
func (_m *SNSClient) Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
	_va := make([]interface{}, len(optFns))
	for _i := range optFns {
		_va[_i] = optFns[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, params)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Publish")
	}

	var r0 *sns.PublishOutput
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *sns.PublishInput, ...func(*sns.Options)) (*sns.PublishOutput, error)); ok {
		return rf(ctx, params, optFns...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *sns.PublishInput, ...func(*sns.Options)) *sns.PublishOutput); ok {
		r0 = rf(ctx, params, optFns...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*sns.PublishOutput)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *sns.PublishInput, ...func(*sns.Options)) error); ok {
		r1 = rf(ctx, params, optFns...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// PublishBatch provides a mock function with given fields: ctx, params, optFns
func (_m *SNSClient) PublishBatch(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	_va := make([]interface{}, len(optFns))
	for _i := range optFns {
		_va[_i] = optFns[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, params)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for PublishBatch")
	}

	var r0 *sns.PublishBatchOutput
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *sns.PublishBatchInput, ...func(*sns.Options)) (*sns.PublishBatchOutput, error)); ok {
		return rf(ctx, params, optFns...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *sns.PublishBatchInput, ...func(*sns.Options)) *sns.PublishBatchOutput); ok {
		r0 = rf(ctx, params, optFns...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*sns.PublishBatchOutput)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *sns.PublishBatchInput, ...func(*sns.Options)) error); ok {
		r1 = rf(ctx, params, optFns...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewSNSClient creates a new instance of SNSClient. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewSNSClient(t interface {
	mock.TestingT
	Cleanup(func())
}) *SNSClient {
	mock := &SNSClient{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
