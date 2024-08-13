package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	loafergo "github.com/justcodes/loafer-go"
	loaferAWS "github.com/justcodes/loafer-go/aws"
)

// NewClient instantiates a new sqs client to be used on the sqs route
func NewClient(ctx context.Context, cfg *loaferAWS.ClientConfig) (client loafergo.SQSClient, err error) {
	cfg, err = loaferAWS.ValidateConfig(cfg)
	if err != nil {
		return nil, err
	}

	c := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(cfg.Config.Key, cfg.Config.Secret, ""))
	_, err = c.Retrieve(ctx)
	if err != nil {
		return client, loafergo.ErrInvalidCreds.Context(err)
	}

	aCfg, err := loaferAWS.LoadAWSConfig(ctx, cfg, c)
	if err != nil {
		return nil, err
	}
	client = sqs.NewFromConfig(aCfg)
	return
}
