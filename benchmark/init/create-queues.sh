#!/bin/bash
set -euo pipefail

ENDPOINT="http://localhost:4566"
REGION="us-east-1"
export AWS_ACCESS_KEY_ID=dummy
export AWS_SECRET_ACCESS_KEY=dummy
export AWS_DEFAULT_REGION="$REGION"

echo "Creating benchmark SQS queues..."
aws --endpoint-url="$ENDPOINT" --region "$REGION" sqs create-queue --queue-name "bench-fork" --output table
aws --endpoint-url="$ENDPOINT" --region "$REGION" sqs create-queue --queue-name "bench-upstream" --output table
echo "Benchmark queues ready."
