#!/bin/bash
# Runs the SQS cost benchmark: seeds MESSAGE_COUNT messages into two LocalStack
# queues and consumes them once with the fork (batching enabled) and once with
# the published upstream module (no batching). Each run's full output is kept
# in its own log file; a single consolidated comparison is printed at the end.
set -euo pipefail

cd "$(dirname "$0")"

MESSAGE_COUNT="${MESSAGE_COUNT:-500}"
AWS_ENDPOINT="${AWS_ENDPOINT:-http://localhost:4566}"

FORK_LOG="$(mktemp)"
UPSTREAM_LOG="$(mktemp)"
trap 'rm -f "$FORK_LOG" "$UPSTREAM_LOG"' EXIT

echo "Starting LocalStack..."
docker compose up -d
trap 'echo "Stopping LocalStack..."; docker compose down -v; rm -f "$FORK_LOG" "$UPSTREAM_LOG"' EXIT

echo "Waiting for LocalStack queues to be ready..."
until docker logs loafer-benchmark-localstack 2>&1 | grep -q "Benchmark queues ready."; do
  sleep 1
done

echo
echo "Running fork (batching enabled)... (log: $FORK_LOG)"
(cd fork && MESSAGE_COUNT="$MESSAGE_COUNT" AWS_ENDPOINT="$AWS_ENDPOINT" QUEUE_NAME=bench-fork go run .) >"$FORK_LOG" 2>&1

echo "Running upstream (no batching)... (log: $UPSTREAM_LOG)"
(cd upstream && MESSAGE_COUNT="$MESSAGE_COUNT" AWS_ENDPOINT="$AWS_ENDPOINT" QUEUE_NAME=bench-upstream go run .) >"$UPSTREAM_LOG" 2>&1

fork_result="$(grep '^RESULT|' "$FORK_LOG" || true)"
upstream_result="$(grep '^RESULT|' "$UPSTREAM_LOG" || true)"

if [[ -z "$fork_result" || -z "$upstream_result" ]]; then
  echo "One of the runs did not produce a result line; dumping logs for debugging."
  echo "--- fork log ($FORK_LOG) ---"
  cat "$FORK_LOG"
  echo "--- upstream log ($UPSTREAM_LOG) ---"
  cat "$UPSTREAM_LOG"
  exit 1
fi

awk -F'|' -v fork="$fork_result" -v upstream="$upstream_result" '
BEGIN {
  split(fork, f, "|")
  split(upstream, u, "|")

  printf "\n%-36s %15s %15s\n", "metric", "fork (batched)", "upstream"
  printf "%-36s %15s %15s\n", "------------------------------------", "---------------", "---------------"
  printf "%-36s %15d %15d\n", "messages", f[3], u[3]
  printf "%-36s %15.2f %15.2f\n", "elapsed (s)", f[4], u[4]
  printf "%-36s %15d %15d\n", "GetQueueUrl calls", f[5], u[5]
  printf "%-36s %15d %15d\n", "ReceiveMessage calls", f[6], u[6]
  printf "%-36s %15d %15d\n", "DeleteMessage calls", f[7], u[7]
  printf "%-36s %15d %15d\n", "DeleteMessageBatch calls", f[8], u[8]
  printf "%-36s %15d %15d\n", "ChangeMessageVisibility calls", f[9], u[9]
  printf "%-36s %15d %15d\n", "ChangeMessageVisibilityBatch calls", f[10], u[10]
  printf "%-36s %15d %15d\n", "total billed SQS API calls", f[11], u[11]
  printf "%-36s %15.3f %15.3f\n", "API calls per message", f[12], u[12]

  reduction = 0
  if (u[11] > 0) {
    reduction = (1 - (f[11] / u[11])) * 100
  }
  printf "\n%d fewer billed API calls with batching enabled (%.1f%% reduction)\n", u[11] - f[11], reduction
}'
