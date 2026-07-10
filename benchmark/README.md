# SQS cost benchmark: fork vs upstream

Compares the number of billed SQS API calls (and wall-clock time) between:

- **fork** (`./fork`): this repository, with `RouteWithDeleteBatching` and
  `RouteWithVisibilityBatching` enabled.
- **upstream** (`./upstream`): the published `github.com/justcodes/loafer-go/v2`
  module (currently pinned to `v2.10.1`), unbatched.

Both programs run against LocalStack, seed the same number of messages into
their own queue, consume them with an equivalent route configuration
(`MaxMessages=10`, `WaitTimeSeconds=2`, `WorkerPoolSize=10`), and print a report
of how many times each SQS operation was called.

Since the two loafer-go versions share the same Go module path
(`github.com/justcodes/loafer-go/v2`), they can't be imported into a single
binary — that's why `fork/` and `upstream/` are separate Go modules, each with
its own `go.mod`. The `counting/` module has no dependency on loafer-go itself:
it just wraps the real `*sqs.Client` and counts calls per operation, so the
same type satisfies both modules' `SQSClient` interface.

## Running

```bash
./run.sh
# or, for a larger sample:
MESSAGE_COUNT=2000 ./run.sh
```

This starts LocalStack (`docker compose up`), waits for the two queues to be
created, and runs `fork` then `upstream` sequentially. Each run's full output
(including per-message logs, if any) goes to its own temp log file; only a
single consolidated comparison table is printed once both runs finish, and
LocalStack is torn down on exit.

## Example result (60 messages)

```
metric                                 fork (batched)        upstream
------------------------------------  ---------------  ---------------
messages                                           60               60
elapsed (s)                                      6.74             3.43
GetQueueUrl calls                                   1                1
ReceiveMessage calls                                8                8
DeleteMessage calls                                 0               60
DeleteMessageBatch calls                           19                0
ChangeMessageVisibility calls                       0               60
ChangeMessageVisibilityBatch calls                  6                0
total billed SQS API calls                         34              129
API calls per message                           0.567            2.150

95 fewer billed API calls with batching enabled (73.6% reduction)
```

If either run doesn't produce a result line (e.g. it crashed), `run.sh` dumps
both log files instead of the comparison table, for debugging.

~74% fewer billed API calls for this run. The handler is a no-op, so this
mainly exercises delete batching (the dominant cost at high throughput);
messages that are committed before the visibility scheduler's first tick skip
the visibility call entirely, which is why fork's `ChangeMessageVisibility(Batch)`
count is so low here. To also exercise the visibility-extension path more heavily,
add an artificial delay in the handler (in `fork/main.go` and `upstream/main.go`)
so messages stay in flight longer than the configured visibility timeout.

Wall-clock time is not the primary metric here — LocalStack's own latency
dominates it at this scale. The number of billed API calls is what maps
directly to AWS SQS cost.
