# CLAUDE.md - Development Guide

## Project Overview
`loafer-go` is a Go client library that coordinates worker pools for processing AWS SQS messages and publishing to AWS SNS. It supports parallel processing as well as FIFO processing per message group ID (`MessageGroupId`) combined with custom attributes (such as `seller_id`).

---

## Build & Test Commands

### Development Environment Setup
To configure your local environment and install development tools (`goimports`, `fieldalignment`, `golangci-lint`, `lefthook`):
```bash
make configure
```

### Formatting Code
Format codebase using `goimports` and optimize struct field alignments:
```bash
make format
```

### Linting
To check and run linter analysis:
```bash
# Formats code and then executes golangci-lint
make lint

# Or run golangci-lint directly (requires v2 configuration schema)
golangci-lint run ./...
```

### Running Tests
To run the full test suite with coverage and race-condition detection:
```bash
# Via Makefile (runs clean first, then go test with -race and coverage)
make test

# Direct Go command
go test -race -cover ./...
```

### Code Coverage Analysis
Generate a filtered coverage report excluding generated fakes and example files:
```bash
make cover
```

### Benchmarks
Run benchmark suites:
```bash
make test-bench
```

### Running the Example Application
The repository comes with a full example application showcasing typical producer and consumer usage patterns using LocalStack.
1. Spin up the AWS LocalStack container:
```bash
cd example
docker-compose up -d
```
2. Wait for the initialization script to configure SQS and SNS queues.
3. Run the example consumer and producer:
```bash
go run example/main.go
```

---

## Code Quality & Architecture Conventions

### Architectural Guidelines
- **Interface-First Design**: Decouple modules by utilizing the core interfaces declared in `interfaces.go` (`Router`, `SQSClient`, `Message`, `SNSClient`, `Logger`).
- **Context First**: Always propagate `context.Context` as the first argument in all exported functions and interface methods.
- **Concurrent-Safe and Thread-Safe**: Ensure all routes and managers handle worker pools safely using goroutines and thread-safe operations.
- **Run Modes**:
  - `Parallel`: Distributes incoming messages across workers randomly/via round-robin or as they become available.
  - `PerGroupID`: Dispatches messages having the same `MessageGroupId` and custom attribute grouping to the exact same worker thread sequentially, ensuring in-order processing.

### Directory Structure Guidelines
- `/` (Root): Main manager orchestration and interface declarations (`manager.go`, `interfaces.go`, `loafer.go`).
- `/aws`: AWS SQS/SNS configuration and implementation modules (`aws/config.go`).
  - `/aws/sns`: AWS SNS client/producer logic.
  - `/aws/sqs`: AWS SQS route client, receiver, and worker-pool logic.
- `/example`: Demonstration and local stack infrastructure.
- `/fake`: Mocks/fakes for interfaces, used to isolate unit tests.

### Testing & Mocking Standards
- Prefer external test files (`package loafergo_test` or `package sns_test`) to verify public API correctness, except when testing unexported logic.
- Mock all AWS dependencies or interfaces using the custom mocks under `fake/`.
- Ensure tests verify edge cases including: context cancellation, network retries, and worker concurrency.
- **Do not introduce flaky tests**: Mock time-dependent mechanisms using short delays or mock tickers if necessary.
- **No warnings/bypasses**: Do not ignore linting errors or use unsafe casts unless explicitly required.
- **Test file naming**: Match the Go convention `*_test.go` and sibling internal test naming `*_internal_test.go` if testing private package internals.
