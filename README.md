# taskflow

[![Go Reference](https://pkg.go.dev/badge/github.com/sing198/taskflow.svg)](https://pkg.go.dev/github.com/sing198/taskflow)
[![Go Report Card](https://goreportcard.com/badge/github.com/sing198/taskflow)](https://goreportcard.com/report/github.com/sing198/taskflow)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

**taskflow** is a high-performance, distributed asynchronous task queue and background worker engine in Go backed by Redis. Designed for high throughput, at-least-once delivery guarantee, delayed scheduling, observable worker pools, and automated fault recovery.

---

## System Architecture

```text
 [ Go Application (Producer) ]
             │
             │ client.Enqueue(ctx, task)
             ▼
    ┌─────────────────────────────────────────┐
    │              Redis Broker               │
    │  ├─ taskflow:queue:default (List)       │ ◄── Immediate Tasks
    │  ├─ taskflow:delayed:default (ZSet)     │ ◄── Delayed/Scheduled
    │  ├─ taskflow:processing:default (List)  │ ◄── Active Tasks
    │  ├─ taskflow:unique:key (String TTL)    │ ◄── Deduplication
    │  └─ taskflow:dlq:default (List)         │ ◄── Dead Letter Queue
    └─────────────────────────────────────────┘
             │
             │ BRPopLPush / Atomic Lua
             ▼
 ┌────────────────────────────────────────────────────────┐
 │           taskflow Server / Worker Pool                │
 │                                                        │
 │   Dispatcher & Delayed Task Scheduler                  │
 │        │                                               │
 │        ├──► Worker 1 (Goroutine + Context Timeout)     │
 │        ├──► Worker 2 (Goroutine + Context Timeout)     │
 │        └──► Worker N (Goroutine + Context Timeout)     │
 │                                                        │
 │   Resilience & Observability:                          │
 │   • Exponential Backoff Retries with Full Jitter       │
 │   • Dead Letter Queue (DLQ) after retry exhaustion     │
 │   • Graceful Shutdown (SIGINT/SIGTERM draining)        │
 │   • Native Prometheus Metrics & slog Integration       │
 └────────────────────────────────────────────────────────┘
```

---

## Features

- **High Concurrency**: Worker pool managing goroutines with backpressure and context deadline support.
- **At-Least-Once Delivery**: Atomic `BRPopLPush` queue operations ensure tasks are never lost on worker crashes.
- **Delayed & Scheduled Execution**: Schedule tasks for future execution with millisecond precision using Redis Sorted Sets.
- **Full-Jitter Exponential Backoff**: Prevents thundering herd problems when retrying failed tasks.
- **Dead Letter Queue (DLQ)**: Automatically routes exhausted or unrecoverable tasks to DLQ for auditing.
- **Task Deduplication (Unique Tasks)**: Prevents duplicate job enqueuing within a configurable time window (`WithUnique`).
- **Graceful Shutdown**: Intercepts `SIGINT`/`SIGTERM` to allow in-flight tasks to complete cleanly without data loss.
- **Built-in Observability**:
  - Native Prometheus metrics exporter (`taskflow_tasks_processed_total`, `taskflow_task_duration_seconds`, `taskflow_queue_depth`, `taskflow_active_workers`).
  - Structured logging via Go standard `log/slog`.
- **Zero Heavy Dependencies**: Clean architecture with high test coverage and race-condition safety.

---

## Installation

```bash
go get github.com/sing198/taskflow
```

---

## Quick Start

### 1. Enqueueing Tasks (Producer)

```go
package main

import (
    "context"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/sing198/taskflow"
)

type EmailPayload struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}

func main() {
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    client := taskflow.NewClient(rdb)

    ctx := context.Background()

    // 1. Immediate task with 3 retries
    _, _ = client.EnqueueTask(ctx, "email:send", EmailPayload{
        To:      "alex@example.com",
        Subject: "Welcome!",
    }, taskflow.WithMaxRetry(3), taskflow.WithTimeout(10*time.Second))

    // 2. Delayed task (executes in 10 minutes)
    _, _ = client.EnqueueTask(ctx, "email:send", EmailPayload{
        To:      "alex@example.com",
        Subject: "Reminder",
    }, taskflow.WithDelay(10*time.Minute))

    // 3. Unique task (deduplicated for 1 hour)
    _, _ = client.EnqueueTask(ctx, "report:generate", map[string]int{"user_id": 42},
        taskflow.WithUnique("report:42", 1*time.Hour),
    )
}
```

### 2. Processing Tasks (Worker Server)

```go
package main

import (
    "context"
    "fmt"
    "log"
    "net/http"
    "time"

    "github.com/prometheus/client_golang/prometheus/promhttp"
    "github.com/redis/go-redis/v9"
    "github.com/sing198/taskflow"
)

func main() {
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    // Optional: Expose Prometheus metrics on :2112/metrics
    go func() {
        http.Handle("/metrics", promhttp.Handler())
        _ = http.ListenAndServe(":2112", nil)
    }()

    // Register task handlers
    mux := taskflow.NewServeMux()

    mux.HandleFunc("email:send", func(ctx context.Context, task *taskflow.Task) error {
        var p EmailPayload
        if err := task.BindJSON(&p); err != nil {
            return err
        }
        fmt.Printf("Sending email to %s\n", p.To)
        return nil
    })

    // Start worker server with 20 concurrent workers
    cfg := taskflow.DefaultConfig()
    cfg.Concurrency = 20
    cfg.ShutdownTimeout = 15 * time.Second

    server := taskflow.NewServer(rdb, cfg)

    // Run blocks until SIGINT/SIGTERM and gracefully drains in-flight jobs
    if err := server.Run(mux); err != nil {
        log.Fatal(err)
    }
}
```

---

## Running with Docker Compose

A complete local environment with Redis, Prometheus, and Grafana is provided under `docker/`:

```bash
cd docker
docker compose up -d
```

- **Redis**: `localhost:6379`
- **Prometheus**: `http://localhost:9090`
- **Grafana**: `http://localhost:3000` (User: `admin`, Pass: `admin`)

---

## Testing

Run unit & integration tests with race detector:

```bash
go test -v -race -cover ./...
```

---

## License

MIT © Thanaphat Khunphet
