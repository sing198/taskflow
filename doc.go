// Package taskflow provides a distributed task queue and worker engine in Go backed by Redis.
// It features worker pool management, at-least-once delivery guarantee, exponential backoff retries,
// delayed task execution, dead-letter queues (DLQ), crash recovery, and Prometheus metrics.
package taskflow
