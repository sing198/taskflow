package taskflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sing198/taskflow"
)

func TestServer_EndToEndProcessing(t *testing.T) {
	_, rdb := setupTestRedis(t)

	client := taskflow.NewClient(rdb, taskflow.WithClientPrefix("test_e2e"))

	cfg := taskflow.DefaultConfig()
	cfg.Prefix = "test_e2e"
	cfg.Concurrency = 4
	cfg.PollInterval = 50 * time.Millisecond
	cfg.DelayedPollInterval = 50 * time.Millisecond

	server := taskflow.NewServer(rdb, cfg)

	var processedCount int32
	mux := taskflow.NewServeMux()
	mux.HandleFunc("job:test", func(ctx context.Context, task *taskflow.Task) error {
		atomic.AddInt32(&processedCount, 1)
		return nil
	})

	if err := server.Start(mux); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, err := client.EnqueueTask(ctx, "job:test", map[string]int{"num": i})
		if err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
	}

	// Wait for worker pool to finish processing 5 tasks
	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadInt32(&processedCount) < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for tasks to process, count=%d", atomic.LoadInt32(&processedCount))
		}
		time.Sleep(50 * time.Millisecond)
	}

	if count := atomic.LoadInt32(&processedCount); count != 5 {
		t.Errorf("expected 5 processed tasks, got %d", count)
	}
}

func TestServer_PanicRecoveryAndDLQ(t *testing.T) {
	_, rdb := setupTestRedis(t)

	client := taskflow.NewClient(rdb, taskflow.WithClientPrefix("test_dlq"))

	cfg := taskflow.DefaultConfig()
	cfg.Prefix = "test_dlq"
	cfg.Concurrency = 2
	cfg.PollInterval = 50 * time.Millisecond
	cfg.DelayedPollInterval = 50 * time.Millisecond
	cfg.Backoff = func(_ int) time.Duration {
		return 10 * time.Millisecond // fast retry for testing
	}

	server := taskflow.NewServer(rdb, cfg)

	var attempts int32
	mux := taskflow.NewServeMux()
	mux.HandleFunc("job:panicky", func(ctx context.Context, task *taskflow.Task) error {
		atomic.AddInt32(&attempts, 1)
		panic("unexpected crash simulation")
	})

	if err := server.Start(mux); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	ctx := context.Background()
	// Task with 0 retries -> directly goes to DLQ on first failure
	_, err := client.EnqueueTask(ctx, "job:panicky", "boom", taskflow.WithMaxRetry(0))
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Wait for DLQ
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, _, _, dlq, _ := client.QueueDepth(ctx, "default")
		if dlq == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for task to route to DLQ")
		}
		time.Sleep(50 * time.Millisecond)
	}

	if at := atomic.LoadInt32(&attempts); at != 1 {
		t.Errorf("expected 1 attempt before DLQ, got %d", at)
	}
}

func TestServer_RetryWithBackoff(t *testing.T) {
	_, rdb := setupTestRedis(t)

	client := taskflow.NewClient(rdb, taskflow.WithClientPrefix("test_retry"))

	cfg := taskflow.DefaultConfig()
	cfg.Prefix = "test_retry"
	cfg.Concurrency = 2
	cfg.PollInterval = 50 * time.Millisecond
	cfg.DelayedPollInterval = 50 * time.Millisecond
	cfg.Backoff = func(_ int) time.Duration {
		return 20 * time.Millisecond
	}

	server := taskflow.NewServer(rdb, cfg)

	var attempts int32
	mux := taskflow.NewServeMux()
	mux.HandleFunc("job:flaky", func(ctx context.Context, task *taskflow.Task) error {
		count := atomic.AddInt32(&attempts, 1)
		if count < 3 {
			return errors.New("temporary error")
		}
		return nil // succeed on 3rd attempt
	})

	if err := server.Start(mux); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	ctx := context.Background()
	_, err := client.EnqueueTask(ctx, "job:flaky", "data", taskflow.WithMaxRetry(3))
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	deadline := time.Now().Add(4 * time.Second)
	for atomic.LoadInt32(&attempts) < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for retry success, attempts=%d", atomic.LoadInt32(&attempts))
		}
		time.Sleep(50 * time.Millisecond)
	}
}
