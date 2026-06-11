package taskflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sing198/taskflow"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, redis.UniversalClient) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	t.Cleanup(func() {
		_ = rdb.Close()
		s.Close()
	})
	return s, rdb
}

func TestBroker_EnqueueAndFetch(t *testing.T) {
	_, rdb := setupTestRedis(t)
	broker := taskflow.NewRedisBroker(rdb, "test")

	ctx := context.Background()
	task, _ := taskflow.NewTask("test:job", "payload-data")

	if err := broker.Enqueue(ctx, task); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	fetched, err := broker.Fetch(ctx, "default", 1*time.Second)
	if err != nil {
		t.Fatalf("failed to fetch: %v", err)
	}
	if fetched == nil {
		t.Fatalf("expected task, got nil")
	}
	if fetched.ID != task.ID || string(fetched.Payload) != "payload-data" {
		t.Fatalf("fetched task mismatch: %+v", fetched)
	}

	// Ack task
	if err := broker.Ack(ctx, fetched); err != nil {
		t.Fatalf("failed to ack: %v", err)
	}

	pending, processing, _, _, _ := broker.QueueDepth(ctx, "default")
	if pending != 0 || processing != 0 {
		t.Errorf("expected empty queues, got pending=%d, processing=%d", pending, processing)
	}
}

func TestBroker_UniqueDeduplication(t *testing.T) {
	_, rdb := setupTestRedis(t)
	broker := taskflow.NewRedisBroker(rdb, "test")

	ctx := context.Background()
	task1, _ := taskflow.NewTask("test:unique", "p1", taskflow.WithUnique("lock:123", 5*time.Second))
	task2, _ := taskflow.NewTask("test:unique", "p2", taskflow.WithUnique("lock:123", 5*time.Second))

	if err := broker.Enqueue(ctx, task1); err != nil {
		t.Fatalf("failed to enqueue first task: %v", err)
	}

	err := broker.Enqueue(ctx, task2)
	if !errors.Is(err, taskflow.ErrDuplicateTask) {
		t.Fatalf("expected ErrDuplicateTask for second task, got %v", err)
	}
}

func TestBroker_DelayedAndMigration(t *testing.T) {
	_, rdb := setupTestRedis(t)
	broker := taskflow.NewRedisBroker(rdb, "test")

	ctx := context.Background()
	task, _ := taskflow.NewTask("test:delayed", "wait-data", taskflow.WithDelay(10*time.Second))

	if err := broker.Enqueue(ctx, task); err != nil {
		t.Fatalf("failed to enqueue delayed task: %v", err)
	}

	// Before time advancement, pending should be 0, delayed should be 1
	p, _, d, _, _ := broker.QueueDepth(ctx, "default")
	if p != 0 || d != 1 {
		t.Errorf("expected p=0, d=1; got p=%d, d=%d", p, d)
	}

	// Simulate future timestamp migration
	futureMilli := time.Now().Add(15 * time.Second).UnixMilli()
	migrated, err := broker.MigrateDelayedUntil(ctx, "default", futureMilli, 10)
	if err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}
	if migrated != 1 {
		t.Errorf("expected 1 task migrated, got %d", migrated)
	}

	p, _, d, _, _ = broker.QueueDepth(ctx, "default")
	if p != 1 || d != 0 {
		t.Errorf("expected p=1, d=0; got p=%d, d=%d", p, d)
	}
}

func TestBroker_RetryAndDLQ(t *testing.T) {
	_, rdb := setupTestRedis(t)
	broker := taskflow.NewRedisBroker(rdb, "test")

	ctx := context.Background()
	task, _ := taskflow.NewTask("test:fail", "data", taskflow.WithMaxRetry(1))
	_ = broker.Enqueue(ctx, task)

	fetched, _ := broker.Fetch(ctx, "default", 1*time.Second)

	// Route to DLQ
	if err := broker.RouteToDLQ(ctx, fetched, errors.New("fatal crash")); err != nil {
		t.Fatalf("failed to route to DLQ: %v", err)
	}

	_, processing, _, dlq, _ := broker.QueueDepth(ctx, "default")
	if processing != 0 || dlq != 1 {
		t.Errorf("expected processing=0, dlq=1; got processing=%d, dlq=%d", processing, dlq)
	}
}
