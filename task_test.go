package taskflow_test

import (
	"context"
	"testing"
	"time"

	"github.com/sing198/taskflow"
)

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func TestNewTask(t *testing.T) {
	payload := EmailPayload{To: "dev@example.com", Subject: "Welcome"}

	task, err := taskflow.NewTask("email:send", payload,
		taskflow.WithQueue("critical"),
		taskflow.WithTimeout(10*time.Second),
		taskflow.WithMaxRetry(5),
		taskflow.WithDelay(2*time.Minute),
		taskflow.WithUnique("email:dev@example.com", 1*time.Hour),
	)

	if err != nil {
		t.Fatalf("unexpected error creating task: %v", err)
	}

	if task.Type != "email:send" {
		t.Errorf("expected type email:send, got %s", task.Type)
	}
	if task.Queue != "critical" {
		t.Errorf("expected queue critical, got %s", task.Queue)
	}
	if task.MaxRetry != 5 {
		t.Errorf("expected max retry 5, got %d", task.MaxRetry)
	}
	if task.Timeout != 10*time.Second {
		t.Errorf("expected timeout 10s, got %v", task.Timeout)
	}
	if task.UniqueKey != "email:dev@example.com" {
		t.Errorf("expected unique key, got %s", task.UniqueKey)
	}

	var bound EmailPayload
	if err := task.BindJSON(&bound); err != nil {
		t.Fatalf("failed to bind json: %v", err)
	}
	if bound != payload {
		t.Errorf("payload mismatch: expected %+v, got %+v", payload, bound)
	}
}

func TestNewTask_Validation(t *testing.T) {
	_, err := taskflow.NewTask("", nil)
	if err == nil {
		t.Errorf("expected error for empty task type, got nil")
	}
}

func TestClient_EnqueueWithOptions(t *testing.T) {
	_, rdb := setupTestRedis(t)
	client := taskflow.NewClient(rdb)

	ctx := context.Background()
	task, err := client.EnqueueTask(ctx, "job:custom", "payload",
		taskflow.WithQueue("custom_q"),
		taskflow.WithTimeout(5*time.Second),
		taskflow.WithMaxRetry(2),
		taskflow.WithDelay(100*time.Millisecond),
	)

	if err != nil {
		t.Fatalf("failed to enqueue task: %v", err)
	}

	if task.Queue != "custom_q" {
		t.Errorf("expected queue custom_q, got %s", task.Queue)
	}

	p, pr, d, dlq, err := client.QueueDepth(ctx, "custom_q")
	if err != nil {
		t.Fatalf("failed to get queue depth: %v", err)
	}
	if d != 1 || p != 0 || pr != 0 || dlq != 0 {
		t.Errorf("unexpected queue stats: p=%d, pr=%d, d=%d, dlq=%d", p, pr, d, dlq)
	}

	// Test Enqueue existing task with options
	task2, _ := taskflow.NewTask("job:custom2", "p2")
	err = client.Enqueue(ctx, task2, taskflow.WithQueue("custom_q2"), taskflow.WithDelay(50*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to enqueue task2: %v", err)
	}
}

func TestTask_BindJSON_Empty(t *testing.T) {
	task := &taskflow.Task{}
	var target any
	if err := task.BindJSON(&target); err == nil {
		t.Errorf("expected error binding empty payload, got nil")
	}
}
