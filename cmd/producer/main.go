package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sing198/taskflow"
)

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type ReportPayload struct {
	ReportID int    `json:"report_id"`
	Format   string `json:"format"`
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("failed to connect to Redis: %v", err)
	}

	client := taskflow.NewClient(rdb)

	fmt.Println("🚀 Enqueuing sample tasks into taskflow...")

	// 1. Enqueue Immediate Task
	task1, err := client.EnqueueTask(ctx, "email:send", EmailPayload{
		To:      "user@example.com",
		Subject: "Welcome to Taskflow",
		Body:    "Your account is ready!",
	}, taskflow.WithMaxRetry(3))
	if err != nil {
		log.Fatalf("failed to enqueue email task: %v", err)
	}
	fmt.Printf("✅ Enqueued immediate task [%s]: %s\n", task1.Type, task1.ID)

	// 2. Enqueue Delayed Task (scheduled for 10s later)
	task2, err := client.EnqueueTask(ctx, "report:generate", ReportPayload{
		ReportID: 101,
		Format:   "pdf",
	}, taskflow.WithDelay(10*time.Second))
	if err != nil {
		log.Fatalf("failed to enqueue report task: %v", err)
	}
	fmt.Printf("⏳ Enqueued delayed task (10s) [%s]: %s\n", task2.Type, task2.ID)

	// 3. Enqueue Unique Task (deduplicated)
	task3, err := client.EnqueueTask(ctx, "notification:push", map[string]string{
		"user_id": "usr_99",
		"message": "New friend request",
	}, taskflow.WithUnique("notify:usr_99", 1*time.Minute))
	if err != nil {
		log.Fatalf("failed to enqueue unique task: %v", err)
	}
	fmt.Printf("🔒 Enqueued unique task [%s]: %s\n", task3.Type, task3.ID)

	// Check queue stats
	p, pr, d, dlq, _ := client.QueueDepth(ctx, "default")
	fmt.Printf("📊 Queue status -> Pending: %d, Processing: %d, Delayed: %d, DLQ: %d\n", p, pr, d, dlq)
}
