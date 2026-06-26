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

	// 1. Expose Prometheus metrics endpoint on :2112
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		})
		fmt.Println("📈 Prometheus metrics exposed at http://localhost:2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Printf("metrics server error: %v", err)
		}
	}()

	// 2. Set up Task Router (ServeMux)
	mux := taskflow.NewServeMux()

	mux.HandleFunc("email:send", func(ctx context.Context, task *taskflow.Task) error {
		var p EmailPayload
		if err := task.BindJSON(&p); err != nil {
			return err
		}
		fmt.Printf("📧 [Worker] Sending email to %s (Subject: %s)\n", p.To, p.Subject)
		time.Sleep(100 * time.Millisecond) // simulate work
		return nil
	})

	mux.HandleFunc("report:generate", func(ctx context.Context, task *taskflow.Task) error {
		var p ReportPayload
		if err := task.BindJSON(&p); err != nil {
			return err
		}
		fmt.Printf("📊 [Worker] Generating report #%d in %s format\n", p.ReportID, p.Format)
		time.Sleep(200 * time.Millisecond) // simulate heavy work
		return nil
	})

	mux.HandleFunc("notification:push", func(ctx context.Context, task *taskflow.Task) error {
		fmt.Printf("🔔 [Worker] Pushing notification payload: %s\n", string(task.Payload))
		return nil
	})

	// 3. Configure and start Taskflow Server with 10 concurrent workers
	cfg := taskflow.DefaultConfig()
	cfg.Concurrency = 10
	cfg.ShutdownTimeout = 10 * time.Second

	server := taskflow.NewServer(rdb, cfg)

	fmt.Println("🚀 Starting Taskflow worker server... (Press Ctrl+C to stop)")
	if err := server.Run(mux); err != nil {
		log.Fatalf("server exited with error: %v", err)
	}
}
