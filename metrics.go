package taskflow

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricsOnce sync.Once
	globalM     *Metrics
)

// Metrics holds Prometheus collectors for taskflow.
type Metrics struct {
	TasksProcessed *prometheus.CounterVec
	TaskDuration   *prometheus.HistogramVec
	ActiveWorkers  *prometheus.GaugeVec
	QueueDepth     *prometheus.GaugeVec
}

// NewMetrics initializes and registers taskflow Prometheus metrics with the default registerer.
func NewMetrics() *Metrics {
	metricsOnce.Do(func() {
		globalM = &Metrics{
			TasksProcessed: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: "taskflow",
					Name:      "tasks_processed_total",
					Help:      "Total number of tasks processed partitioned by queue, type and status (success, retry, dlq).",
				},
				[]string{"queue", "type", "status"},
			),
			TaskDuration: promauto.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace: "taskflow",
					Name:      "task_duration_seconds",
					Help:      "Duration of task execution in seconds.",
					Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
				},
				[]string{"queue", "type"},
			),
			ActiveWorkers: promauto.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: "taskflow",
					Name:      "active_workers",
					Help:      "Number of currently active worker goroutines processing tasks.",
				},
				[]string{"queue"},
			),
			QueueDepth: promauto.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: "taskflow",
					Name:      "queue_depth",
					Help:      "Current number of tasks in queues partitioned by queue and state (pending, processing, delayed, dlq).",
				},
				[]string{"queue", "state"},
			),
		}
	})
	return globalM
}
