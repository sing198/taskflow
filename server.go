package taskflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config configures the taskflow Server.
type Config struct {
	Concurrency         int
	Queues              []string
	PollInterval        time.Duration
	DelayedPollInterval time.Duration
	ShutdownTimeout     time.Duration
	Prefix              string
	Logger              *slog.Logger
	Backoff             BackoffFunc
}

// DefaultConfig provides sensible production defaults.
func DefaultConfig() Config {
	return Config{
		Concurrency:         10,
		Queues:              []string{DefaultQueue},
		PollInterval:        1 * time.Second,
		DelayedPollInterval: 1 * time.Second,
		ShutdownTimeout:     15 * time.Second,
		Prefix:              "taskflow",
		Logger:              slog.Default(),
		Backoff:             DefaultBackoff,
	}
}

// Server is the worker engine pulling and processing tasks concurrently.
type Server struct {
	cfg     Config
	broker  *RedisBroker
	metrics *Metrics
	logger  *slog.Logger

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
	wg      sync.WaitGroup
}

// NewServer creates a new Server instance.
func NewServer(rdb redis.UniversalClient, cfg Config) *Server {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if len(cfg.Queues) == 0 {
		cfg.Queues = []string{DefaultQueue}
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.DelayedPollInterval <= 0 {
		cfg.DelayedPollInterval = 1 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 15 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Backoff == nil {
		cfg.Backoff = DefaultBackoff
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "taskflow"
	}

	return &Server{
		cfg:     cfg,
		broker:  NewRedisBroker(rdb, cfg.Prefix),
		metrics: NewMetrics(),
		logger:  cfg.Logger,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

// Run starts the server and blocks until a termination signal (SIGINT, SIGTERM) is received.
func (s *Server) Run(handler Handler) error {
	if err := s.Start(handler); err != nil {
		return err
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		s.logger.Info("received termination signal, initiating graceful shutdown", "signal", sig.String())
	case <-s.stopCh:
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()

	return s.Shutdown(ctx)
}

// Start launches worker goroutines and background schedulers without blocking.
func (s *Server) Start(handler Handler) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return errors.New("taskflow: server already running")
	}
	if handler == nil {
		return errors.New("taskflow: nil handler provided to server")
	}

	s.running = true
	s.stopCh = make(chan struct{})
	s.doneCh = make(chan struct{})

	s.logger.Info("starting taskflow server",
		"concurrency", s.cfg.Concurrency,
		"queues", s.cfg.Queues,
	)

	// Launch delayed task scheduler for each queue
	for _, q := range s.cfg.Queues {
		s.wg.Add(1)
		go s.runDelayedScheduler(q)
	}

	// Launch dispatchers for each queue
	for _, q := range s.cfg.Queues {
		s.wg.Add(1)
		go s.runDispatcher(q, handler)
	}

	// Launch queue depth metrics reporter
	s.wg.Add(1)
	go s.runMetricsReporter()

	return nil
}

// Shutdown gracefully drains active workers within the given context deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	close(s.stopCh)
	s.mu.Unlock()

	s.logger.Info("waiting for in-flight tasks to complete...")

	stopped := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(stopped)
	}()

	select {
	case <-stopped:
		s.logger.Info("all workers stopped cleanly")
		close(s.doneCh)
		return nil
	case <-ctx.Done():
		s.logger.Warn("shutdown timed out, some workers may have been interrupted")
		return ctx.Err()
	}
}

// Stop stops the server immediately.
func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	_ = s.Shutdown(ctx)
}

func (s *Server) runDelayedScheduler(queue string) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.cfg.DelayedPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			n, err := s.broker.MigrateDelayed(ctx, queue, 100)
			cancel()
			if err != nil {
				s.logger.Error("error migrating delayed tasks", "queue", queue, "error", err)
			} else if n > 0 {
				s.logger.Debug("migrated delayed tasks to active queue", "queue", queue, "count", n)
			}
		}
	}
}

func (s *Server) runDispatcher(queue string, handler Handler) {
	defer s.wg.Done()

	sem := make(chan struct{}, s.cfg.Concurrency)

	for {
		select {
		case <-s.stopCh:
			return
		case sem <- struct{}{}:
			// Acquired worker token
		}

		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.PollInterval)
		task, err := s.broker.Fetch(ctx, queue, s.cfg.PollInterval)
		cancel()

		if err != nil {
			<-sem
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				s.logger.Error("error fetching task from queue", "queue", queue, "error", err)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if task == nil {
			<-sem // No task available, release worker token
			continue
		}

		// Process task concurrently
		s.wg.Add(1)
		go func(t *Task) {
			defer func() {
				<-sem
				s.wg.Done()
			}()
			s.execTask(t, handler)
		}(task)
	}
}

func (s *Server) execTask(task *Task, handler Handler) {
	s.metrics.ActiveWorkers.WithLabelValues(task.Queue).Inc()
	defer s.metrics.ActiveWorkers.WithLabelValues(task.Queue).Dec()

	start := time.Now()
	timeout := task.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("task panicked: %v", r)
			}
		}()
		err = handler.ProcessTask(ctx, task)
	}()

	duration := time.Since(start).Seconds()
	s.metrics.TaskDuration.WithLabelValues(task.Queue, task.Type).Observe(duration)

	if err == nil {
		// Task succeeded
		s.metrics.TasksProcessed.WithLabelValues(task.Queue, task.Type, "success").Inc()
		s.logger.Info("task completed successfully",
			"task_id", task.ID,
			"type", task.Type,
			"queue", task.Queue,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ackCancel()
		_ = s.broker.Ack(ackCtx, task)
		return
	}

	// Task failed
	s.logger.Warn("task execution failed",
		"task_id", task.ID,
		"type", task.Type,
		"retried", task.Retried,
		"max_retry", task.MaxRetry,
		"error", err,
	)

	failCtx, failCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer failCancel()

	if task.Retried < task.MaxRetry {
		// Schedule retry with exponential backoff
		delay := s.cfg.Backoff(task.Retried + 1)
		s.metrics.TasksProcessed.WithLabelValues(task.Queue, task.Type, "retry").Inc()
		_ = s.broker.Retry(failCtx, task, delay, err)
		s.logger.Info("scheduled task retry",
			"task_id", task.ID,
			"retry_attempt", task.Retried,
			"delay", delay.String(),
		)
	} else {
		// Move to Dead Letter Queue (DLQ)
		s.metrics.TasksProcessed.WithLabelValues(task.Queue, task.Type, "dlq").Inc()
		_ = s.broker.RouteToDLQ(failCtx, task, err)
		s.logger.Error("task exhausted all retries, routed to DLQ",
			"task_id", task.ID,
			"final_error", err,
		)
	}
}

func (s *Server) runMetricsReporter() {
	defer s.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			for _, q := range s.cfg.Queues {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				pending, processing, delayed, dlq, err := s.broker.QueueDepth(ctx, q)
				cancel()
				if err == nil {
					s.metrics.QueueDepth.WithLabelValues(q, "pending").Set(float64(pending))
					s.metrics.QueueDepth.WithLabelValues(q, "processing").Set(float64(processing))
					s.metrics.QueueDepth.WithLabelValues(q, "delayed").Set(float64(delayed))
					s.metrics.QueueDepth.WithLabelValues(q, "dlq").Set(float64(dlq))
				}
			}
		}
	}
}
