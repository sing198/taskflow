package taskflow

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// ClientOption configures the Client.
type ClientOption func(*Client)

// Client is used to enqueue tasks to taskflow queues.
type Client struct {
	broker *RedisBroker
}

// WithClientPrefix sets a custom Redis key prefix for the client.
func WithClientPrefix(prefix string) ClientOption {
	return func(c *Client) {
		if prefix != "" {
			c.broker.prefix = prefix
		}
	}
}

// NewClient creates a new taskflow Client.
func NewClient(rdb redis.UniversalClient, opts ...ClientOption) *Client {
	c := &Client{
		broker: NewRedisBroker(rdb, "taskflow"),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Enqueue adds a pre-created Task to the queue.
func (c *Client) Enqueue(ctx context.Context, task *Task, opts ...Option) error {
	if len(opts) > 0 {
		opt := TaskOptions{
			Queue:     task.Queue,
			Timeout:   task.Timeout,
			MaxRetry:  task.MaxRetry,
			UniqueKey: task.UniqueKey,
			UniqueTTL: task.UniqueTTL,
		}
		for _, fn := range opts {
			fn(&opt)
		}
		task.Queue = opt.Queue
		task.Timeout = opt.Timeout
		task.MaxRetry = opt.MaxRetry
		task.UniqueKey = opt.UniqueKey
		task.UniqueTTL = opt.UniqueTTL
		if opt.Delay > 0 {
			task.ProcessAt = task.CreatedAt.Add(opt.Delay)
		} else if !opt.ProcessAt.IsZero() {
			task.ProcessAt = opt.ProcessAt
		}
	}
	return c.broker.Enqueue(ctx, task)
}

// EnqueueTask creates and immediately enqueues a new task with given payload and options.
func (c *Client) EnqueueTask(ctx context.Context, taskType string, payload any, opts ...Option) (*Task, error) {
	task, err := NewTask(taskType, payload, opts...)
	if err != nil {
		return nil, err
	}

	if err := c.broker.Enqueue(ctx, task); err != nil {
		return nil, err
	}

	return task, nil
}

// QueueDepth inspects the current queue status.
func (c *Client) QueueDepth(ctx context.Context, queue string) (pending, processing, delayed, dlq int64, err error) {
	if queue == "" {
		queue = DefaultQueue
	}
	return c.broker.QueueDepth(ctx, queue)
}
