package taskflow

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Default settings for tasks.
const (
	DefaultMaxRetry = 3
	DefaultTimeout  = 30 * time.Second
	DefaultQueue    = "default"
)

// Task represents a discrete unit of work to be processed asynchronously.
type Task struct {
	ID        string        `json:"id"`
	Type      string        `json:"type"`
	Queue     string        `json:"queue"`
	Payload   []byte        `json:"payload"`
	MaxRetry  int           `json:"max_retry"`
	Retried   int           `json:"retried"`
	Timeout   time.Duration `json:"timeout"`
	ProcessAt time.Time     `json:"process_at"`
	CreatedAt time.Time     `json:"created_at"`
	UniqueKey string        `json:"unique_key,omitempty"`
	UniqueTTL time.Duration `json:"unique_ttl,omitempty"`
	LastError string        `json:"last_error,omitempty"`

	// raw JSON representation as stored in Redis (internal)
	raw string `json:"-"`
}

// TaskOptions holds optional parameters for creating and enqueuing tasks.
type TaskOptions struct {
	Queue     string
	Delay     time.Duration
	ProcessAt time.Time
	Timeout   time.Duration
	MaxRetry  int
	UniqueKey string
	UniqueTTL time.Duration
}

// Option configures TaskOptions.
type Option func(*TaskOptions)

// WithQueue specifies the target queue name for the task.
func WithQueue(q string) Option {
	return func(o *TaskOptions) {
		if q != "" {
			o.Queue = q
		}
	}
}

// WithDelay schedules the task to run after the specified delay duration.
func WithDelay(d time.Duration) Option {
	return func(o *TaskOptions) {
		o.Delay = d
	}
}

// WithProcessAt schedules the task to run at a specific timestamp.
func WithProcessAt(t time.Time) Option {
	return func(o *TaskOptions) {
		o.ProcessAt = t
	}
}

// WithTimeout sets a hard context deadline for task execution.
func WithTimeout(d time.Duration) Option {
	return func(o *TaskOptions) {
		o.Timeout = d
	}
}

// WithMaxRetry sets the maximum retry attempts on failure.
func WithMaxRetry(n int) Option {
	return func(o *TaskOptions) {
		o.MaxRetry = n
	}
}

// WithUnique enforces task deduplication with a given unique key and TTL.
func WithUnique(key string, ttl time.Duration) Option {
	return func(o *TaskOptions) {
		o.UniqueKey = key
		o.UniqueTTL = ttl
	}
}

// NewTask creates a new Task instance with serialized JSON payload and applied options.
func NewTask(taskType string, payload any, opts ...Option) (*Task, error) {
	if taskType == "" {
		return nil, errors.New("taskflow: task type cannot be empty")
	}

	var rawPayload []byte
	if payload != nil {
		switch p := payload.(type) {
		case []byte:
			rawPayload = p
		case string:
			rawPayload = []byte(p)
		default:
			b, err := json.Marshal(payload)
			if err != nil {
				return nil, err
			}
			rawPayload = b
		}
	}

	opt := TaskOptions{
		Queue:    DefaultQueue,
		Timeout:  DefaultTimeout,
		MaxRetry: DefaultMaxRetry,
	}

	for _, fn := range opts {
		fn(&opt)
	}

	now := time.Now().UTC()
	processAt := now
	if opt.Delay > 0 {
		processAt = now.Add(opt.Delay)
	} else if !opt.ProcessAt.IsZero() {
		processAt = opt.ProcessAt.UTC()
	}

	return &Task{
		ID:        uuid.NewString(),
		Type:      taskType,
		Queue:     opt.Queue,
		Payload:   rawPayload,
		MaxRetry:  opt.MaxRetry,
		Retried:   0,
		Timeout:   opt.Timeout,
		ProcessAt: processAt,
		CreatedAt: now,
		UniqueKey: opt.UniqueKey,
		UniqueTTL: opt.UniqueTTL,
	}, nil
}

// BindJSON unmarshals the task payload into target struct.
func (t *Task) BindJSON(target any) error {
	if len(t.Payload) == 0 {
		return errors.New("taskflow: payload is empty")
	}
	return json.Unmarshal(t.Payload, target)
}
