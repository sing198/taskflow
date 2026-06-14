package taskflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrNoHandler is returned when no handler is registered for a task type.
var ErrNoHandler = errors.New("taskflow: no handler registered for task type")

// Handler processes incoming tasks.
type Handler interface {
	ProcessTask(ctx context.Context, task *Task) error
}

// HandlerFunc is an adapter allowing normal functions to serve as Handlers.
type HandlerFunc func(ctx context.Context, task *Task) error

// ProcessTask calls f(ctx, task).
func (f HandlerFunc) ProcessTask(ctx context.Context, task *Task) error {
	return f(ctx, task)
}

// MiddlewareFunc wraps a Handler to execute cross-cutting concerns (logging, recovery, metrics).
type MiddlewareFunc func(Handler) Handler

// ServeMux routes tasks to handlers based on their task type.
type ServeMux struct {
	mu          sync.RWMutex
	handlers    map[string]Handler
	middlewares []MiddlewareFunc
}

// NewServeMux allocates and initializes a new ServeMux.
func NewServeMux() *ServeMux {
	return &ServeMux{
		handlers: make(map[string]Handler),
	}
}

// Handle registers the handler for the given task type pattern.
func (m *ServeMux) Handle(pattern string, handler Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if pattern == "" {
		panic("taskflow: nil pattern")
	}
	if handler == nil {
		panic("taskflow: nil handler")
	}
	if _, exist := m.handlers[pattern]; exist {
		panic(fmt.Sprintf("taskflow: multiple registrations for %s", pattern))
	}

	m.handlers[pattern] = handler
}

// HandleFunc registers the handler function for the given task type pattern.
func (m *ServeMux) HandleFunc(pattern string, handler func(context.Context, *Task) error) {
	if handler == nil {
		panic("taskflow: nil handler")
	}
	m.Handle(pattern, HandlerFunc(handler))
}

// Use appends middlewares to the dispatch chain.
func (m *ServeMux) Use(middlewares ...MiddlewareFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.middlewares = append(m.middlewares, middlewares...)
}

// ProcessTask routes the task to the registered handler, executing through middlewares.
func (m *ServeMux) ProcessTask(ctx context.Context, task *Task) error {
	m.mu.RLock()
	h, ok := m.handlers[task.Type]
	middlewares := make([]MiddlewareFunc, len(m.middlewares))
	copy(middlewares, m.middlewares)
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: %s", ErrNoHandler, task.Type)
	}

	// Chain middlewares in order
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}

	return h.ProcessTask(ctx, task)
}
