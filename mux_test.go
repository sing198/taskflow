package taskflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sing198/taskflow"
)

func TestServeMux_RoutingAndMiddleware(t *testing.T) {
	mux := taskflow.NewServeMux()

	var order []string

	middleware := func(next taskflow.Handler) taskflow.Handler {
		return taskflow.HandlerFunc(func(ctx context.Context, task *taskflow.Task) error {
			order = append(order, "mw_before")
			err := next.ProcessTask(ctx, task)
			order = append(order, "mw_after")
			return err
		})
	}

	mux.Use(middleware)

	mux.HandleFunc("order:process", func(ctx context.Context, task *taskflow.Task) error {
		order = append(order, "handler")
		return nil
	})

	task, _ := taskflow.NewTask("order:process", map[string]string{"id": "123"})
	err := mux.ProcessTask(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedOrder := []string{"mw_before", "handler", "mw_after"}
	if len(order) != len(expectedOrder) {
		t.Fatalf("expected sequence %+v, got %+v", expectedOrder, order)
	}
	for i, v := range expectedOrder {
		if order[i] != v {
			t.Errorf("step %d: expected %s, got %s", i, v, order[i])
		}
	}
}

func TestServeMux_NotFound(t *testing.T) {
	mux := taskflow.NewServeMux()
	task, _ := taskflow.NewTask("unknown:task", nil)
	err := mux.ProcessTask(context.Background(), task)
	if !errors.Is(err, taskflow.ErrNoHandler) {
		t.Errorf("expected ErrNoHandler, got %v", err)
	}
}
