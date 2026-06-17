package taskflow_test

import (
	"testing"
	"time"

	"github.com/sing198/taskflow"
)

func TestExponentialBackoff(t *testing.T) {
	base := 100 * time.Millisecond
	max := 5 * time.Second

	for retry := 1; retry <= 5; retry++ {
		d := taskflow.ExponentialBackoff(retry, base, max)
		if d < base/2 {
			t.Errorf("retry %d: backoff duration %v is less than min bound %v", retry, d, base/2)
		}
		if d > max {
			t.Errorf("retry %d: backoff duration %v exceeded max bound %v", retry, d, max)
		}
	}
}

func TestDefaultBackoff(t *testing.T) {
	d := taskflow.DefaultBackoff(1)
	if d <= 0 {
		t.Errorf("expected positive backoff duration, got %v", d)
	}
}
