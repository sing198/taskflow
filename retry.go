package taskflow

import (
	"math"
	"math/rand"
	"time"
)

// BackoffFunc calculates the duration to wait before the next retry attempt.
type BackoffFunc func(retryCount int) time.Duration

const (
	defaultBaseDelay = 1 * time.Second
	defaultMaxDelay  = 30 * time.Minute
)

// ExponentialBackoff computes an exponential backoff duration with randomized full jitter.
func ExponentialBackoff(retryCount int, baseDelay, maxDelay time.Duration) time.Duration {
	if retryCount <= 0 {
		return baseDelay
	}

	if baseDelay <= 0 {
		baseDelay = defaultBaseDelay
	}
	if maxDelay <= 0 {
		maxDelay = defaultMaxDelay
	}

	// Calculate exponential backoff: base * 2^(retries - 1)
	factor := math.Pow(2, float64(retryCount-1))
	temp := float64(baseDelay) * factor

	if temp > float64(maxDelay) || math.IsInf(temp, 0) {
		temp = float64(maxDelay)
	}

	// Apply full jitter: random value between 0 and calculated backoff duration
	jitter := rand.Float64() * temp
	duration := time.Duration(jitter)

	// Ensure at least baseDelay / 2 to prevent instantaneous retry
	minWait := baseDelay / 2
	if duration < minWait {
		duration = minWait
	}

	return duration
}

// DefaultBackoff provides the standard exponential backoff with full jitter for taskflow.
func DefaultBackoff(retryCount int) time.Duration {
	return ExponentialBackoff(retryCount, defaultBaseDelay, defaultMaxDelay)
}
