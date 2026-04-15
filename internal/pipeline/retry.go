package pipeline

import (
	"context"
	"errors"
	"time"
)

// RetryPolicy defines how retries are attempted.
type RetryPolicy struct {
	MaxAttempts int
	InitialDelay time.Duration
	Multiplier   float64
	MaxDelay     time.Duration
}

// DefaultRetryPolicy returns a sensible default exponential backoff policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		Multiplier:   2.0,
		MaxDelay:     5 * time.Second,
	}
}

// RetryWriter wraps a Sink and retries failed writes according to a policy.
type RetryWriter struct {
	sink   Sink
	policy RetryPolicy
}

// NewRetryWriter creates a RetryWriter wrapping the given sink.
func NewRetryWriter(sink Sink, policy RetryPolicy) *RetryWriter {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}
	if policy.Multiplier <= 0 {
		policy.Multiplier = 1.0
	}
	return &RetryWriter{sink: sink, policy: policy}
}

// Write attempts to write the event, retrying on error with exponential backoff.
func (r *RetryWriter) Write(ctx context.Context, event *Event) error {
	delay := r.policy.InitialDelay
	var lastErr error
	for attempt := 0; attempt < r.policy.MaxAttempts; attempt++ {
		if err := r.sink.Write(ctx, event); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt < r.policy.MaxAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay = time.Duration(float64(delay) * r.policy.Multiplier)
			if delay > r.policy.MaxDelay && r.policy.MaxDelay > 0 {
				delay = r.policy.MaxDelay
			}
		}
	}
	return errors.New("retry exhausted: " + lastErr.Error())
}

// Close closes the underlying sink.
func (r *RetryWriter) Close() error {
	return r.sink.Close()
}
