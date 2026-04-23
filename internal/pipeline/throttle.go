package pipeline

import (
	"context"
	"sync"
	"time"
)

// ThrottlePolicy controls what happens when the throttle window is exhausted.
type ThrottlePolicy int

const (
	// ThrottleDrop silently drops events that exceed the window limit.
	ThrottleDrop ThrottlePolicy = iota
	// ThrottleBlock waits until the next window opens before accepting the event.
	ThrottleBlock
)

// Throttle limits the number of events forwarded to a sink within a fixed
// time window. It is safe for concurrent use.
type Throttle struct {
	mu       sync.Mutex
	sink     Sink
	limit    int
	window   time.Duration
	policy   ThrottlePolicy
	count    int
	windowAt time.Time
}

// NewThrottle creates a Throttle that allows at most limit events per window
// duration. Events beyond the limit are handled according to policy.
func NewThrottle(sink Sink, limit int, window time.Duration, policy ThrottlePolicy) *Throttle {
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Second
	}
	return &Throttle{
		sink:     sink,
		limit:    limit,
		window:   window,
		policy:   policy,
		windowAt: time.Now().Add(window),
	}
}

// Write forwards the event to the underlying sink if the throttle budget
// allows it. Behaviour when the budget is exceeded depends on the policy.
func (t *Throttle) Write(ctx context.Context, e *Event) error {
	for {
		t.mu.Lock()
		now := time.Now()
		if now.After(t.windowAt) {
			t.count = 0
			t.windowAt = now.Add(t.window)
		}
		if t.count < t.limit {
			t.count++
			t.mu.Unlock()
			return t.sink.Write(ctx, e)
		}
		if t.policy == ThrottleDrop {
			t.mu.Unlock()
			return nil
		}
		// Block: wait until the current window expires then retry.
		waitUntil := t.windowAt
		t.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Until(waitUntil)):
			// retry
		}
	}
}

// Close closes the underlying sink.
func (t *Throttle) Close() error {
	return t.sink.Close()
}

// Name returns the name of the underlying sink.
func (t *Throttle) Name() string {
	return t.sink.Name()
}
