package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

// countingSink records how many events it receives.
type countingSink struct {
	mu    sync.Mutex
	count int
}

func (c *countingSink) Write(_ context.Context, _ *Event) error {
	c.mu.Lock()
	c.count++
	c.mu.Unlock()
	return nil
}
func (c *countingSink) Close() error  { return nil }
func (c *countingSink) Name() string  { return "counting" }
func (c *countingSink) n() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func makeThrottleEvent(msg string) *Event {
	e := NewEvent("test")
	e.Set("msg", msg)
	return e
}

func TestThrottle_DropPolicy_LimitsEvents(t *testing.T) {
	sink := &countingSink{}
	th := NewThrottle(sink, 3, time.Second, ThrottleDrop)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_ = th.Write(ctx, makeThrottleEvent("x"))
	}

	if sink.n() != 3 {
		t.Fatalf("expected 3 events forwarded, got %d", sink.n())
	}
}

func TestThrottle_DropPolicy_ResetsAfterWindow(t *testing.T) {
	sink := &countingSink{}
	th := NewThrottle(sink, 2, 50*time.Millisecond, ThrottleDrop)

	ctx := context.Background()
	for i := 0; i < 4; i++ {
		_ = th.Write(ctx, makeThrottleEvent("x"))
	}
	// Only 2 should pass in the first window.
	if sink.n() != 2 {
		t.Fatalf("expected 2 before window reset, got %d", sink.n())
	}

	time.Sleep(60 * time.Millisecond)

	for i := 0; i < 2; i++ {
		_ = th.Write(ctx, makeThrottleEvent("y"))
	}
	if sink.n() != 4 {
		t.Fatalf("expected 4 total after window reset, got %d", sink.n())
	}
}

func TestThrottle_BlockPolicy_AcceptsAfterWindow(t *testing.T) {
	sink := &countingSink{}
	th := NewThrottle(sink, 1, 50*time.Millisecond, ThrottleBlock)

	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 2; i++ {
		_ = th.Write(ctx, makeThrottleEvent("z"))
	}
	elapsed := time.Since(start)

	if sink.n() != 2 {
		t.Fatalf("expected 2 events, got %d", sink.n())
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected blocking delay, elapsed=%v", elapsed)
	}
}

func TestThrottle_BlockPolicy_RespectsContextCancel(t *testing.T) {
	sink := &countingSink{}
	th := NewThrottle(sink, 1, 5*time.Second, ThrottleBlock)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_ = th.Write(ctx, makeThrottleEvent("first")) // consumes budget
	err := th.Write(ctx, makeThrottleEvent("second")) // should block then ctx cancel

	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}
