package pipeline

import (
	"context"
	"sync"
	"time"
)

// RateLimiter limits the number of events processed per second.
type RateLimiter struct {
	mu       sync.Mutex
	rate     int
	tokens   int
	last     time.Time
	ticker   *time.Ticker
	stopCh   chan struct{}
}

// NewRateLimiter creates a RateLimiter that allows up to rate events per second.
// A rate <= 0 disables limiting (always allows).
func NewRateLimiter(rate int) *RateLimiter {
	rl := &RateLimiter{
		rate:   rate,
		tokens: rate,
		last:   time.Now(),
		stopCh: make(chan struct{}),
	}
	if rate > 0 {
		rl.ticker = time.NewTicker(time.Second)
		go rl.refill()
	}
	return rl
}

// refill restores tokens to the configured rate every second.
func (rl *RateLimiter) refill() {
	for {
		select {
		case <-rl.ticker.C:
			rl.mu.Lock()
			rl.tokens = rl.rate
			rl.mu.Unlock()
		case <-rl.stopCh:
			return
		}
	}
}

// Allow returns true if an event is permitted under the current rate limit.
// If rate <= 0, it always returns true.
func (rl *RateLimiter) Allow() bool {
	if rl.rate <= 0 {
		return true
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if rl.tokens > 0 {
		rl.tokens--
		return true
	}
	return false
}

// Wait blocks until a token is available or the context is cancelled.
func (rl *RateLimiter) Wait(ctx context.Context) error {
	if rl.rate <= 0 {
		return nil
	}
	for {
		rl.mu.Lock()
		if rl.tokens > 0 {
			rl.tokens--
			rl.mu.Unlock()
			return nil
		}
		rl.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Close stops the background refill goroutine.
func (rl *RateLimiter) Close() {
	if rl.ticker != nil {
		rl.ticker.Stop()
		close(rl.stopCh)
	}
}
