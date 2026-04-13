package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestRateLimiter_Unlimited(t *testing.T) {
	rl := NewRateLimiter(0)
	defer rl.Close()
	for i := 0; i < 1000; i++ {
		if !rl.Allow() {
			t.Fatal("expected unlimited limiter to always allow")
		}
	}
}

func TestRateLimiter_AllowConsumesTokens(t *testing.T) {
	rl := NewRateLimiter(3)
	defer rl.Close()

	if !rl.Allow() {
		t.Fatal("expected first Allow to succeed")
	}
	if !rl.Allow() {
		t.Fatal("expected second Allow to succeed")
	}
	if !rl.Allow() {
		t.Fatal("expected third Allow to succeed")
	}
	if rl.Allow() {
		t.Fatal("expected fourth Allow to be denied after exhausting tokens")
	}
}

func TestRateLimiter_RefillsTokens(t *testing.T) {
	rl := NewRateLimiter(2)
	defer rl.Close()

	rl.Allow()
	rl.Allow()
	if rl.Allow() {
		t.Fatal("tokens should be exhausted")
	}

	// Wait for refill
	time.Sleep(1100 * time.Millisecond)

	if !rl.Allow() {
		t.Fatal("expected tokens to be refilled after 1 second")
	}
}

func TestRateLimiter_Wait_Succeeds(t *testing.T) {
	rl := NewRateLimiter(5)
	defer rl.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := rl.Wait(ctx); err != nil {
			t.Fatalf("Wait failed unexpectedly: %v", err)
		}
	}
}

func TestRateLimiter_Wait_RespectsContextCancel(t *testing.T) {
	rl := NewRateLimiter(1)
	defer rl.Close()

	// Exhaust the single token
	rl.Allow()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := rl.Wait(ctx)
	if err == nil {
		t.Fatal("expected Wait to return error when context is cancelled")
	}
}

func TestRateLimiter_Wait_Unlimited(t *testing.T) {
	rl := NewRateLimiter(0)
	defer rl.Close()

	ctx := context.Background()
	if err := rl.Wait(ctx); err != nil {
		t.Fatalf("Wait on unlimited limiter should not error: %v", err)
	}
}
