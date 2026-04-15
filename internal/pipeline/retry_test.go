package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

type countingSink struct {
	calls  int
	failN  int // fail the first N calls
	closed bool
}

func (c *countingSink) Name() string { return "counting" }
func (c *countingSink) Write(_ context.Context, _ *Event) error {
	c.calls++
	if c.calls <= c.failN {
		return errors.New("transient error")
	}
	return nil
}
func (c *countingSink) Close() error { c.closed = true; return nil }

func TestRetryWriter_SucceedsOnFirstAttempt(t *testing.T) {
	sink := &countingSink{failN: 0}
	w := NewRetryWriter(sink, DefaultRetryPolicy())
	err := w.Write(context.Background(), NewEvent("src", "msg"))
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if sink.calls != 1 {
		t.Fatalf("expected 1 call, got %d", sink.calls)
	}
}

func TestRetryWriter_RetriesAndSucceeds(t *testing.T) {
	sink := &countingSink{failN: 2}
	policy := RetryPolicy{MaxAttempts: 3, InitialDelay: time.Millisecond, Multiplier: 1.0, MaxDelay: 10 * time.Millisecond}
	w := NewRetryWriter(sink, policy)
	err := w.Write(context.Background(), NewEvent("src", "msg"))
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if sink.calls != 3 {
		t.Fatalf("expected 3 calls, got %d", sink.calls)
	}
}

func TestRetryWriter_ExhaustsRetries(t *testing.T) {
	sink := &countingSink{failN: 10}
	policy := RetryPolicy{MaxAttempts: 3, InitialDelay: time.Millisecond, Multiplier: 1.0, MaxDelay: 5 * time.Millisecond}
	w := NewRetryWriter(sink, policy)
	err := w.Write(context.Background(), NewEvent("src", "msg"))
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if sink.calls != 3 {
		t.Fatalf("expected 3 calls, got %d", sink.calls)
	}
}

func TestRetryWriter_RespectsContextCancel(t *testing.T) {
	sink := &countingSink{failN: 10}
	policy := RetryPolicy{MaxAttempts: 5, InitialDelay: 200 * time.Millisecond, Multiplier: 1.0, MaxDelay: time.Second}
	w := NewRetryWriter(sink, policy)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	err := w.Write(ctx, NewEvent("src", "msg"))
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestRetryWriter_Close(t *testing.T) {
	sink := &countingSink{}
	w := NewRetryWriter(sink, DefaultRetryPolicy())
	if err := w.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if !sink.closed {
		t.Fatal("expected underlying sink to be closed")
	}
}

func TestDefaultRetryPolicy_Values(t *testing.T) {
	p := DefaultRetryPolicy()
	if p.MaxAttempts != 3 {
		t.Errorf("expected MaxAttempts=3, got %d", p.MaxAttempts)
	}
	if p.Multiplier != 2.0 {
		t.Errorf("expected Multiplier=2.0, got %f", p.Multiplier)
	}
}
