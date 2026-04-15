package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

// alwaysFailSink always returns an error on Write.
type alwaysFailSink struct{ closed bool }

func (a *alwaysFailSink) Name() string                              { return "fail" }
func (a *alwaysFailSink) Write(_ context.Context, _ *Event) error  { return errors.New("permanent failure") }
func (a *alwaysFailSink) Close() error                             { a.closed = true; return nil }

func TestRetryWriter_FallsIntoDeadLetterQueue(t *testing.T) {
	sink := &alwaysFailSink{}
	policy := RetryPolicy{
		MaxAttempts:  2,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
		MaxDelay:     5 * time.Millisecond,
	}
	retryWriter := NewRetryWriter(sink, policy)
	dlq := NewDeadLetterQueue(100)

	event := NewEvent("integration", "test message")
	ctx := context.Background()

	if err := retryWriter.Write(ctx, event); err != nil {
		dlq.Push(ctx, event)
	}

	if dlq.Len() != 1 {
		t.Fatalf("expected 1 event in DLQ, got %d", dlq.Len())
	}

	events := dlq.Drain()
	if events[0].Message != event.Message {
		t.Errorf("expected message %q, got %q", event.Message, events[0].Message)
	}
	if events[0].Source != event.Source {
		t.Errorf("expected source %q, got %q", event.Source, events[0].Source)
	}
}

func TestRetryWriter_NoDLQEntryOnSuccess(t *testing.T) {
	sink := &countingSink{failN: 0}
	retryWriter := NewRetryWriter(sink, DefaultRetryPolicy())
	dlq := NewDeadLetterQueue(100)

	ctx := context.Background()
	if err := retryWriter.Write(ctx, NewEvent("src", "ok")); err != nil {
		dlq.Push(ctx, NewEvent("src", "ok"))
	}

	if dlq.Len() != 0 {
		t.Fatalf("expected empty DLQ on success, got %d events", dlq.Len())
	}
}
