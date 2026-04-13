package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestBackpressureWriter_PolicyDrop(t *testing.T) {
	buf := NewBuffer(2)
	w := NewBackpressureWriter(buf, PolicyDrop, 0)
	ctx := context.Background()

	if !w.Write(ctx, makeBufferEvent("1")) {
		t.Error("expected first write to succeed")
	}
	if !w.Write(ctx, makeBufferEvent("2")) {
		t.Error("expected second write to succeed")
	}
	if w.Write(ctx, makeBufferEvent("3")) {
		t.Error("expected third write to be dropped")
	}
	if buf.Dropped() != 1 {
		t.Errorf("expected 1 dropped event, got %d", buf.Dropped())
	}
}

func TestBackpressureWriter_PolicyBlock_AcceptsWhenSpaceFrees(t *testing.T) {
	buf := NewBuffer(1)
	w := NewBackpressureWriter(buf, PolicyBlock, 2*time.Millisecond)
	ctx := context.Background()

	// Fill the buffer.
	w.Write(ctx, makeBufferEvent("a"))

	// Unblock by consuming after a short delay.
	go func() {
		time.Sleep(10 * time.Millisecond)
		buf.Pop()
	}()

	start := time.Now()
	accepted := w.Write(ctx, makeBufferEvent("b"))
	if !accepted {
		t.Error("expected Write to eventually succeed")
	}
	if time.Since(start) < 5*time.Millisecond {
		t.Error("expected Write to have blocked")
	}
}

func TestBackpressureWriter_PolicyBlock_RespectsContextCancel(t *testing.T) {
	buf := NewBuffer(1)
	w := NewBackpressureWriter(buf, PolicyBlock, 2*time.Millisecond)

	// Fill the buffer so the next write will block.
	w.Write(context.Background(), makeBufferEvent("x"))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	accepted := w.Write(ctx, makeBufferEvent("y"))
	if accepted {
		t.Error("expected Write to fail after context cancellation")
	}
}

func TestBackpressureWriter_DefaultPollRate(t *testing.T) {
	buf := NewBuffer(4)
	w := NewBackpressureWriter(buf, PolicyBlock, 0)
	if w.pollRate != 5*time.Millisecond {
		t.Errorf("expected default poll rate 5ms, got %v", w.pollRate)
	}
}
