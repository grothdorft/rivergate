package pipeline

import (
	"context"
	"time"
)

// BackpressurePolicy controls what happens when a Buffer is full.
type BackpressurePolicy int

const (
	// PolicyDrop silently discards the event.
	PolicyDrop BackpressurePolicy = iota
	// PolicyBlock waits until space is available or the context is cancelled.
	PolicyBlock
)

// BackpressureWriter wraps a Buffer and applies a BackpressurePolicy on Push.
type BackpressureWriter struct {
	buf      *Buffer
	policy   BackpressurePolicy
	pollRate time.Duration
}

// NewBackpressureWriter creates a BackpressureWriter.
// pollRate is used only for PolicyBlock to control how often availability is
// checked; if zero it defaults to 5 ms.
func NewBackpressureWriter(buf *Buffer, policy BackpressurePolicy, pollRate time.Duration) *BackpressureWriter {
	if pollRate <= 0 {
		pollRate = 5 * time.Millisecond
	}
	return &BackpressureWriter{buf: buf, policy: policy, pollRate: pollRate}
}

// Write attempts to push e into the buffer according to the policy.
// Returns true if the event was accepted, false if it was dropped or the
// context was cancelled before space became available.
func (w *BackpressureWriter) Write(ctx context.Context, e *Event) bool {
	switch w.policy {
	case PolicyBlock:
		for {
			if w.buf.Push(e) {
				return true
			}
			select {
			case <-ctx.Done():
				return false
			case <-time.After(w.pollRate):
			}
		}
	default: // PolicyDrop
		return w.buf.Push(e)
	}
}
