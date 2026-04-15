package pipeline

import (
	"context"
	"sync"
)

// DeadLetterQueue collects events that could not be delivered after all retries.
type DeadLetterQueue struct {
	mu       sync.Mutex
	events   []*Event
	capacity int
	dropped  int64
}

// NewDeadLetterQueue creates a DeadLetterQueue with the given capacity.
// A capacity <= 0 means unlimited.
func NewDeadLetterQueue(capacity int) *DeadLetterQueue {
	return &DeadLetterQueue{capacity: capacity}
}

// Push adds an event to the dead-letter queue.
// If the queue is at capacity the event is dropped and the dropped counter incremented.
func (d *DeadLetterQueue) Push(_ context.Context, event *Event) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.capacity > 0 && len(d.events) >= d.capacity {
		d.dropped++
		return
	}
	d.events = append(d.events, event.Clone())
}

// Drain returns all queued events and clears the queue.
func (d *DeadLetterQueue) Drain() []*Event {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]*Event, len(d.events))
	copy(out, d.events)
	d.events = d.events[:0]
	return out
}

// Len returns the number of events currently in the queue.
func (d *DeadLetterQueue) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.events)
}

// Dropped returns the number of events dropped due to capacity limits.
func (d *DeadLetterQueue) Dropped() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dropped
}
