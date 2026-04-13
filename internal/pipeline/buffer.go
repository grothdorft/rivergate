package pipeline

import (
	"sync"
)

// Buffer is a bounded, thread-safe in-memory queue of Events.
type Buffer struct {
	mu       sync.Mutex
	events   []*Event
	capacity int
	dropped  int64
}

// NewBuffer creates a Buffer with the given capacity.
// If capacity <= 0, it defaults to 1024.
func NewBuffer(capacity int) *Buffer {
	if capacity <= 0 {
		capacity = 1024
	}
	return &Buffer{
		events:   make([]*Event, 0, capacity),
		capacity: capacity,
	}
}

// Push adds an event to the buffer. If the buffer is full the event is
// dropped and the dropped counter is incremented. Returns true if the
// event was accepted.
func (b *Buffer) Push(e *Event) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) >= b.capacity {
		b.dropped++
		return false
	}
	b.events = append(b.events, e)
	return true
}

// Pop removes and returns the oldest event, or nil if the buffer is empty.
func (b *Buffer) Pop() *Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) == 0 {
		return nil
	}
	e := b.events[0]
	b.events = b.events[1:]
	return e
}

// Len returns the current number of events in the buffer.
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events)
}

// Dropped returns the total number of events dropped due to capacity overflow.
func (b *Buffer) Dropped() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped
}

// Flush returns all buffered events and clears the buffer.
func (b *Buffer) Flush() []*Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*Event, len(b.events))
	copy(out, b.events)
	b.events = b.events[:0]
	return out
}
