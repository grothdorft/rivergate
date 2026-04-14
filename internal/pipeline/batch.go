package pipeline

import (
	"context"
	"sync"
	"time"
)

// Batch accumulates events and flushes them when either the size limit or
// the flush interval is reached.
type Batch struct {
	mu       sync.Mutex
	events   []*Event
	size     int
	interval time.Duration
	flushFn  func([]*Event)
}

// NewBatch creates a Batch that flushes when it reaches size events or after
// interval duration, whichever comes first. flushFn is called with the
// accumulated slice on each flush.
func NewBatch(size int, interval time.Duration, flushFn func([]*Event)) *Batch {
	if size <= 0 {
		size = 100
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Batch{
		size:     size,
		interval: interval,
		flushFn:  flushFn,
	}
}

// Add appends an event to the batch. If the batch reaches its size limit the
// flush function is called synchronously before returning.
func (b *Batch) Add(e *Event) {
	b.mu.Lock()
	b.events = append(b.events, e)
	ready := len(b.events) >= b.size
	var toFlush []*Event
	if ready {
		toFlush = b.events
		b.events = nil
	}
	b.mu.Unlock()
	if ready {
		b.flushFn(toFlush)
	}
}

// Flush drains the current batch immediately, regardless of size.
func (b *Batch) Flush() {
	b.mu.Lock()
	toFlush := b.events
	b.events = nil
	b.mu.Unlock()
	if len(toFlush) > 0 {
		b.flushFn(toFlush)
	}
}

// Run starts the interval-based flush loop. It blocks until ctx is cancelled.
func (b *Batch) Run(ctx context.Context) {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.Flush()
		case <-ctx.Done():
			b.Flush()
			return
		}
	}
}
