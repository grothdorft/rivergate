package pipeline

import (
	"sync"
	"time"
)

// WindowFunc is called with all events in the window when it flushes.
type WindowFunc func(events []*Event)

// Window collects events over a fixed time duration and flushes them periodically.
type Window struct {
	mu       sync.Mutex
	duration time.Duration
	events   []*Event
	flushFn  WindowFunc
	stop     chan struct{}
}

// NewWindow creates a Window that flushes every d using fn.
func NewWindow(d time.Duration, fn WindowFunc) *Window {
	return &Window{
		duration: d,
		flushFn:  fn,
		stop:     make(chan struct{}),
	}
}

// Add appends an event to the current window.
func (w *Window) Add(e *Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, e.Clone())
}

// Flush drains the current window and invokes the flush function.
func (w *Window) Flush() {
	w.mu.Lock()
	events := w.events
	w.events = nil
	w.mu.Unlock()
	if len(events) > 0 {
		w.flushFn(events)
	}
}

// Run starts the periodic flush loop; blocks until Stop is called.
func (w *Window) Run() {
	ticker := time.NewTicker(w.duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.Flush()
		case <-w.stop:
			w.Flush()
			return
		}
	}
}

// Stop halts the flush loop.
func (w *Window) Stop() {
	close(w.stop)
}
