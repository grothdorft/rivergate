package pipeline

import (
	"sort"
	"sync"
	"time"
)

// ReorderBuffer collects events over a window and emits them sorted by a
// numeric field (e.g. a sequence number or timestamp millis). Events that
// arrive after the window closes are dropped.
type ReorderBuffer struct {
	mu       sync.Mutex
	field    string
	window   time.Duration
	bucket   []*Event
	ticker   *time.Ticker
	output   chan []*Event
	stopCh   chan struct{}
	stopped  bool
}

// NewReorderBuffer creates a ReorderBuffer that groups events by field,
// flushes sorted slices every window duration, and sends them on the
// returned read-only channel.
func NewReorderBuffer(field string, window time.Duration) *ReorderBuffer {
	if field == "" {
		field = "seq"
	}
	if window <= 0 {
		window = time.Second
	}
	return &ReorderBuffer{
		field:  field,
		window: window,
		bucket: make([]*Event, 0, 64),
		output: make(chan []*Event, 8),
		stopCh: make(chan struct{}),
	}
}

// Push adds an event to the current accumulation bucket.
func (r *ReorderBuffer) Push(e *Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	r.bucket = append(r.bucket, e)
}

// Output returns the channel on which sorted batches are delivered.
func (r *ReorderBuffer) Output() <-chan []*Event {
	return r.output
}

// Run starts the flush loop. It blocks until Stop is called.
func (r *ReorderBuffer) Run() {
	r.ticker = time.NewTicker(r.window)
	defer r.ticker.Stop()
	for {
		select {
		case <-r.ticker.C:
			r.flush()
		case <-r.stopCh:
			r.flush()
			return
		}
	}
}

// Stop signals the run loop to flush remaining events and exit.
func (r *ReorderBuffer) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.mu.Unlock()
	close(r.stopCh)
}

func (r *ReorderBuffer) flush() {
	r.mu.Lock()
	if len(r.bucket) == 0 {
		r.mu.Unlock()
		return
	}
	batch := r.bucket
	r.bucket = make([]*Event, 0, 64)
	r.mu.Unlock()

	sort.Slice(batch, func(i, j int) bool {
		vi, _ := batch[i].Get(r.field)
		vj, _ := batch[j].Get(r.field)
		fi, _ := toFloat(vi)
		fj, _ := toFloat(vj)
		return fi < fj
	})
	r.output <- batch
}

func toFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint64:
		return float64(n), true
	}
	return 0, false
}
