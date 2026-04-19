package pipeline

import (
	"sync"
	"time"
)

// AggregateFunc reduces a slice of events into a single output event.
type AggregateFunc func(events []*Event) *Event

// Aggregator collects events by a key field and periodically emits aggregated results.
type Aggregator struct {
	keyField string
	window   time.Duration
	aggFn    AggregateFunc
	buckets  map[string][]*Event
	mu       sync.Mutex
	out      chan *Event
}

// NewAggregator creates an Aggregator that groups events by keyField over window duration.
func NewAggregator(keyField string, window time.Duration, fn AggregateFunc) *Aggregator {
	return &Aggregator{
		keyField: keyField,
		window:   window,
		aggFn:    fn,
		buckets:  make(map[string][]*Event),
		out:      make(chan *Event, 64),
	}
}

// Add places an event into the appropriate bucket.
func (a *Aggregator) Add(e *Event) {
	key, _ := e.Get(a.keyField)
	k, _ := key.(string)
	a.mu.Lock()
	a.buckets[k] = append(a.buckets[k], e.Clone())
	a.mu.Unlock()
}

// flush emits one aggregated event per bucket and resets state.
func (a *Aggregator) flush() {
	a.mu.Lock()
	buckets := a.buckets
	a.buckets = make(map[string][]*Event)
	a.mu.Unlock()
	for _, events := range buckets {
		if len(events) == 0 {
			continue
		}
		if result := a.aggFn(events); result != nil {
			select {
			case a.out <- result:
			default:
			}
		}
	}
}

// Run starts the flush ticker. Call in a goroutine; stops when stop is closed.
func (a *Aggregator) Run(stop <-chan struct{}) {
	ticker := time.NewTicker(a.window)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.flush()
		case <-stop:
			a.flush()
			close(a.out)
			return
		}
	}
}

// Out returns the channel of aggregated events.
func (a *Aggregator) Out() <-chan *Event { return a.out }
