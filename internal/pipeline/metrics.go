package pipeline

import (
	"sync"
	"sync/atomic"
)

// Metrics tracks runtime counters for the pipeline.
type Metrics struct {
	mu       sync.RWMutex
	counters map[string]*atomic.Int64
}

// NewMetrics creates an initialised Metrics instance.
func NewMetrics() *Metrics {
	return &Metrics{
		counters: make(map[string]*atomic.Int64),
	}
}

// Inc increments the named counter by 1.
func (m *Metrics) Inc(name string) {
	m.counter(name).Add(1)
}

// Add adds delta to the named counter.
func (m *Metrics) Add(name string, delta int64) {
	m.counter(name).Add(delta)
}

// Get returns the current value of the named counter.
func (m *Metrics) Get(name string) int64 {
	m.mu.RLock()
	c, ok := m.counters[name]
	m.mu.RUnlock()
	if !ok {
		return 0
	}
	return c.Load()
}

// Snapshot returns a point-in-time copy of all counters.
func (m *Metrics) Snapshot() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]int64, len(m.counters))
	for k, v := range m.counters {
		out[k] = v.Load()
	}
	return out
}

// Reset sets every counter back to zero.
func (m *Metrics) Reset() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, v := range m.counters {
		v.Store(0)
	}
}

// counter returns (creating if necessary) the atomic for name.
func (m *Metrics) counter(name string) *atomic.Int64 {
	m.mu.RLock()
	c, ok := m.counters[name]
	m.mu.RUnlock()
	if ok {
		return c
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok = m.counters[name]; ok {
		return c
	}
	c = &atomic.Int64{}
	m.counters[name] = c
	return c
}
