package pipeline

import (
	"sync"
	"testing"
)

func TestMetrics_IncAndGet(t *testing.T) {
	m := NewMetrics()
	m.Inc("events_in")
	m.Inc("events_in")
	if got := m.Get("events_in"); got != 2 {
		t.Fatalf("expected 2, got %d", got)
	}
}

func TestMetrics_Add(t *testing.T) {
	m := NewMetrics()
	m.Add("bytes", 512)
	m.Add("bytes", 256)
	if got := m.Get("bytes"); got != 768 {
		t.Fatalf("expected 768, got %d", got)
	}
}

func TestMetrics_GetMissingReturnsZero(t *testing.T) {
	m := NewMetrics()
	if got := m.Get("nonexistent"); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestMetrics_Snapshot(t *testing.T) {
	m := NewMetrics()
	m.Inc("a")
	m.Add("b", 5)
	snap := m.Snapshot()
	if snap["a"] != 1 {
		t.Errorf("snapshot a: expected 1, got %d", snap["a"])
	}
	if snap["b"] != 5 {
		t.Errorf("snapshot b: expected 5, got %d", snap["b"])
	}
	// Mutating after snapshot should not affect the copy.
	m.Inc("a")
	if snap["a"] != 1 {
		t.Errorf("snapshot should be immutable, got %d", snap["a"])
	}
}

func TestMetrics_Reset(t *testing.T) {
	m := NewMetrics()
	m.Inc("x")
	m.Inc("x")
	m.Reset()
	if got := m.Get("x"); got != 0 {
		t.Fatalf("expected 0 after reset, got %d", got)
	}
}

func TestMetrics_ConcurrentInc(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	const goroutines = 50
	const incsEach = 100
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < incsEach; j++ {
				m.Inc("concurrent")
			}
		}()
	}
	wg.Wait()
	expected := int64(goroutines * incsEach)
	if got := m.Get("concurrent"); got != expected {
		t.Fatalf("expected %d, got %d", expected, got)
	}
}
