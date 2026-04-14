package pipeline

import (
	"sync"
	"testing"
	"time"
)

// TestBatch_ProcessorIntegration verifies that a Processor and Batch work
// together: processed events are accumulated and flushed as a group.
func TestBatch_ProcessorIntegration(t *testing.T) {
	var mu sync.Mutex
	var received []*Event

	batch := NewBatch(3, 10*time.Second, func(evs []*Event) {
		mu.Lock()
		received = append(received, evs...)
		mu.Unlock()
	})

	setTransform := NewTransformer([]TransformRule{
		{Op: SetField, Field: "processed", Value: "true"},
	})
	proc := NewProcessor("batcher", nil, setTransform)

	for _, msg := range []string{"one", "two", "three"} {
		e := NewEvent("src")
		e.Set("message", msg)
		out := proc.Process(e)
		if out != nil {
			batch.Add(out)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("expected 3 events flushed, got %d", len(received))
	}
	for _, e := range received {
		v, ok := e.Get("processed")
		if !ok || v != "true" {
			t.Errorf("event missing processed field: %+v", e)
		}
	}
}
