package pipeline

import (
	"testing"
	"time"
)

// TestReorderBuffer_ProcessorIntegration verifies that events pushed through
// a Processor and collected by a ReorderBuffer arrive in sorted order even
// when inserted out of order.
func TestReorderBuffer_ProcessorIntegration(t *testing.T) {
	rb := NewReorderBuffer("seq", 60*time.Millisecond)
	go rb.Run()
	defer rb.Stop()

	// Build a no-op processor (nil filter, nil transformer) and feed it events.
	p := NewProcessor("reorder-proc", nil, nil)

	events := []*Event{
		makeReorderEvent(5, "e"),
		makeReorderEvent(3, "c"),
		makeReorderEvent(1, "a"),
		makeReorderEvent(4, "d"),
		makeReorderEvent(2, "b"),
	}

	for _, e := range events {
		out, err := p.Process(e)
		if err != nil {
			t.Fatalf("processor error: %v", err)
		}
		if out != nil {
			rb.Push(out)
		}
	}

	select {
	case batch := <-rb.Output():
		if len(batch) != 5 {
			t.Fatalf("expected 5 sorted events, got %d", len(batch))
		}
		for i := 0; i < len(batch)-1; i++ {
			vi, _ := batch[i].Get("seq")
			vj, _ := batch[i+1].Get("seq")
			fi, _ := toFloat(vi)
			fj, _ := toFloat(vj)
			if fi > fj {
				t.Errorf("out of order at index %d: seq %v > %v", i, fi, fj)
			}
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for reorder batch in processor integration")
	}
}
