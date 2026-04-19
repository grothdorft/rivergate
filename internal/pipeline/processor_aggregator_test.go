package pipeline

import (
	"testing"
	"time"
)

func TestAggregator_ProcessorIntegration(t *testing.T) {
	// Build an aggregator that sums events by key, then feed results into a processor.
	agg := NewAggregator("service", 60*time.Millisecond, func(events []*Event) *Event {
		out := events[0].Clone()
		out.Set("total", len(events))
		return out
	})

	stop := make(chan struct{})
	go agg.Run(stop)

	for i := 0; i < 3; i++ {
		e := NewEvent("src")
		e.Set("service", "auth")
		agg.Add(e)
	}

	close(stop)

	var found *Event
	for e := range agg.Out() {
		if v, _ := e.Get("service"); v == "auth" {
			found = e
		}
	}

	if found == nil {
		t.Fatal("expected aggregated event for service=auth")
	}
	total, _ := found.Get("total")
	if total.(int) != 3 {
		t.Fatalf("expected total=3, got %v", total)
	}

	// Pass through a no-op processor to verify compatibility.
	p := NewProcessor("agg-proc", nil, nil)
	result := p.Process(found)
	if result == nil {
		t.Fatal("processor dropped aggregated event unexpectedly")
	}
}
