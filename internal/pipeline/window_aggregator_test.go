package pipeline

import (
	"testing"
	"time"
)

func TestWindow_AggregatorIntegration(t *testing.T) {
	results := make(map[string]float64)

	agg := NewAggregator("env", func(key string, events []*Event) {
		results[key] = float64(len(events))
	})

	win := NewWindow(time.Hour, func(events []*Event) {
		for _, e := range events {
			agg.Add(e)
		}
		agg.Flush()
	})

	for i := 0; i < 3; i++ {
		e := NewEvent("src")
		e.Set("env", "prod")
		win.Add(e)
	}
	e2 := NewEvent("src")
	e2.Set("env", "staging")
	win.Add(e2)

	win.Flush()

	if results["prod"] != 3 {
		t.Fatalf("expected prod=3, got %v", results["prod"])
	}
	if results["staging"] != 1 {
		t.Fatalf("expected staging=1, got %v", results["staging"])
	}
}
