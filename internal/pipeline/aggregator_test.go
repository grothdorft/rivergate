package pipeline

import (
	"testing"
	"time"
)

func makeAggEvent(key, msg string) *Event {
	e := NewEvent("test")
	e.Set("key", key)
	e.Set("msg", msg)
	return e
}

func countAgg(events []*Event) *Event {
	if len(events) == 0 {
		return nil
	}
	out := events[0].Clone()
	out.Set("count", len(events))
	return out
}

func TestAggregator_GroupsByKey(t *testing.T) {
	a := NewAggregator("key", 50*time.Millisecond, countAgg)
	stop := make(chan struct{})
	go a.Run(stop)

	a.Add(makeAggEvent("a", "1"))
	a.Add(makeAggEvent("a", "2"))
	a.Add(makeAggEvent("b", "3"))

	time.Sleep(80 * time.Millisecond)

	var results []*Event
	for len(a.Out()) > 0 {
		results = append(results, <-a.Out())
	}
	close(stop)
	// drain remaining
	for e := range a.Out() {
		results = append(results, e)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 aggregated events, got %d", len(results))
	}
}

func TestAggregator_CountIsCorrect(t *testing.T) {
	a := NewAggregator("key", 200*time.Millisecond, countAgg)
	stop := make(chan struct{})
	go a.Run(stop)

	for i := 0; i < 5; i++ {
		a.Add(makeAggEvent("x", "v"))
	}
	close(stop)

	var got *Event
	for e := range a.Out() {
		if v, _ := e.Get("key"); v == "x" {
			got = e
		}
	}
	if got == nil {
		t.Fatal("no aggregated event for key x")
	}
	count, _ := got.Get("count")
	if count.(int) != 5 {
		t.Fatalf("expected count 5, got %v", count)
	}
}

func TestAggregator_EmptyBucketSkipped(t *testing.T) {
	a := NewAggregator("key", 30*time.Millisecond, countAgg)
	stop := make(chan struct{})
	go a.Run(stop)
	close(stop)
	var results []*Event
	for e := range a.Out() {
		results = append(results, e)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results on empty aggregator, got %d", len(results))
	}
}
