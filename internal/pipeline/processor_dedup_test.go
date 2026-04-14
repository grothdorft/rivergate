package pipeline

import (
	"testing"
	"time"
)

// TestProcessor_DedupIntegration verifies that a Processor can use a
// DedupWindow as its filter to drop duplicate events end-to-end.
func TestProcessor_DedupIntegration(t *testing.T) {
	window := NewDedupWindow(time.Second, []string{"msg"})

	// Wrap the dedup window as a FilterFunc so it integrates with Processor.
	filter := FilterFunc(func(e *Event) bool {
		return !window.IsDuplicate(e)
	})

	p := NewProcessor("dedup-proc", filter, nil)

	out := make(chan *Event, 4)

	send := func(msg string) {
		e := NewEvent("src")
		e.Set("msg", msg)
		p.Process(e, out)
	}

	send("hello")
	send("hello") // duplicate — should be dropped
	send("world")
	send("world") // duplicate — should be dropped
	send("hello") // still duplicate within TTL

	close(out)

	var got []string
	for e := range out {
		v, _ := e.Get("msg")
		got = append(got, v.(string))
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 unique events, got %d: %v", len(got), got)
	}
	if got[0] != "hello" || got[1] != "world" {
		t.Fatalf("unexpected events: %v", got)
	}
}
