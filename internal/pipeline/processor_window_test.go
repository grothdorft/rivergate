package pipeline

import (
	"testing"
	"time"
)

func TestWindow_ProcessorIntegration(t *testing.T) {
	var flushed []*Event
	win := NewWindow(time.Hour, func(events []*Event) {
		flushed = append(flushed, events...)
	})

	p := NewProcessor("win-proc", nil, nil)

	events := []*Event{
		NewEvent("src"),
		NewEvent("src"),
		NewEvent("src"),
	}
	for _, e := range events {
		out := p.Process(e)
		if out != nil {
			win.Add(out)
		}
	}
	win.Flush()

	if len(flushed) != 3 {
		t.Fatalf("expected 3 flushed events, got %d", len(flushed))
	}
}
