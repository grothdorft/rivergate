package pipeline

import "testing"

func TestEnricher_ProcessorIntegration(t *testing.T) {
	enricher := NewEnricher(
		StaticField("datacenter", "us-east-1"),
		CopyField("datacenter", "dc"),
	)

	transformer := NewTransformer(func(e *Event) {
		enricher.Enrich(e)
	})

	proc := NewProcessor("enrich-proc", nil, transformer)

	events := make(chan *Event, 1)
	out := make(chan *Event, 1)

	e := NewEvent("src")
	events <- e
	close(events)

	go func() {
		for ev := range events {
			result := proc.Process(ev)
			if result != nil {
				out <- result
			}
		}
		close(out)
	}()

	result := <-out
	if result == nil {
		t.Fatal("expected enriched event")
	}
	if v, ok := result.Get("datacenter"); !ok || v != "us-east-1" {
		t.Fatalf("datacenter mismatch: %v", v)
	}
	if v, ok := result.Get("dc"); !ok || v != "us-east-1" {
		t.Fatalf("dc mismatch: %v", v)
	}
}
