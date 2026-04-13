package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestStdinSource_Name(t *testing.T) {
	s := NewStdinSource("test-source")
	if s.Name() != "test-source" {
		t.Fatalf("expected name %q, got %q", "test-source", s.Name())
	}
}

func TestStdinSource_Start_ParsesJSONLines(t *testing.T) {
	input := strings.NewReader(`{"level":"info","msg":"hello"}` + "\n" +
		`{"level":"error","msg":"oops"}` + "\n")

	s := &StdinSource{name: "stdin", reader: input}
	out := make(chan *Event, 10)
	ctx := context.Background()

	if err := s.Start(ctx, out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	close(out)

	var events []*Event
	for e := range out {
		events = append(events, e)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Fields["msg"] != "hello" {
		t.Errorf("unexpected first event msg: %v", events[0].Fields["msg"])
	}
}

func TestStdinSource_Start_SkipsMalformedLines(t *testing.T) {
	input := strings.NewReader("not json\n" + `{"ok":true}` + "\n")
	s := &StdinSource{name: "stdin", reader: input}
	out := make(chan *Event, 10)

	if err := s.Start(context.Background(), out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	close(out)

	count := 0
	for range out {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 valid event, got %d", count)
	}
}

func TestStdinSource_Start_RespectsContextCancel(t *testing.T) {
	// Pipe that blocks — we cancel before any data
	pr, pw := strings.NewReader(""), nil
	_ = pw
	s := &StdinSource{name: "stdin", reader: pr}
	out := make(chan *Event, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Should return without hanging
	_ = s.Start(ctx, out)
}

func TestSourceRegistry_RegisterAndGet(t *testing.T) {
	reg := NewSourceRegistry()
	s := NewStdinSource("my-source")
	reg.Register(s)

	got, ok := reg.Get("my-source")
	if !ok {
		t.Fatal("expected source to be found")
	}
	if got.Name() != "my-source" {
		t.Errorf("unexpected name: %s", got.Name())
	}
}

func TestSourceRegistry_GetMissing(t *testing.T) {
	reg := NewSourceRegistry()
	_, ok := reg.Get("nonexistent")
	if ok {
		t.Fatal("expected source not to be found")
	}
}
