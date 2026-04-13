package pipeline

import (
	"errors"
	"testing"

	"github.com/yourorg/rivergate/internal/config"
)

// mockSink is a test double for the Sink interface.
type mockSink struct {
	name   string
	events []*Event
	errOn  bool
}

func (m *mockSink) Write(e *Event) error {
	if m.errOn {
		return errors.New("mock write error")
	}
	m.events = append(m.events, e)
	return nil
}

func (m *mockSink) Name() string { return m.name }

func makeConfig(from string, to []string) *config.Config {
	return &config.Config{
		Routes: []config.Route{{From: from, To: to}},
	}
}

func TestRouter_RouteEvent(t *testing.T) {
	sink := &mockSink{name: "out"}
	r, err := NewRouter(makeConfig("app", []string{"out"}), map[string]Sink{"out": sink})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	e := NewEvent("app", map[string]any{"msg": "hello"})
	if err := r.Route(e); err != nil {
		t.Fatalf("route error: %v", err)
	}
	if len(sink.events) != 1 {
		t.Errorf("expected 1 event, got %d", len(sink.events))
	}
}

func TestRouter_UnknownSourceDrops(t *testing.T) {
	sink := &mockSink{name: "out"}
	r, _ := NewRouter(makeConfig("app", []string{"out"}), map[string]Sink{"out": sink})

	e := NewEvent("other", nil)
	if err := r.Route(e); err != nil {
		t.Fatalf("expected no error for unknown source, got %v", err)
	}
	if len(sink.events) != 0 {
		t.Error("expected no events delivered for unknown source")
	}
}

func TestRouter_UnknownSinkInConfig(t *testing.T) {
	_, err := NewRouter(makeConfig("app", []string{"missing"}), map[string]Sink{})
	if err == nil {
		t.Error("expected error for unknown sink, got nil")
	}
}

func TestRouter_SinkWriteError(t *testing.T) {
	sink := &mockSink{name: "out", errOn: true}
	r, _ := NewRouter(makeConfig("app", []string{"out"}), map[string]Sink{"out": sink})

	e := NewEvent("app", nil)
	if err := r.Route(e); err == nil {
		t.Error("expected error from failing sink")
	}
}
