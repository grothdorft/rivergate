package pipeline

import (
	"testing"
	"time"
)

func TestNewEvent(t *testing.T) {
	before := time.Now().UTC()
	e := NewEvent("test-source", map[string]any{"level": "info", "msg": "hello"})
	after := time.Now().UTC()

	if e.Source != "test-source" {
		t.Errorf("expected source %q, got %q", "test-source", e.Source)
	}
	if e.Timestamp.Before(before) || e.Timestamp.After(after) {
		t.Errorf("timestamp %v out of expected range", e.Timestamp)
	}
	if e.Fields["level"] != "info" {
		t.Errorf("expected level info, got %v", e.Fields["level"])
	}
}

func TestEvent_Clone(t *testing.T) {
	orig := NewEvent("src", map[string]any{"key": "value"})
	clone := orig.Clone()

	if clone.Source != orig.Source {
		t.Errorf("clone source mismatch")
	}

	// Mutating clone should not affect original.
	clone.Fields["key"] = "modified"
	if orig.Fields["key"] != "value" {
		t.Errorf("clone mutation affected original")
	}
}

func TestEvent_SetGet(t *testing.T) {
	e := &Event{}
	e.Set("foo", 42)

	v, ok := e.Get("foo")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if v != 42 {
		t.Errorf("expected 42, got %v", v)
	}

	_, ok = e.Get("missing")
	if ok {
		t.Error("expected missing key to return false")
	}
}

func TestEvent_SetInitializesNilFields(t *testing.T) {
	e := &Event{}
	e.Set("k", "v")
	if e.Fields == nil {
		t.Error("Fields should not be nil after Set")
	}
}
