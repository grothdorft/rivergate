package pipeline

import (
	"testing"
)

func makeTransformEvent() *Event {
	return NewEvent("test-source", map[string]any{
		"host":    "web-01",
		"level":   "info",
		"message": "hello world",
	})
}

func TestSetField(t *testing.T) {
	e := makeTransformEvent()
	op := SetField("env", "production")
	op(e)
	if v, _ := e.Get("env"); v != "production" {
		t.Errorf("expected 'production', got %v", v)
	}
}

func TestDeleteField(t *testing.T) {
	e := makeTransformEvent()
	op := DeleteField("level")
	op(e)
	if _, ok := e.Get("level"); ok {
		t.Error("expected 'level' to be deleted")
	}
}

func TestRenameField(t *testing.T) {
	e := makeTransformEvent()
	op := RenameField("host", "hostname")
	op(e)
	if v, _ := e.Get("hostname"); v != "web-01" {
		t.Errorf("expected 'web-01', got %v", v)
	}
	if _, ok := e.Get("host"); ok {
		t.Error("expected 'host' to be removed after rename")
	}
}

func TestNewTransformer_AppliesAll(t *testing.T) {
	cfg := []map[string]string{
		{"type": "set", "field": "env", "value": "staging"},
		{"type": "delete", "field": "level"},
		{"type": "rename", "field": "host", "to": "hostname"},
	}
	transformer, err := NewTransformer(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := makeTransformEvent()
	transformer.Apply(e)

	if v, _ := e.Get("env"); v != "staging" {
		t.Errorf("expected env=staging, got %v", v)
	}
	if _, ok := e.Get("level"); ok {
		t.Error("expected level to be deleted")
	}
	if v, _ := e.Get("hostname"); v != "web-01" {
		t.Errorf("expected hostname=web-01, got %v", v)
	}
}

func TestNewTransformer_UnknownType(t *testing.T) {
	cfg := []map[string]string{
		{"type": "explode", "field": "x"},
	}
	_, err := NewTransformer(cfg)
	if err == nil {
		t.Error("expected error for unknown transform type")
	}
}

func TestNewTransformer_EmptyConfig(t *testing.T) {
	transformer, err := NewTransformer(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := makeTransformEvent()
	transformer.Apply(e)
	if v, _ := e.Get("host"); v != "web-01" {
		t.Error("event should be unchanged with no transforms")
	}
}
