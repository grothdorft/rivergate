package pipeline

import (
	"testing"
)

func makeEnrichEvent(fields map[string]any) *Event {
	e := NewEvent("test")
	for k, v := range fields {
		e.Set(k, v)
	}
	return e
}

func TestStaticField(t *testing.T) {
	e := makeEnrichEvent(nil)
	en := NewEnricher(StaticField("env", "production"))
	en.Enrich(e)
	v, ok := e.Get("env")
	if !ok || v != "production" {
		t.Fatalf("expected env=production, got %v", v)
	}
}

func TestUpperCaseField(t *testing.T) {
	e := makeEnrichEvent(map[string]any{"level": "warn"})
	en := NewEnricher(UpperCaseField("level"))
	en.Enrich(e)
	v, _ := e.Get("level")
	if v != "WARN" {
		t.Fatalf("expected WARN, got %v", v)
	}
}

func TestUpperCaseField_NonString(t *testing.T) {
	e := makeEnrichEvent(map[string]any{"count": 42})
	en := NewEnricher(UpperCaseField("count"))
	en.Enrich(e) // should not panic
	v, _ := e.Get("count")
	if v != 42 {
		t.Fatalf("expected 42, got %v", v)
	}
}

func TestCopyField(t *testing.T) {
	e := makeEnrichEvent(map[string]any{"host": "web-01"})
	en := NewEnricher(CopyField("host", "source_host"))
	en.Enrich(e)
	v, ok := e.Get("source_host")
	if !ok || v != "web-01" {
		t.Fatalf("expected source_host=web-01, got %v", v)
	}
}

func TestCopyField_MissingSrc(t *testing.T) {
	e := makeEnrichEvent(nil)
	en := NewEnricher(CopyField("missing", "dst"))
	en.Enrich(e)
	_, ok := e.Get("dst")
	if ok {
		t.Fatal("expected dst to be absent")
	}
}

func TestEnricher_AppliesAll(t *testing.T) {
	e := makeEnrichEvent(map[string]any{"svc": "api"})
	en := NewEnricher(
		StaticField("env", "staging"),
		UpperCaseField("svc"),
		CopyField("svc", "service"),
	)
	en.Enrich(e)
	if v, _ := e.Get("env"); v != "staging" {
		t.Fatalf("env mismatch: %v", v)
	}
	if v, _ := e.Get("svc"); v != "API" {
		t.Fatalf("svc mismatch: %v", v)
	}
	if v, _ := e.Get("service"); v != "API" {
		t.Fatalf("service mismatch: %v", v)
	}
}
