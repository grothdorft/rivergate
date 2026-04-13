package pipeline

import (
	"testing"
)

func makeEvent(fields map[string]any) *Event {
	e := NewEvent("test", nil)
	for k, v := range fields {
		e.Set(k, v)
	}
	return e
}

func TestFilterRule_Equals(t *testing.T) {
	r, err := NewFilterRule("level", OpEquals, "error")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !r.Match(makeEvent(map[string]any{"level": "error"})) {
		t.Error("expected match")
	}
	if r.Match(makeEvent(map[string]any{"level": "info"})) {
		t.Error("expected no match")
	}
}

func TestFilterRule_NotEquals(t *testing.T) {
	r, _ := NewFilterRule("level", OpNotEquals, "debug")
	if !r.Match(makeEvent(map[string]any{"level": "info"})) {
		t.Error("expected match for neq")
	}
	if r.Match(makeEvent(map[string]any{"level": "debug"})) {
		t.Error("expected no match for neq when equal")
	}
}

func TestFilterRule_Contains(t *testing.T) {
	r, _ := NewFilterRule("message", OpContains, "timeout")
	if !r.Match(makeEvent(map[string]any{"message": "connection timeout occurred"})) {
		t.Error("expected contains match")
	}
	if r.Match(makeEvent(map[string]any{"message": "all good"})) {
		t.Error("expected no contains match")
	}
}

func TestFilterRule_Matches(t *testing.T) {
	r, err := NewFilterRule("code", OpMatches, `^5\d{2}$`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !r.Match(makeEvent(map[string]any{"code": "503"})) {
		t.Error("expected regex match")
	}
	if r.Match(makeEvent(map[string]any{"code": "200"})) {
		t.Error("expected no regex match")
	}
}

func TestFilterRule_InvalidRegex(t *testing.T) {
	_, err := NewFilterRule("f", OpMatches, "[invalid")
	if err == nil {
		t.Error("expected error for invalid regex")
	}
}

func TestFilterRule_MissingField(t *testing.T) {
	r, _ := NewFilterRule("missing", OpEquals, "x")
	if r.Match(makeEvent(map[string]any{"other": "x"})) {
		t.Error("expected no match when field absent")
	}
}

func TestFilter_Allow_AllRulesMustMatch(t *testing.T) {
	r1, _ := NewFilterRule("level", OpEquals, "error")
	r2, _ := NewFilterRule("service", OpEquals, "auth")
	f := NewFilter(r1, r2)

	if !f.Allow(makeEvent(map[string]any{"level": "error", "service": "auth"})) {
		t.Error("expected allow when all rules match")
	}
	if f.Allow(makeEvent(map[string]any{"level": "error", "service": "billing"})) {
		t.Error("expected deny when one rule fails")
	}
}

func TestFilter_Allow_EmptyRulesAllowsAll(t *testing.T) {
	f := NewFilter()
	if !f.Allow(makeEvent(nil)) {
		t.Error("empty filter should allow everything")
	}
}
