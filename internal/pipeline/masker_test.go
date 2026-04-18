package pipeline

import (
	"testing"
)

func makeMaskEvent(fields map[string]any) *Event {
	e := NewEvent("test")
	for k, v := range fields {
		e.Set(k, v)
	}
	return e
}

func TestRedactMask(t *testing.T) {
	m := NewMasker().AddRule("password", RedactMask())
	e := makeMaskEvent(map[string]any{"password": "secret123"})
	m.Apply(e)
	val, _ := e.Get("password")
	if val != "[REDACTED]" {
		t.Fatalf("expected [REDACTED], got %v", val)
	}
}

func TestPartialMask(t *testing.T) {
	m := NewMasker().AddRule("card", PartialMask(4))
	e := makeMaskEvent(map[string]any{"card": "1234567890123456"})
	m.Apply(e)
	val, _ := e.Get("card")
	if val != "************3456" {
		t.Fatalf("unexpected masked value: %v", val)
	}
}

func TestPartialMask_ShortValue(t *testing.T) {
	m := NewMasker().AddRule("pin", PartialMask(4))
	e := makeMaskEvent(map[string]any{"pin": "12"})
	m.Apply(e)
	val, _ := e.Get("pin")
	if val != "**" {
		t.Fatalf("expected **, got %v", val)
	}
}

func TestRegexpMask(t *testing.T) {
	m := NewMasker().AddRule("email", RegexpMask(`[^@]+@`, "***@"))
	e := makeMaskEvent(map[string]any{"email": "user@example.com"})
	m.Apply(e)
	val, _ := e.Get("email")
	if val != "***@example.com" {
		t.Fatalf("unexpected value: %v", val)
	}
}

func TestMasker_SkipsNonStringField(t *testing.T) {
	m := NewMasker().AddRule("count", RedactMask())
	e := makeMaskEvent(map[string]any{"count": 42})
	m.Apply(e)
	val, _ := e.Get("count")
	if val != 42 {
		t.Fatalf("expected 42 unchanged, got %v", val)
	}
}

func TestMasker_MultipleRules(t *testing.T) {
	m := NewMasker().
		AddRule("password", RedactMask()).
		AddRule("token", PartialMask(3))
	e := makeMaskEvent(map[string]any{"password": "hunter2", "token": "abcdef"})
	m.Apply(e)
	pw, _ := e.Get("password")
	tok, _ := e.Get("token")
	if pw != "[REDACTED]" {
		t.Fatalf("password not redacted: %v", pw)
	}
	if tok != "***def" {
		t.Fatalf("token not masked: %v", tok)
	}
}
