package pipeline

import (
	"context"
	"testing"
)

func makeProcessorEvent(msg string) *Event {
	e := NewEvent("test")
	e.Set("message", msg)
	e.Set("level", "info")
	return e
}

func TestProcessor_Name(t *testing.T) {
	p := NewProcessor("my-proc", nil, nil)
	if p.Name() != "my-proc" {
		t.Errorf("expected 'my-proc', got %q", p.Name())
	}
}

func TestProcessor_NilFilterAndTransformer_PassesThrough(t *testing.T) {
	p := NewProcessor("passthrough", nil, nil)
	e := makeProcessorEvent("hello")
	out, ok := p.Process(context.Background(), e)
	if !ok {
		t.Fatal("expected event to pass through")
	}
	if v, _ := out.Get("message"); v != "hello" {
		t.Errorf("unexpected message: %v", v)
	}
}

func TestProcessor_FilterDropsEvent(t *testing.T) {
	rule, _ := NewFilterRule("level", "equals", "error")
	f := NewFilter([]*FilterRule{rule})
	p := NewProcessor("errors-only", f, nil)

	e := makeProcessorEvent("hello") // level=info, should be dropped
	_, ok := p.Process(context.Background(), e)
	if ok {
		t.Fatal("expected event to be dropped")
	}
}

func TestProcessor_FilterPassesAndTransforms(t *testing.T) {
	rule, _ := NewFilterRule("level", "equals", "info")
	f := NewFilter([]*FilterRule{rule})
	tr := NewTransformer([]TransformFunc{SetField("env", "prod")})
	p := NewProcessor("enrich-info", f, tr)

	e := makeProcessorEvent("hello")
	out, ok := p.Process(context.Background(), e)
	if !ok {
		t.Fatal("expected event to pass")
	}
	if v, _ := out.Get("env"); v != "prod" {
		t.Errorf("expected env=prod, got %v", v)
	}
}

func TestProcessor_CancelledContext_DropsEvent(t *testing.T) {
	p := NewProcessor("noop", nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, ok := p.Process(ctx, makeProcessorEvent("hi"))
	if ok {
		t.Fatal("expected cancelled context to drop event")
	}
}

func TestProcessorChain_AllPass(t *testing.T) {
	tr1 := NewTransformer([]TransformFunc{SetField("a", "1")})
	tr2 := NewTransformer([]TransformFunc{SetField("b", "2")})
	chain := []*Processor{
		NewProcessor("p1", nil, tr1),
		NewProcessor("p2", nil, tr2),
	}
	e := makeProcessorEvent("chained")
	out, ok := ProcessorChain(context.Background(), e, chain)
	if !ok {
		t.Fatal("expected chain to pass")
	}
	if v, _ := out.Get("a"); v != "1" {
		t.Errorf("expected a=1, got %v", v)
	}
	if v, _ := out.Get("b"); v != "2" {
		t.Errorf("expected b=2, got %v", v)
	}
}

func TestProcessorChain_MiddleDrops(t *testing.T) {
	rule, _ := NewFilterRule("level", "equals", "error")
	f := NewFilter([]*FilterRule{rule})
	chain := []*Processor{
		NewProcessor("p1", nil, nil),
		NewProcessor("p2", f, nil), // drops info events
		NewProcessor("p3", nil, nil),
	}
	e := makeProcessorEvent("hello") // level=info
	_, ok := ProcessorChain(context.Background(), e, chain)
	if ok {
		t.Fatal("expected chain to drop event at p2")
	}
}
