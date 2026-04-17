package pipeline

import (
	"bytes"
	"strings"
	"testing"
)

func makeSplitEvent(fields map[string]any) *Event {
	e := NewEvent("test", "splitter")
	for k, v := range fields {
		e.Set(k, v)
	}
	return e
}

func newFilledRegistry(t *testing.T, names ...string) (*SinkRegistry, map[string]*bytes.Buffer) {
	t.Helper()
	reg := NewSinkRegistry()
	bufs := make(map[string]*bytes.Buffer)
	for _, n := range names {
		buf := &bytes.Buffer{}
		sink := NewStdoutSink(n, buf)
		reg.Register(sink)
		bufs[n] = buf
	}
	return reg, bufs
}

func TestSplitter_FansOutToMultipleSinks(t *testing.T) {
	reg, bufs := newFilledRegistry(t, "a", "b")
	sp := NewSplitter("fan", func(_ *Event) []string { return []string{"a", "b"} }, reg)
	e := makeSplitEvent(map[string]any{"msg": "hello"})
	if err := sp.Write(e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, name := range []string{"a", "b"} {
		if !strings.Contains(bufs[name].String(), "hello") {
			t.Errorf("sink %s did not receive event", name)
		}
	}
}

func TestSplitter_SkipsUnknownSink(t *testing.T) {
	reg, bufs := newFilledRegistry(t, "a")
	sp := NewSplitter("fan", func(_ *Event) []string { return []string{"a", "missing"} }, reg)
	e := makeSplitEvent(map[string]any{"msg": "hi"})
	if err := sp.Write(e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(bufs["a"].String(), "hi") {
		t.Error("sink a should have received event")
	}
}

func TestFieldValueSplitFunc_UsesFieldValue(t *testing.T) {
	fn := FieldValueSplitFunc("dest", "default")
	e := makeSplitEvent(map[string]any{"dest": "sink-x"})
	if got := fn(e); len(got) != 1 || got[0] != "sink-x" {
		t.Errorf("expected [sink-x], got %v", got)
	}
}

func TestFieldValueSplitFunc_FallsBackWhenMissing(t *testing.T) {
	fn := FieldValueSplitFunc("dest", "default")
	e := makeSplitEvent(nil)
	if got := fn(e); len(got) != 1 || got[0] != "default" {
		t.Errorf("expected [default], got %v", got)
	}
}

func TestFieldValueSplitFunc_ReturnsNilWhenNoFallback(t *testing.T) {
	fn := FieldValueSplitFunc("dest", "")
	e := makeSplitEvent(nil)
	if got := fn(e); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestSplitter_Name(t *testing.T) {
	reg := NewSinkRegistry()
	sp := NewSplitter("my-splitter", func(_ *Event) []string { return nil }, reg)
	if sp.Name() != "my-splitter" {
		t.Errorf("unexpected name: %s", sp.Name())
	}
}
