package pipeline

import (
	"bytes"
	"strings"
	"testing"
)

func TestStdoutSink_Write(t *testing.T) {
	var buf bytes.Buffer
	sink := &StdoutSink{writer: &buf}

	e := NewEvent("app", "hello world")
	e.Set("env", "prod")

	if err := sink.Write(e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	line := buf.String()
	if !strings.Contains(line, `"source":"app"`) {
		t.Errorf("expected source in output, got: %s", line)
	}
	if !strings.Contains(line, `"message":"hello world"`) {
		t.Errorf("expected message in output, got: %s", line)
	}
	if !strings.Contains(line, `"env":"prod"`) {
		t.Errorf("expected env field in output, got: %s", line)
	}
}

func TestStdoutSink_WriteEmptyFields(t *testing.T) {
	var buf bytes.Buffer
	sink := &StdoutSink{writer: &buf}

	e := NewEvent("svc", "msg")
	if err := sink.Write(e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(buf.String(), `"fields":{}`) {
		t.Errorf("expected empty fields object, got: %s", buf.String())
	}
}

func TestStdoutSink_Close(t *testing.T) {
	sink := NewStdoutSink()
	if err := sink.Close(); err != nil {
		t.Errorf("expected nil from Close, got: %v", err)
	}
}

func TestSinkRegistry_RegisterAndGet(t *testing.T) {
	reg := NewSinkRegistry()
	sink := NewStdoutSink()

	reg.Register("stdout", sink)

	got, ok := reg.Get("stdout")
	if !ok {
		t.Fatal("expected to find registered sink")
	}
	if got != sink {
		t.Error("retrieved sink does not match registered sink")
	}
}

func TestSinkRegistry_GetMissing(t *testing.T) {
	reg := NewSinkRegistry()
	_, ok := reg.Get("missing")
	if ok {
		t.Error("expected false for missing sink")
	}
}
