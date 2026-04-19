package pipeline

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestWindow_FlushesToSink(t *testing.T) {
	var buf bytes.Buffer
	sink := NewStdoutSink("window-sink", &buf)

	win := NewWindow(time.Hour, func(events []*Event) {
		for _, e := range events {
			_ = sink.Write(e)
		}
	})

	e := NewEvent("src")
	e.Set("level", "info")
	win.Add(e)
	win.Flush()

	out := buf.String()
	if !strings.Contains(out, "window-sink") {
		t.Fatalf("expected sink name in output, got: %s", out)
	}
	if !strings.Contains(out, "info") {
		t.Fatalf("expected field value in output, got: %s", out)
	}
}
