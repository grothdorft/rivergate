package pipeline

import (
	"bytes"
	"strings"
	"testing"
)

// TestSplitter_ProcessorIntegration verifies that a Processor can write through
// a Splitter to fan events out to multiple sinks.
func TestSplitter_ProcessorIntegration(t *testing.T) {
	reg := NewSinkRegistry()
	bufA, bufB := &bytes.Buffer{}, &bytes.Buffer{}
	reg.Register(NewStdoutSink("alpha", bufA))
	reg.Register(NewStdoutSink("beta", bufB))

	sp := NewSplitter("fan", func(_ *Event) []string {
		return []string{"alpha", "beta"}
	}, reg)

	// Processor with no filter/transformer just passes events through.
	proc := NewProcessor("fan-proc", nil, nil, sp)

	e := NewEvent("src", "fan-proc")
	e.Set("level", "info")
	e.Set("msg", "integration")

	if err := proc.Write(e); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	for name, buf := range map[string]*bytes.Buffer{"alpha": bufA, "beta": bufB} {
		if !strings.Contains(buf.String(), "integration") {
			t.Errorf("sink %s missing event output", name)
		}
	}
}
