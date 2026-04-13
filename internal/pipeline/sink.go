package pipeline

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// Sink represents a destination for log events.
type Sink interface {
	Write(event *Event) error
	Close() error
}

// StdoutSink writes events as JSON lines to stdout.
type StdoutSink struct {
	writer io.Writer
	mu     sync.Mutex
}

// NewStdoutSink creates a Sink that writes to stdout.
func NewStdoutSink() *StdoutSink {
	return &StdoutSink{writer: os.Stdout}
}

// Write serializes the event and writes it to stdout.
func (s *StdoutSink) Write(event *Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	line := fmt.Sprintf(`{"source":%q,"message":%q,"fields":%s}`,
		event.Source, event.Message, fieldsJSON(event.Fields))
	_, err := fmt.Fprintln(s.writer, line)
	return err
}

// Close is a no-op for StdoutSink.
func (s *StdoutSink) Close() error { return nil }

// fieldsJSON encodes a map[string]string as a simple JSON object.
func fieldsJSON(fields map[string]string) string {
	if len(fields) == 0 {
		return "{}"
	}
	out := "{"
	first := true
	for k, v := range fields {
		if !first {
			out += ","
		}
		out += fmt.Sprintf("%q:%q", k, v)
		first = false
	}
	return out + "}"
}

// SinkRegistry holds named sinks.
type SinkRegistry struct {
	mu    sync.RWMutex
	sinks map[string]Sink
}

// NewSinkRegistry creates an empty SinkRegistry.
func NewSinkRegistry() *SinkRegistry {
	return &SinkRegistry{sinks: make(map[string]Sink)}
}

// Register adds a named sink to the registry.
func (r *SinkRegistry) Register(name string, sink Sink) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sinks[name] = sink
}

// Get retrieves a sink by name.
func (r *SinkRegistry) Get(name string) (Sink, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.sinks[name]
	return s, ok
}
