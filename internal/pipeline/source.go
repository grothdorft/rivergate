package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Source reads events and emits them to a channel.
type Source interface {
	Start(ctx context.Context, out chan<- *Event) error
	Name() string
}

// StdinSource reads JSON log events from stdin line by line.
type StdinSource struct {
	name   string
	reader io.Reader
}

// NewStdinSource creates a StdinSource with the given name.
func NewStdinSource(name string) *StdinSource {
	return &StdinSource{name: name, reader: os.Stdin}
}

func (s *StdinSource) Name() string { return s.name }

// Start begins reading lines from stdin, parsing each as a JSON event.
// It stops when the context is cancelled or EOF is reached.
func (s *StdinSource) Start(ctx context.Context, out chan<- *Event) error {
	scanner := bufio.NewScanner(s.reader)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if !scanner.Scan() {
			break
		}
		line := scanner.Bytes()
		fields := make(map[string]any)
		if err := json.Unmarshal(line, &fields); err != nil {
			continue // skip malformed lines
		}
		event := NewEvent(s.name, fields)
		select {
		case out <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("stdin source %q scan error: %w", s.name, err)
	}
	return nil
}

// SourceRegistry holds named Source instances.
type SourceRegistry struct {
	sources map[string]Source
}

func NewSourceRegistry() *SourceRegistry {
	return &SourceRegistry{sources: make(map[string]Source)}
}

func (r *SourceRegistry) Register(s Source) {
	r.sources[s.Name()] = s
}

func (r *SourceRegistry) Get(name string) (Source, bool) {
	s, ok := r.sources[name]
	return s, ok
}
