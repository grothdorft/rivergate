package pipeline

// SplitFunc decides which sink names an event should be routed to.
type SplitFunc func(e *Event) []string

// Splitter fans an event out to multiple named sinks.
type Splitter struct {
	name     string
	splitFn  SplitFunc
	registry *SinkRegistry
}

// NewSplitter creates a Splitter that uses splitFn to determine target sinks.
func NewSplitter(name string, fn SplitFunc, registry *SinkRegistry) *Splitter {
	return &Splitter{name: name, splitFn: fn, registry: registry}
}

// Name returns the splitter's identifier.
func (s *Splitter) Name() string { return s.name }

// Write fans the event out to every sink returned by the split function.
// Errors from individual sinks are collected and the first is returned.
func (s *Splitter) Write(e *Event) error {
	targets := s.splitFn(e)
	var firstErr error
	for _, t := range targets {
		sk, ok := s.registry.Get(t)
		if !ok {
			continue
		}
		if err := sk.Write(e.Clone()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// FieldValueSplitFunc returns a SplitFunc that routes events to sinks whose
// names match the value of the given field. Falls back to fallback if missing.
func FieldValueSplitFunc(field, fallback string) SplitFunc {
	return func(e *Event) []string {
		if v, ok := e.Get(field); ok {
			if s, ok := v.(string); ok && s != "" {
				return []string{s}
			}
		}
		if fallback != "" {
			return []string{fallback}
		}
		return nil
	}
}
