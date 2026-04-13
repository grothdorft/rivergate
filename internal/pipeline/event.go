package pipeline

import "time"

// Event represents a single log event flowing through the pipeline.
type Event struct {
	// Source is the name of the source that produced this event.
	Source string

	// Timestamp is when the event was received.
	Timestamp time.Time

	// Fields holds the structured data of the log event.
	Fields map[string]any

	// Raw is the original raw bytes of the event, if available.
	Raw []byte
}

// NewEvent creates a new Event with the current timestamp.
func NewEvent(source string, fields map[string]any) *Event {
	return &Event{
		Source:    source,
		Timestamp: time.Now().UTC(),
		Fields:    fields,
	}
}

// Clone returns a shallow copy of the event.
func (e *Event) Clone() *Event {
	fields := make(map[string]any, len(e.Fields))
	for k, v := range e.Fields {
		fields[k] = v
	}
	return &Event{
		Source:    e.Source,
		Timestamp: e.Timestamp,
		Fields:    fields,
		Raw:       e.Raw,
	}
}

// Set sets a field value on the event.
func (e *Event) Set(key string, value any) {
	if e.Fields == nil {
		e.Fields = make(map[string]any)
	}
	e.Fields[key] = value
}

// Get retrieves a field value from the event.
func (e *Event) Get(key string) (any, bool) {
	v, ok := e.Fields[key]
	return v, ok
}
