package pipeline

import (
	"sync/atomic"
)

// Sequencer assigns a monotonically increasing sequence number to each event
// that passes through it, storing the value under a configurable field name.
type Sequencer struct {
	field   string
	counter atomic.Int64
	start   int64
}

// NewSequencer creates a Sequencer that writes sequence numbers into field.
// start is the first value emitted; it increments by 1 for each event.
func NewSequencer(field string, start int64) (*Sequencer, error) {
	if field == "" {
		return nil, ErrEmptyField
	}
	s := &Sequencer{field: field, start: start}
	s.counter.Store(start)
	return s, nil
}

// ErrEmptyField is returned when an empty field name is supplied.
var ErrEmptyField = sequencerError("sequencer: field name must not be empty")

type sequencerError string

func (e sequencerError) Error() string { return string(e) }

// Assign stamps e with the next sequence number and returns it.
// The method is safe for concurrent use.
func (s *Sequencer) Assign(e *Event) *Event {
	seq := s.counter.Add(1) - 1
	e.Set(s.field, seq)
	return e
}

// Next returns the sequence number that will be assigned to the next event
// without consuming it.
func (s *Sequencer) Next() int64 {
	return s.counter.Load()
}

// Reset restores the counter to the configured start value.
func (s *Sequencer) Reset() {
	s.counter.Store(s.start)
}

// SequencerTransformFunc returns a TransformFunc that stamps events with
// monotonically increasing sequence numbers via s.
func SequencerTransformFunc(s *Sequencer) TransformFunc {
	return func(e *Event) *Event {
		return s.Assign(e)
	}
}
