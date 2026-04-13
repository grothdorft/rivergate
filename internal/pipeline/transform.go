package pipeline

import (
	"fmt"
	"strings"
)

// TransformFunc is a function that mutates a cloned Event.
type TransformFunc func(e *Event) error

// Transformer applies an ordered list of TransformFuncs to events.
type Transformer struct {
	steps []TransformFunc
}

// NewTransformer creates a Transformer with the given steps.
func NewTransformer(steps ...TransformFunc) *Transformer {
	return &Transformer{steps: steps}
}

// Apply clones the event, runs every step in order, and returns the result.
// The original event is never modified. Returns an error if any step fails.
func (t *Transformer) Apply(e *Event) (*Event, error) {
	out := e.Clone()
	for i, step := range t.steps {
		if err := step(out); err != nil {
			return nil, fmt.Errorf("transform step %d: %w", i, err)
		}
	}
	return out, nil
}

// --- built-in transform helpers ---

// SetField returns a TransformFunc that sets a fixed field value.
func SetField(key string, value any) TransformFunc {
	return func(e *Event) error {
		e.Set(key, value)
		return nil
	}
}

// DeleteField returns a TransformFunc that removes a field from the event.
func DeleteField(key string) TransformFunc {
	return func(e *Event) error {
		delete(e.Fields, key)
		return nil
	}
}

// RenameField returns a TransformFunc that renames a field, preserving its value.
func RenameField(from, to string) TransformFunc {
	return func(e *Event) error {
		if v, ok := e.Get(from); ok {
			e.Set(to, v)
			delete(e.Fields, from)
		}
		return nil
	}
}

// UppercaseField returns a TransformFunc that upper-cases a string field value.
func UppercaseField(key string) TransformFunc {
	return func(e *Event) error {
		if v, ok := e.Get(key); ok {
			if s, ok := v.(string); ok {
				e.Set(key, strings.ToUpper(s))
			}
		}
		return nil
	}
}
