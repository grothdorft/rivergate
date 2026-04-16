package pipeline

import "strings"

// EnrichFunc is a function that adds or modifies fields on an event.
type EnrichFunc func(e *Event)

// Enricher applies a list of EnrichFuncs to events.
type Enricher struct {
	fns []EnrichFunc
}

// NewEnricher creates an Enricher from the provided functions.
func NewEnricher(fns ...EnrichFunc) *Enricher {
	return &Enricher{fns: fns}
}

// Enrich applies all enrich functions to the event.
func (en *Enricher) Enrich(e *Event) {
	for _, fn := range en.fns {
		fn(e)
	}
}

// StaticField returns an EnrichFunc that sets a fixed key/value.
func StaticField(key, value string) EnrichFunc {
	return func(e *Event) {
		e.Set(key, value)
	}
}

// UpperCaseField returns an EnrichFunc that uppercases the string value of a field.
func UpperCaseField(key string) EnrichFunc {
	return func(e *Event) {
		if v, ok := e.Get(key); ok {
			if s, ok := v.(string); ok {
				e.Set(key, strings.ToUpper(s))
			}
		}
	}
}

// CopyField returns an EnrichFunc that copies src field value to dst.
func CopyField(src, dst string) EnrichFunc {
	return func(e *Event) {
		if v, ok := e.Get(src); ok {
			e.Set(dst, v)
		}
	}
}
