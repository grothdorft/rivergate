package pipeline

import (
	"regexp"
	"strings"
)

// MaskFunc replaces or redacts a field value.
type MaskFunc func(value string) string

// Masker redacts sensitive fields on events.
type Masker struct {
	rules []maskRule
}

type maskRule struct {
	field string
	fn    MaskFunc
}

// NewMasker creates a Masker with no rules.
func NewMasker() *Masker {
	return &Masker{}
}

// AddRule registers a MaskFunc for the given field.
func (m *Masker) AddRule(field string, fn MaskFunc) *Masker {
	m.rules = append(m.rules, maskRule{field: field, fn: fn})
	return m
}

// Apply runs all masking rules against the event in place.
func (m *Masker) Apply(e *Event) {
	for _, r := range m.rules {
		if val, ok := e.Get(r.field); ok {
			if s, ok := val.(string); ok {
				e.Set(r.field, r.fn(s))
			}
		}
	}
}

// RedactMask replaces the entire value with "[REDACTED]".
func RedactMask() MaskFunc {
	return func(_ string) string {
		return "[REDACTED]"
	}
}

// PartialMask reveals only the last n characters, masking the rest with '*'.
func PartialMask(show int) MaskFunc {
	return func(value string) string {
		if show <= 0 || len(value) <= show {
			return strings.Repeat("*", len(value))
		}
		return strings.Repeat("*", len(value)-show) + value[len(value)-show:]
	}
}

// RegexpMask replaces all matches of pattern with replacement.
func RegexpMask(pattern, replacement string) MaskFunc {
	re := regexp.MustCompile(pattern)
	return func(value string) string {
		return re.ReplaceAllString(value, replacement)
	}
}
