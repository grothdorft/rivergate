package pipeline

import (
	"fmt"
	"regexp"
	"strings"
)

// FilterOp represents a comparison operation used in a filter rule.
type FilterOp string

const (
	OpEquals     FilterOp = "eq"
	OpNotEquals  FilterOp = "neq"
	OpContains   FilterOp = "contains"
	OpMatches    FilterOp = "matches"
)

// FilterRule defines a single field-based predicate on an Event.
type FilterRule struct {
	Field  string
	Op     FilterOp
	Value  string
	re     *regexp.Regexp
}

// NewFilterRule constructs and validates a FilterRule.
func NewFilterRule(field string, op FilterOp, value string) (*FilterRule, error) {
	if field == "" {
		return nil, fmt.Errorf("filter rule: field must not be empty")
	}
	r := &FilterRule{Field: field, Op: op, Value: value}
	if op == OpMatches {
		re, err := regexp.Compile(value)
		if err != nil {
			return nil, fmt.Errorf("filter rule: invalid regex %q: %w", value, err)
		}
		r.re = re
	}
	return r, nil
}

// Match reports whether the event satisfies the rule.
func (r *FilterRule) Match(e *Event) bool {
	v, ok := e.Get(r.Field)
	if !ok {
		return false
	}
	s := fmt.Sprintf("%v", v)
	switch r.Op {
	case OpEquals:
		return s == r.Value
	case OpNotEquals:
		return s != r.Value
	case OpContains:
		return strings.Contains(s, r.Value)
	case OpMatches:
		return r.re != nil && r.re.MatchString(s)
	}
	return false
}

// Filter holds a set of rules and passes events that satisfy ALL rules.
type Filter struct {
	rules []*FilterRule
}

// NewFilter creates a Filter from the provided rules.
func NewFilter(rules ...*FilterRule) *Filter {
	return &Filter{rules: rules}
}

// Allow returns true when the event matches every rule in the filter.
func (f *Filter) Allow(e *Event) bool {
	for _, r := range f.rules {
		if !r.Match(e) {
			return false
		}
	}
	return true
}
