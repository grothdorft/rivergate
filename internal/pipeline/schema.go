package pipeline

import (
	"errors"
	"fmt"
	"regexp"
)

// FieldType represents the expected type of a field value.
type FieldType string

const (
	FieldTypeString FieldType = "string"
	FieldTypeNumber FieldType = "number"
	FieldTypeBool   FieldType = "bool"
)

// FieldSchema defines validation rules for a single event field.
type FieldSchema struct {
	Name     string
	Type     FieldType
	Required bool
	Pattern  *regexp.Regexp // optional, only for string fields
}

// Schema validates events against a set of field rules.
type Schema struct {
	fields map[string]FieldSchema
}

// NewSchema constructs a Schema from a slice of FieldSchema definitions.
func NewSchema(fields []FieldSchema) (*Schema, error) {
	m := make(map[string]FieldSchema, len(fields))
	for _, f := range fields {
		if f.Name == "" {
			return nil, errors.New("schema field name must not be empty")
		}
		if f.Type != FieldTypeString && f.Type != FieldTypeNumber && f.Type != FieldTypeBool {
			return nil, fmt.Errorf("schema field %q has unknown type %q", f.Name, f.Type)
		}
		m[f.Name] = f
	}
	return &Schema{fields: m}, nil
}

// Validate checks an event against the schema.
// Returns a non-nil error describing the first violation found.
func (s *Schema) Validate(e *Event) error {
	for name, fs := range s.fields {
		val, ok := e.Get(name)
		if !ok {
			if fs.Required {
				return fmt.Errorf("required field %q is missing", name)
			}
			continue
		}
		if err := checkType(name, fs.Type, val); err != nil {
			return err
		}
		if fs.Pattern != nil {
			str, _ := val.(string)
			if !fs.Pattern.MatchString(str) {
				return fmt.Errorf("field %q value %q does not match pattern %s", name, str, fs.Pattern)
			}
		}
	}
	return nil
}

func checkType(name string, ft FieldType, val interface{}) error {
	switch ft {
	case FieldTypeString:
		if _, ok := val.(string); !ok {
			return fmt.Errorf("field %q expected string, got %T", name, val)
		}
	case FieldTypeNumber:
		switch val.(type) {
		case int, int64, float32, float64:
		default:
			return fmt.Errorf("field %q expected number, got %T", name, val)
		}
	case FieldTypeBool:
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("field %q expected bool, got %T", name, val)
		}
	}
	return nil
}
