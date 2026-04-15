package pipeline

import (
	"regexp"
	"testing"
)

func makeSchemaEvent(fields map[string]interface{}) *Event {
	e := NewEvent("test", "schema-test")
	for k, v := range fields {
		e.Set(k, v)
	}
	return e
}

func TestNewSchema_InvalidFieldName(t *testing.T) {
	_, err := NewSchema([]FieldSchema{{Name: "", Type: FieldTypeString}})
	if err == nil {
		t.Fatal("expected error for empty field name")
	}
}

func TestNewSchema_InvalidFieldType(t *testing.T) {
	_, err := NewSchema([]FieldSchema{{Name: "x", Type: "unknown"}})
	if err == nil {
		t.Fatal("expected error for unknown field type")
	}
}

func TestSchema_Validate_RequiredFieldMissing(t *testing.T) {
	s, _ := NewSchema([]FieldSchema{
		{Name: "level", Type: FieldTypeString, Required: true},
	})
	e := makeSchemaEvent(nil)
	if err := s.Validate(e); err == nil {
		t.Fatal("expected validation error for missing required field")
	}
}

func TestSchema_Validate_OptionalFieldMissing(t *testing.T) {
	s, _ := NewSchema([]FieldSchema{
		{Name: "level", Type: FieldTypeString, Required: false},
	})
	e := makeSchemaEvent(nil)
	if err := s.Validate(e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSchema_Validate_WrongType(t *testing.T) {
	s, _ := NewSchema([]FieldSchema{
		{Name: "count", Type: FieldTypeNumber, Required: true},
	})
	e := makeSchemaEvent(map[string]interface{}{"count": "not-a-number"})
	if err := s.Validate(e); err == nil {
		t.Fatal("expected type mismatch error")
	}
}

func TestSchema_Validate_CorrectTypes(t *testing.T) {
	s, _ := NewSchema([]FieldSchema{
		{Name: "msg", Type: FieldTypeString, Required: true},
		{Name: "count", Type: FieldTypeNumber, Required: true},
		{Name: "ok", Type: FieldTypeBool, Required: true},
	})
	e := makeSchemaEvent(map[string]interface{}{
		"msg":   "hello",
		"count": float64(42),
		"ok":    true,
	})
	if err := s.Validate(e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSchema_Validate_PatternMatch(t *testing.T) {
	pat := regexp.MustCompile(`^(info|warn|error)$`)
	s, _ := NewSchema([]FieldSchema{
		{Name: "level", Type: FieldTypeString, Required: true, Pattern: pat},
	})

	valid := makeSchemaEvent(map[string]interface{}{"level": "info"})
	if err := s.Validate(valid); err != nil {
		t.Fatalf("unexpected error for valid pattern: %v", err)
	}

	invalid := makeSchemaEvent(map[string]interface{}{"level": "debug"})
	if err := s.Validate(invalid); err == nil {
		t.Fatal("expected pattern mismatch error")
	}
}
