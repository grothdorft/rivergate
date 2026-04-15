package pipeline

import (
	"testing"
)

// TestSchema_ProcessorIntegration verifies that a Processor using a schema-based
// filter correctly drops events that fail validation.
func TestSchema_ProcessorIntegration(t *testing.T) {
	schema, err := NewSchema([]FieldSchema{
		{Name: "level", Type: FieldTypeString, Required: true},
		{Name: "code", Type: FieldTypeNumber, Required: true},
	})
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	// schemaFilter wraps Schema.Validate as a FilterFunc
	schemaFilter := FilterFunc(func(e *Event) bool {
		return schema.Validate(e) == nil
	})

	proc := NewProcessor("schema-proc", schemaFilter, nil)

	validEvent := NewEvent("src", "schema-proc")
	validEvent.Set("level", "info")
	validEvent.Set("code", float64(200))

	out := proc.Process(validEvent)
	if out == nil {
		t.Fatal("expected valid event to pass through processor")
	}

	missingField := NewEvent("src", "schema-proc")
	missingField.Set("level", "warn") // missing "code"

	out = proc.Process(missingField)
	if out != nil {
		t.Fatal("expected event missing required field to be dropped")
	}

	wrongType := NewEvent("src", "schema-proc")
	wrongType.Set("level", "error")
	wrongType.Set("code", "not-a-number")

	out = proc.Process(wrongType)
	if out != nil {
		t.Fatal("expected event with wrong type to be dropped")
	}
}
