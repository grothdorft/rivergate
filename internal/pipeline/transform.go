package pipeline

import "fmt"

// TransformOp is a function that mutates an Event in place.
type TransformOp func(*Event)

// Transformer holds an ordered list of transform operations.
type Transformer struct {
	ops []TransformOp
}

// NewTransformer builds a Transformer from a slice of config maps.
// Each map must contain at least "type" and "field" keys.
func NewTransformer(cfg []map[string]string) (*Transformer, error) {
	ops := make([]TransformOp, 0, len(cfg))
	for _, entry := range cfg {
		kind := entry["type"]
		field := entry["field"]
		switch kind {
		case "set":
			value := entry["value"]
			ops = append(ops, SetField(field, value))
		case "delete":
			ops = append(ops, DeleteField(field))
		case "rename":
			to := entry["to"]
			ops = append(ops, RenameField(field, to))
		default:
			return nil, fmt.Errorf("transform: unknown type %q", kind)
		}
	}
	return &Transformer{ops: ops}, nil
}

// Apply runs all transform operations against the given event.
func (t *Transformer) Apply(e *Event) {
	for _, op := range t.ops {
		op(e)
	}
}

// SetField returns a TransformOp that sets key to value on the event.
func SetField(key, value string) TransformOp {
	return func(e *Event) {
		e.Set(key, value)
	}
}

// DeleteField returns a TransformOp that removes key from the event.
func DeleteField(key string) TransformOp {
	return func(e *Event) {
		if e.Fields != nil {
			delete(e.Fields, key)
		}
	}
}

// RenameField returns a TransformOp that moves the value at oldKey to newKey.
func RenameField(oldKey, newKey string) TransformOp {
	return func(e *Event) {
		if e.Fields == nil {
			return
		}
		if v, ok := e.Fields[oldKey]; ok {
			e.Fields[newKey] = v
			delete(e.Fields, oldKey)
		}
	}
}
