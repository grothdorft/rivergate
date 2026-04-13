package pipeline

import (
	"testing"
)

func makeBufferEvent(msg string) *Event {
	e := NewEvent("test")
	e.Set("message", msg)
	return e
}

func TestBuffer_PushAndPop(t *testing.T) {
	b := NewBuffer(4)
	b.Push(makeBufferEvent("a"))
	b.Push(makeBufferEvent("b"))

	if b.Len() != 2 {
		t.Fatalf("expected len 2, got %d", b.Len())
	}

	e := b.Pop()
	if e == nil {
		t.Fatal("expected event, got nil")
	}
	v, _ := e.Get("message")
	if v != "a" {
		t.Errorf("expected 'a', got %v", v)
	}
}

func TestBuffer_PopEmptyReturnsNil(t *testing.T) {
	b := NewBuffer(4)
	if b.Pop() != nil {
		t.Error("expected nil from empty buffer")
	}
}

func TestBuffer_CapacityDrops(t *testing.T) {
	b := NewBuffer(2)
	b.Push(makeBufferEvent("1"))
	b.Push(makeBufferEvent("2"))
	accepted := b.Push(makeBufferEvent("3"))

	if accepted {
		t.Error("expected Push to return false when buffer is full")
	}
	if b.Dropped() != 1 {
		t.Errorf("expected 1 dropped, got %d", b.Dropped())
	}
	if b.Len() != 2 {
		t.Errorf("expected len 2, got %d", b.Len())
	}
}

func TestBuffer_DefaultCapacity(t *testing.T) {
	b := NewBuffer(0)
	if b.capacity != 1024 {
		t.Errorf("expected default capacity 1024, got %d", b.capacity)
	}
}

func TestBuffer_Flush(t *testing.T) {
	b := NewBuffer(8)
	b.Push(makeBufferEvent("x"))
	b.Push(makeBufferEvent("y"))
	b.Push(makeBufferEvent("z"))

	out := b.Flush()
	if len(out) != 3 {
		t.Fatalf("expected 3 events, got %d", len(out))
	}
	if b.Len() != 0 {
		t.Error("expected buffer to be empty after flush")
	}
}

func TestBuffer_FIFOOrder(t *testing.T) {
	b := NewBuffer(8)
	msgs := []string{"first", "second", "third"}
	for _, m := range msgs {
		b.Push(makeBufferEvent(m))
	}
	for _, want := range msgs {
		e := b.Pop()
		got, _ := e.Get("message")
		if got != want {
			t.Errorf("expected %q, got %v", want, got)
		}
	}
}
