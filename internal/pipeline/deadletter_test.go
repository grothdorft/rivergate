package pipeline

import (
	"context"
	"testing"
)

func TestDeadLetterQueue_Push(t *testing.T) {
	dlq := NewDeadLetterQueue(10)
	dlq.Push(context.Background(), NewEvent("src", "failed event"))
	if dlq.Len() != 1 {
		t.Fatalf("expected 1 event, got %d", dlq.Len())
	}
}

func TestDeadLetterQueue_Drain(t *testing.T) {
	dlq := NewDeadLetterQueue(10)
	e1 := NewEvent("src", "e1")
	e2 := NewEvent("src", "e2")
	dlq.Push(context.Background(), e1)
	dlq.Push(context.Background(), e2)
	events := dlq.Drain()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if dlq.Len() != 0 {
		t.Fatal("expected queue to be empty after drain")
	}
}

func TestDeadLetterQueue_CapacityDrops(t *testing.T) {
	dlq := NewDeadLetterQueue(2)
	for i := 0; i < 5; i++ {
		dlq.Push(context.Background(), NewEvent("src", "msg"))
	}
	if dlq.Len() != 2 {
		t.Fatalf("expected 2 events (capacity), got %d", dlq.Len())
	}
	if dlq.Dropped() != 3 {
		t.Fatalf("expected 3 dropped, got %d", dlq.Dropped())
	}
}

func TestDeadLetterQueue_UnlimitedCapacity(t *testing.T) {
	dlq := NewDeadLetterQueue(0)
	for i := 0; i < 50; i++ {
		dlq.Push(context.Background(), NewEvent("src", "msg"))
	}
	if dlq.Len() != 50 {
		t.Fatalf("expected 50 events, got %d", dlq.Len())
	}
	if dlq.Dropped() != 0 {
		t.Fatalf("expected 0 dropped, got %d", dlq.Dropped())
	}
}

func TestDeadLetterQueue_DrainClonesEvents(t *testing.T) {
	dlq := NewDeadLetterQueue(10)
	original := NewEvent("src", "original")
	dlq.Push(context.Background(), original)
	events := dlq.Drain()
	events[0].Set("mutated", true)
	// push original again and drain to confirm it was cloned
	dlq.Push(context.Background(), original)
	second := dlq.Drain()
	if _, ok := second[0].Get("mutated"); ok {
		t.Fatal("expected stored event to be a clone, not the same reference")
	}
}
