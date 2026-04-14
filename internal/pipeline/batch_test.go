package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

func makeBatchEvent(msg string) *Event {
	e := NewEvent("test")
	e.Set("message", msg)
	return e
}

func TestBatch_FlushOnSizeLimit(t *testing.T) {
	var mu sync.Mutex
	var got [][]*Event
	flush := func(evs []*Event) {
		mu.Lock()
		got = append(got, evs)
		mu.Unlock()
	}
	b := NewBatch(3, 10*time.Second, flush)
	b.Add(makeBatchEvent("a"))
	b.Add(makeBatchEvent("b"))
	b.Add(makeBatchEvent("c")) // triggers flush
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("expected 1 flush, got %d", len(got))
	}
	if len(got[0]) != 3 {
		t.Fatalf("expected 3 events in flush, got %d", len(got[0]))
	}
}

func TestBatch_ManualFlush(t *testing.T) {
	var flushed []*Event
	b := NewBatch(100, 10*time.Second, func(evs []*Event) {
		flushed = evs
	})
	b.Add(makeBatchEvent("x"))
	b.Add(makeBatchEvent("y"))
	b.Flush()
	if len(flushed) != 2 {
		t.Fatalf("expected 2 events, got %d", len(flushed))
	}
}

func TestBatch_FlushEmptyIsNoop(t *testing.T) {
	called := false
	b := NewBatch(10, time.Second, func(evs []*Event) { called = true })
	b.Flush()
	if called {
		t.Fatal("flush of empty batch should not invoke flushFn")
	}
}

func TestBatch_RunFlushesOnInterval(t *testing.T) {
	var mu sync.Mutex
	count := 0
	b := NewBatch(100, 30*time.Millisecond, func(evs []*Event) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	b.Add(makeBatchEvent("z"))
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	b.Run(ctx) // blocks until ctx expires
	mu.Lock()
	defer mu.Unlock()
	if count == 0 {
		t.Fatal("expected at least one interval flush")
	}
}

func TestBatch_DefaultsApplied(t *testing.T) {
	b := NewBatch(0, 0, func(_ []*Event) {})
	if b.size != 100 {
		t.Errorf("expected default size 100, got %d", b.size)
	}
	if b.interval != 5*time.Second {
		t.Errorf("expected default interval 5s, got %v", b.interval)
	}
}
