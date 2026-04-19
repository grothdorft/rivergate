package pipeline

import (
	"sync"
	"testing"
	"time"
)

func makeWindowEvent(msg string) *Event {
	e := NewEvent("test")
	e.Set("msg", msg)
	return e
}

func TestWindow_Add_And_Flush(t *testing.T) {
	var mu sync.Mutex
	var got []*Event
	w := NewWindow(time.Hour, func(events []*Event) {
		mu.Lock()
		got = append(got, events...)
		mu.Unlock()
	})
	w.Add(makeWindowEvent("a"))
	w.Add(makeWindowEvent("b"))
	w.Flush()
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
}

func TestWindow_FlushEmptyIsNoop(t *testing.T) {
	called := false
	w := NewWindow(time.Hour, func(events []*Event) { called = true })
	w.Flush()
	if called {
		t.Fatal("flush fn should not be called on empty window")
	}
}

func TestWindow_Run_FlushesOnInterval(t *testing.T) {
	var mu sync.Mutex
	var got []*Event
	w := NewWindow(30*time.Millisecond, func(events []*Event) {
		mu.Lock()
		got = append(got, events...)
		mu.Unlock()
	})
	w.Add(makeWindowEvent("x"))
	go w.Run()
	time.Sleep(80 * time.Millisecond)
	w.Stop()
	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("expected at least one flushed event")
	}
}

func TestWindow_Stop_FlushesRemaining(t *testing.T) {
	var mu sync.Mutex
	var got []*Event
	w := NewWindow(time.Hour, func(events []*Event) {
		mu.Lock()
		got = append(got, events...)
		mu.Unlock()
	})
	w.Add(makeWindowEvent("final"))
	go w.Run()
	w.Stop()
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("expected 1 event on stop, got %d", len(got))
	}
}

func TestWindow_ClonesEvents(t *testing.T) {
	var got []*Event
	w := NewWindow(time.Hour, func(events []*Event) { got = events })
	e := makeWindowEvent("orig")
	w.Add(e)
	e.Set("msg", "mutated")
	w.Flush()
	if v, _ := got[0].Get("msg"); v != "orig" {
		t.Fatalf("expected cloned value 'orig', got %v", v)
	}
}
