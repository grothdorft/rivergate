package pipeline

import (
	"testing"
	"time"
)

func makeReorderEvent(seq int, msg string) *Event {
	e := NewEvent()
	e.Set("seq", seq)
	e.Set("msg", msg)
	return e
}

func TestReorderBuffer_SortsEvents(t *testing.T) {
	rb := NewReorderBuffer("seq", 50*time.Millisecond)
	go rb.Run()
	defer rb.Stop()

	rb.Push(makeReorderEvent(3, "third"))
	rb.Push(makeReorderEvent(1, "first"))
	rb.Push(makeReorderEvent(2, "second"))

	select {
	case batch := <-rb.Output():
		if len(batch) != 3 {
			t.Fatalf("expected 3 events, got %d", len(batch))
		}
		for i, want := range []int{1, 2, 3} {
			v, _ := batch[i].Get("seq")
			if v.(int) != want {
				t.Errorf("position %d: got seq %v, want %d", i, v, want)
			}
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for reorder batch")
	}
}

func TestReorderBuffer_EmptyFlushIsNoop(t *testing.T) {
	rb := NewReorderBuffer("seq", 30*time.Millisecond)
	go rb.Run()

	select {
	case batch := <-rb.Output():
		t.Fatalf("expected no output for empty buffer, got %d events", len(batch))
	case <-time.After(120 * time.Millisecond):
		// correct — nothing emitted
	}
	rb.Stop()
}

func TestReorderBuffer_StopFlushesRemaining(t *testing.T) {
	rb := NewReorderBuffer("seq", 10*time.Second) // long window
	go rb.Run()

	rb.Push(makeReorderEvent(2, "b"))
	rb.Push(makeReorderEvent(1, "a"))
	rb.Stop()

	select {
	case batch := <-rb.Output():
		if len(batch) != 2 {
			t.Fatalf("expected 2 events on stop-flush, got %d", len(batch))
		}
		v, _ := batch[0].Get("seq")
		if v.(int) != 1 {
			t.Errorf("first event after sort should have seq=1, got %v", v)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for stop-flush")
	}
}

func TestReorderBuffer_DefaultFieldAndWindow(t *testing.T) {
	// empty field and zero window should use defaults without panic
	rb := NewReorderBuffer("", 0)
	if rb.field != "seq" {
		t.Errorf("expected default field 'seq', got %q", rb.field)
	}
	if rb.window != time.Second {
		t.Errorf("expected default window 1s, got %v", rb.window)
	}
}

func TestReorderBuffer_PushAfterStopIsNoop(t *testing.T) {
	rb := NewReorderBuffer("seq", 50*time.Millisecond)
	go rb.Run()
	rb.Stop()
	// drain any stop-flush output
	select {
	case <-rb.Output():
	case <-time.After(200 * time.Millisecond):
	}
	// push after stop must not panic or block
	rb.Push(makeReorderEvent(1, "late"))
}
