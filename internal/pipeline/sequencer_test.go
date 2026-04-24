package pipeline

import (
	"sync"
	"testing"
)

func makeSeqEvent(msg string) *Event {
	return NewEvent("test", map[string]any{"msg": msg})
}

func TestSequencer_StartsAtConfiguredValue(t *testing.T) {
	s, err := NewSequencer("seq", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := makeSeqEvent("hello")
	s.Assign(e)
	if got, _ := e.Get("seq"); got != int64(10) {
		t.Errorf("expected 10, got %v", got)
	}
}

func TestSequencer_Increments(t *testing.T) {
	s, _ := NewSequencer("n", 0)
	for i := int64(0); i < 5; i++ {
		e := makeSeqEvent("x")
		s.Assign(e)
		v, _ := e.Get("n")
		if v != i {
			t.Errorf("step %d: expected %d, got %v", i, i, v)
		}
	}
}

func TestSequencer_EmptyFieldReturnsError(t *testing.T) {
	_, err := NewSequencer("", 0)
	if err == nil {
		t.Fatal("expected error for empty field name")
	}
}

func TestSequencer_Reset(t *testing.T) {
	s, _ := NewSequencer("seq", 5)
	s.Assign(makeSeqEvent("a"))
	s.Assign(makeSeqEvent("b"))
	s.Reset()
	if got := s.Next(); got != 5 {
		t.Errorf("expected counter reset to 5, got %d", got)
	}
}

func TestSequencer_ConcurrentSafe(t *testing.T) {
	s, _ := NewSequencer("seq", 0)
	const workers = 50
	var wg sync.WaitGroup
	results := make([]int64, workers)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()
			e := makeSeqEvent("concurrent")
			s.Assign(e)
			v, _ := e.Get("seq")
			results[idx] = v.(int64)
		}(i)
	}
	wg.Wait()
	seen := make(map[int64]bool)
	for _, v := range results {
		if seen[v] {
			t.Errorf("duplicate sequence number %d", v)
		}
		seen[v] = true
	}
}

func TestSequencerTransformFunc(t *testing.T) {
	s, _ := NewSequencer("idx", 1)
	fn := SequencerTransformFunc(s)
	e := makeSeqEvent("via transform")
	out := fn(e)
	if out == nil {
		t.Fatal("expected non-nil event")
	}
	v, ok := out.Get("idx")
	if !ok {
		t.Fatal("field idx not set")
	}
	if v != int64(1) {
		t.Errorf("expected 1, got %v", v)
	}
}
