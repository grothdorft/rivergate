package pipeline

import (
	"testing"
)

func TestNewSampler_InvalidRate(t *testing.T) {
	_, err := NewSampler(0)
	if err == nil {
		t.Fatal("expected error for rate=0")
	}
	_, err = NewSampler(1.5)
	if err == nil {
		t.Fatal("expected error for rate=1.5")
	}
}

func TestNewSateOne_PassesAll(t *testing.T) {
	s, err := NewSampler(1.0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < 100; i++ {
		if !s.Sample() {
			t.Fatalf("expected all events to pass at rate=1.0, failed at i=%d", i)
		}
	}
}

func TestNewSampler_RateNearZero_DropsMany(t *testing.T) {
	s, err := NewSampler(0.01)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	passed := 0
	const trials = 10000
	for i := 0; i < trials; i++ {
		if s.Sample() {
			passed++
		}
	}
	// Expect roughly 1% ± generous margin
	if passed > 300 {
		t.Errorf("expected ~1%% pass rate, got %d/%d", passed, trials)
	}
}

func TestNewEveryNSampler_InvalidN(t *testing.T) {
	_, err := NewEveryNSampler(0)
	if err == nil {
		t.Fatal("expected error for n=0")
	}
}

func TestEveryNSampler_PassesEveryN(t *testing.T) {
	s, err := NewEveryNSampler(3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	results := make([]bool, 9)
	for i := range results {
		results[i] = s.Sample()
	}
	// Expect pass at indices 2, 5, 8 (every 3rd call)
	expected := []bool{false, false, true, false, false, true, false, false, true}
	for i, want := range expected {
		if results[i] != want {
			t.Errorf("index %d: got %v, want %v", i, results[i], want)
		}
	}
}

func TestEveryNSampler_N1_PassesAll(t *testing.T) {
	s, err := NewEveryNSampler(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < 10; i++ {
		if !s.Sample() {
			t.Fatalf("expected all events to pass at n=1, failed at i=%d", i)
		}
	}
}
