package pipeline

import (
	"math/rand"
	"sync"
)

// SamplingStrategy defines how events are sampled.
type SamplingStrategy int

const (
	// SampleRandom samples events with a fixed probability.
	SampleRandom SamplingStrategy = iota
	// SampleEveryN passes every Nth event through.
	SampleEveryN
)

// Sampler drops events based on a sampling strategy, reducing
// throughput to a configured fraction of the input stream.
type Sampler struct {
	mu       sync.Mutex
	strategy SamplingStrategy
	rate     float64 // 0.0–1.0 for Random; ignored for EveryN
	n        int     // interval for EveryN
	counter  int
	rng      *rand.Rand
}

// NewSampler creates a Sampler using SampleRandom with the given rate.
// rate must be in the range (0, 1]. A rate of 1.0 passes all events.
func NewSampler(rate float64) (*Sampler, error) {
	if rate <= 0 || rate > 1.0 {
		return nil, fmt.Errorf("sampling rate must be in (0, 1], got %f", rate)
	}
	return &Sampler{
		strategy: SampleRandom,
		rate:     rate,
		rng:      rand.New(rand.NewSource(rand.Int63())),
	}, nil
}

// NewEveryNSampler creates a Sampler that passes every Nth event.
// n must be >= 1.
func NewEveryNSampler(n int) (*Sampler, error) {
	if n < 1 {
		return nil, fmt.Errorf("everyN must be >= 1, got %d", n)
	}
	return &Sampler{
		strategy: SampleEveryN,
		n:        n,
	}, nil
}

// Sample returns true if the event should be forwarded, false if it
// should be dropped.
func (s *Sampler) Sample() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.strategy {
	case SampleEveryN:
		s.counter++
		if s.counter >= s.n {
			s.counter = 0
			return true
		}
		return false
	default: // SampleRandom
		return s.rng.Float64() < s.rate
	}
}

// fmt import is needed for error formatting.
import "fmt"
