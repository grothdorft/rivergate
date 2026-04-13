package pipeline

import (
	"context"
	"log"
)

// Processor combines a Filter and Transformer to conditionally transform events.
type Processor struct {
	name        string
	filter      *Filter
	transformer *Transformer
}

// NewProcessor creates a Processor with the given name, filter, and transformer.
// Either filter or transformer may be nil, in which case that step is skipped.
func NewProcessor(name string, filter *Filter, transformer *Transformer) *Processor {
	return &Processor{
		name:        name,
		filter:      filter,
		transformer: transformer,
	}
}

// Name returns the processor's identifier.
func (p *Processor) Name() string {
	return p.name
}

// Process applies the filter and transformer to the event.
// Returns the (possibly modified) event and true if it should continue,
// or nil and false if the event was dropped by the filter.
func (p *Processor) Process(ctx context.Context, event *Event) (*Event, bool) {
	if ctx.Err() != nil {
		return nil, false
	}

	if p.filter != nil && !p.filter.Match(event) {
		log.Printf("[processor:%s] event dropped by filter", p.name)
		return nil, false
	}

	out := event.Clone()
	if p.transformer != nil {
		p.transformer.Apply(out)
	}

	return out, true
}

// ProcessorChain runs an event through multiple processors in order.
// Processing stops if any processor drops the event.
func ProcessorChain(ctx context.Context, event *Event, processors []*Processor) (*Event, bool) {
	current := event
	for _, proc := range processors {
		var ok bool
		current, ok = proc.Process(ctx, current)
		if !ok {
			return nil, false
		}
	}
	return current, true
}
