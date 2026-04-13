package pipeline

import (
	"context"
	"fmt"
)

// Dispatcher wires sources through processors to sinks using a Router,
// and records per-event metrics.
type Dispatcher struct {
	router    *Router
	processor *ProcessorChain
	sinks     *SinkRegistry
	metrics   *Metrics
}

// NewDispatcher creates a Dispatcher from the provided components.
func NewDispatcher(router *Router, processor *ProcessorChain, sinks *SinkRegistry, metrics *Metrics) *Dispatcher {
	return &Dispatcher{
		router:    router,
		processor: processor,
		sinks:     sinks,
		metrics:   metrics,
	}
}

// Dispatch reads events from the named source, routes them, applies the
// processor chain, and writes to the resolved sinks until ctx is done.
func (d *Dispatcher) Dispatch(ctx context.Context, source Source) error {
	ch := make(chan *Event, 64)
	errCh := make(chan error, 1)

	go func() {
		errCh <- source.Start(ctx, ch)
	}()

	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return <-errCh
			}
			d.metrics.Inc("events_received")
			if err := d.handle(ev, source.Name()); err != nil {
				d.metrics.Inc("events_errored")
			}
		case <-ctx.Done():
			return <-errCh
		}
	}
}

func (d *Dispatcher) handle(ev *Event, sourceName string) error {
	sinkNames := d.router.Route(sourceName)
	if len(sinkNames) == 0 {
		d.metrics.Inc("events_dropped")
		return nil
	}

	out := d.processor.Run(ev)
	if out == nil {
		d.metrics.Inc("events_filtered")
		return nil
	}

	for _, name := range sinkNames {
		sink, err := d.sinks.Get(name)
		if err != nil {
			return fmt.Errorf("dispatcher: sink %q not found: %w", name, err)
		}
		if err := sink.Write(out); err != nil {
			return fmt.Errorf("dispatcher: sink %q write error: %w", name, err)
		}
		d.metrics.Inc("events_written")
	}
	return nil
}
