package pipeline

import (
	"fmt"
)

// Dispatcher routes events from the router to the appropriate sinks.
type Dispatcher struct {
	router *Router
	sinks  *SinkRegistry
}

// NewDispatcher creates a Dispatcher wiring a Router to a SinkRegistry.
func NewDispatcher(router *Router, sinks *SinkRegistry) *Dispatcher {
	return &Dispatcher{router: router, sinks: sinks}
}

// Dispatch routes the event and writes it to all resolved sinks.
// Returns the number of sinks written to, or an error if any write fails.
func (d *Dispatcher) Dispatch(event *Event) (int, error) {
	sinkNames := d.router.Route(event)
	if len(sinkNames) == 0 {
		return 0, nil
	}

	written := 0
	for _, name := range sinkNames {
		sink, ok := d.sinks.Get(name)
		if !ok {
			return written, fmt.Errorf("dispatcher: sink %q not found", name)
		}
		if err := sink.Write(event); err != nil {
			return written, fmt.Errorf("dispatcher: sink %q write error: %w", name, err)
		}
		written++
	}
	return written, nil
}
