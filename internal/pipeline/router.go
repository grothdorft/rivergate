package pipeline

import (
	"fmt"

	"github.com/yourorg/rivergate/internal/config"
)

// Sink is the interface that wraps the Write method.
// All sink implementations must satisfy this interface.
type Sink interface {
	Write(event *Event) error
	Name() string
}

// Router routes events from sources to their configured sinks.
type Router struct {
	// routes maps source name -> list of sink names.
	routes map[string][]string
	sinks  map[string]Sink
}

// NewRouter builds a Router from the provided config and registered sinks.
func NewRouter(cfg *config.Config, sinks map[string]Sink) (*Router, error) {
	r := &Router{
		routes: make(map[string][]string),
		sinks:  sinks,
	}
	for _, route := range cfg.Routes {
		for _, sinkName := range route.To {
			if _, ok := sinks[sinkName]; !ok {
				return nil, fmt.Errorf("router: unknown sink %q in route for source %q", sinkName, route.From)
			}
		}
		r.routes[route.From] = route.To
	}
	return r, nil
}

// Route sends the event to all sinks configured for its source.
// It returns a combined error if any sink fails.
func (r *Router) Route(event *Event) error {
	sinkNames, ok := r.routes[event.Source]
	if !ok {
		// No route configured for this source; drop silently.
		return nil
	}
	var errs []error
	for _, name := range sinkNames {
		sink, ok := r.sinks[name]
		if !ok {
			continue
		}
		if err := sink.Write(event); err != nil {
			errs = append(errs, fmt.Errorf("sink %q: %w", name, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("route errors: %v", errs)
	}
	return nil
}
