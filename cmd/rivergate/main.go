// Package main is the entry point for the rivergate stream processor.
// It loads the declarative config, wires up sources, processors, and sinks,
// then runs the pipeline until the process receives a termination signal.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/yourorg/rivergate/internal/config"
	"github.com/yourorg/rivergate/internal/pipeline"
)

func main() {
	cfgPath := flag.String("config", "rivergate.yaml", "path to the rivergate config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rivergate: failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Build registries.
	sourceReg := pipeline.NewSourceRegistry()
	sinkReg := pipeline.NewSinkRegistry()

	// Register built-in sources.
	sourceReg.Register("stdin", pipeline.NewStdinSource("stdin"))

	// Register built-in sinks.
	sinkReg.Register("stdout", pipeline.NewStdoutSink("stdout"))

	// Wire up the router and dispatcher.
	router := pipeline.NewRouter(cfg, sourceReg)
	dispatcher := pipeline.NewDispatcher(sinkReg)
	metrics := pipeline.NewMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals for graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("rivergate: received signal %s, shutting down", sig)
		cancel()
	}()

	// Start all registered sources.
	events := make(chan *pipeline.Event, 256)

	for _, name := range sourceReg.Names() {
		src, ok := sourceReg.Get(name)
		if !ok {
			continue
		}
		go func(s pipeline.Source) {
			if err := s.Start(ctx, events); err != nil && err != context.Canceled {
				log.Printf("rivergate: source %q exited with error: %v", s.Name(), err)
			}
		}(src)
	}

	log.Printf("rivergate: pipeline started (config: %s)", *cfgPath)

	// Main event loop.
	for {
		select {
		case <-ctx.Done():
			snap := metrics.Snapshot()
			log.Printf("rivergate: shutdown complete — processed=%d dropped=%d",
				snap["processed"], snap["dropped"])
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			sinkNames := router.RouteEvent(ev)
			if len(sinkNames) == 0 {
				metrics.Inc("dropped")
				continue
			}
			for _, sinkName := range sinkNames {
				if werr := dispatcher.Dispatch(ev, sinkName); werr != nil {
					log.Printf("rivergate: dispatch to sink %q failed: %v", sinkName, werr)
					metrics.Inc("errors")
				}
			}
			metrics.Inc("processed")
		}
	}
}
