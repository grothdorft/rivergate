package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Source defines an input stream source.
type Source struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
	Options map[string]string `yaml:"options"`
}

// Sink defines an output stream destination.
type Sink struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
	Options map[string]string `yaml:"options"`
}

// Route maps a source to one or more sinks with optional filters.
type Route struct {
	From string `yaml:"from"`
	To []string `yaml:"to"`
	Filter string `yaml:"filter,omitempty"`
}

// Config is the top-level rivergate configuration.
type Config struct {
	Sources []Source `yaml:"sources"`
	Sinks []Sink `yaml:"sinks"`
	Routes []Route `yaml:"routes"`
}

// Load reads and parses a YAML config file from the given path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading file %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parsing yaml: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: validation failed: %w", err)
	}

	return &cfg, nil
}

// Validate checks that the config is semantically valid.
func (c *Config) Validate() error {
	sourceNames := make(map[string]struct{}, len(c.Sources))
	for _, s := range c.Sources {
		if s.Name == "" {
			return fmt.Errorf("source missing required field 'name'")
		}
		if s.Type == "" {
			return fmt.Errorf("source %q missing required field 'type'", s.Name)
		}
		sourceNames[s.Name] = struct{}{}
	}

	sinkNames := make(map[string]struct{}, len(c.Sinks))
	for _, s := range c.Sinks {
		if s.Name == "" {
			return fmt.Errorf("sink missing required field 'name'")
		}
		if s.Type == "" {
			return fmt.Errorf("sink %q missing required field 'type'", s.Name)
		}
		sinkNames[s.Name] = struct{}{}
	}

	for i, r := range c.Routes {
		if r.From == "" {
			return fmt.Errorf("route[%d] missing required field 'from'", i)
		}
		if _, ok := sourceNames[r.From]; !ok {
			return fmt.Errorf("route[%d] references unknown source %q", i, r.From)
		}
		if len(r.To) == 0 {
			return fmt.Errorf("route[%d] must have at least one sink in 'to'", i)
		}
		for _, sink := range r.To {
			if _, ok := sinkNames[sink]; !ok {
				return fmt.Errorf("route[%d] references unknown sink %q", i, sink)
			}
		}
	}

	return nil
}
