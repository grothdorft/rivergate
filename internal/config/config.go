// Package config loads and validates the rivergate configuration file.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Source defines an input source for log events.
type Source struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
	Addr string `yaml:"addr,omitempty"`
}

// Sink defines an output destination for log events.
type Sink struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
	Addr string `yaml:"addr,omitempty"`
}

// Route maps a source to one or more sinks.
type Route struct {
	From string   `yaml:"from"`
	To   []string `yaml:"to"`
}

// Config is the top-level configuration structure.
type Config struct {
	Sources []Source `yaml:"sources"`
	Sinks   []Sink   `yaml:"sinks"`
	Routes  []Route  `yaml:"routes"`
}

// Load reads and validates the configuration from the given file path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse yaml: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// validate performs semantic validation of the loaded configuration.
func validate(cfg *Config) error {
	sourceNames := make(map[string]struct{}, len(cfg.Sources))
	for _, s := range cfg.Sources {
		sourceNames[s.Name] = struct{}{}
	}

	for i, route := range cfg.Routes {
		if _, ok := sourceNames[route.From]; !ok {
			return fmt.Errorf("config: route[%d]: unknown source %q", i, route.From)
		}
		if len(route.To) == 0 {
			return fmt.Errorf("config: route[%d]: 'to' must have at least one sink", i)
		}
	}

	return nil
}
