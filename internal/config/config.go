package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// SourceConfig describes an event source.
type SourceConfig struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

// SinkConfig describes an event sink.
type SinkConfig struct {
	Name   string `yaml:"name"`
	Type   string `yaml:"type"`
	Format string `yaml:"format"`
}

// FilterConfig holds a single filter rule.
type FilterConfig struct {
	Field    string `yaml:"field"`
	Operator string `yaml:"operator"`
	Value    string `yaml:"value"`
}

// TransformConfig holds a single transform operation.
type TransformConfig struct {
	Op    string `yaml:"op"`
	Field string `yaml:"field"`
	Value string `yaml:"value"`
	To    string `yaml:"to"`
}

// RateLimitConfig controls per-route rate limiting.
type RateLimitConfig struct {
	Rate   int    `yaml:"rate"`
	Policy string `yaml:"policy"` // "drop" or "block"
}

// RouteConfig maps a source to one or more sinks with optional processing.
type RouteConfig struct {
	From       string            `yaml:"from"`
	To         []string          `yaml:"to"`
	Filters    []FilterConfig    `yaml:"filters"`
	Transforms []TransformConfig `yaml:"transforms"`
	RateLimit  *RateLimitConfig  `yaml:"rate_limit"`
}

// Config is the top-level rivergate configuration.
type Config struct {
	Sources []SourceConfig `yaml:"sources"`
	Sinks   []SinkConfig   `yaml:"sinks"`
	Routes  []RouteConfig  `yaml:"routes"`
}

// Load reads and validates a YAML config file from the given path.
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

// validate checks the config for required fields and referential integrity.
func validate(cfg *Config) error {
	sourceNames := make(map[string]bool, len(cfg.Sources))
	for _, s := range cfg.Sources {
		if s.Name == "" {
			return fmt.Errorf("config: source missing name")
		}
		sourceNames[s.Name] = true
	}
	for i, r := range cfg.Routes {
		if r.From == "" {
			return fmt.Errorf("config: route[%d] missing 'from'", i)
		}
		if !sourceNames[r.From] {
			return fmt.Errorf("config: route[%d] references unknown source %q", i, r.From)
		}
		if len(r.To) == 0 {
			return fmt.Errorf("config: route[%d] missing 'to'", i)
		}
		if r.RateLimit != nil && r.RateLimit.Policy == "" {
			cfg.Routes[i].RateLimit.Policy = "drop"
		}
	}
	return nil
}
