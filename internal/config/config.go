package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// SourceConfig describes a named input source.
type SourceConfig struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"` // e.g. "stdin"
}

// SinkConfig describes a named output sink.
type SinkConfig struct {
	Name   string `yaml:"name"`
	Type   string `yaml:"type"` // e.g. "stdout"
	Format string `yaml:"format,omitempty"` // e.g. "json", "text"
}

// FilterConfig holds a single filter rule.
type FilterConfig struct {
	Field    string `yaml:"field"`
	Operator string `yaml:"operator"` // equals, not_equals, contains, matches
	Value    string `yaml:"value"`
}

// TransformConfig holds a single transform operation.
type TransformConfig struct {
	Op    string `yaml:"op"` // set, delete, rename
	Field string `yaml:"field"`
	Value string `yaml:"value,omitempty"`
	To    string `yaml:"to,omitempty"`
}

// RouteConfig maps a source to a sink with optional filters and transforms.
type RouteConfig struct {
	From       string            `yaml:"from"`
	To         string            `yaml:"to"`
	Filters    []FilterConfig    `yaml:"filters,omitempty"`
	Transforms []TransformConfig `yaml:"transforms,omitempty"`
}

// Config is the top-level rivergate configuration.
type Config struct {
	Sources []SourceConfig `yaml:"sources"`
	Sinks   []SinkConfig   `yaml:"sinks"`
	Routes  []RouteConfig  `yaml:"routes"`
}

// Load reads and parses a YAML config file from the given path.
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

func validate(cfg *Config) error {
	sourceNames := make(map[string]bool, len(cfg.Sources))
	for _, s := range cfg.Sources {
		sourceNames[s.Name] = true
	}
	sinkNames := make(map[string]bool, len(cfg.Sinks))
	for _, s := range cfg.Sinks {
		sinkNames[s.Name] = true
	}
	for i, r := range cfg.Routes {
		if r.To == "" {
			return fmt.Errorf("config: route[%d] missing 'to' field", i)
		}
		if !sourceNames[r.From] {
			return fmt.Errorf("config: route[%d] references unknown source %q", i, r.From)
		}
		if !sinkNames[r.To] {
			return fmt.Errorf("config: route[%d] references unknown sink %q", i, r.To)
		}
	}
	return nil
}
