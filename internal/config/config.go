package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// FilterRuleConfig defines a single filter condition.
type FilterRuleConfig struct {
	Field    string `yaml:"field"`
	Operator string `yaml:"operator"`
	Value    string `yaml:"value"`
}

// TransformConfig defines a single transformation step.
type TransformConfig struct {
	Op    string `yaml:"op"`
	Field string `yaml:"field"`
	Value string `yaml:"value,omitempty"`
	To    string `yaml:"to,omitempty"`
}

// ProcessorConfig defines a named processor with optional filter and transforms.
type ProcessorConfig struct {
	Name       string             `yaml:"name"`
	Filter     []FilterRuleConfig `yaml:"filter,omitempty"`
	Transforms []TransformConfig  `yaml:"transforms,omitempty"`
}

// SourceConfig defines an input source.
type SourceConfig struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

// SinkConfig defines an output sink.
type SinkConfig struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

// RouteConfig defines how events flow from a source through processors to sinks.
type RouteConfig struct {
	From       string   `yaml:"from"`
	Processors []string `yaml:"processors,omitempty"`
	To         []string `yaml:"to"`
}

// Config is the top-level configuration structure.
type Config struct {
	Sources    []SourceConfig    `yaml:"sources"`
	Sinks      []SinkConfig      `yaml:"sinks"`
	Processors []ProcessorConfig `yaml:"processors,omitempty"`
	Routes     []RouteConfig     `yaml:"routes"`
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
	sourceNames := make(map[string]bool)
	for _, s := range cfg.Sources {
		sourceNames[s.Name] = true
	}

	processorNames := make(map[string]bool)
	for _, p := range cfg.Processors {
		processorNames[p.Name] = true
	}

	for i, r := range cfg.Routes {
		if !sourceNames[r.From] {
			return fmt.Errorf("config: route[%d]: unknown source %q", i, r.From)
		}
		if len(r.To) == 0 {
			return fmt.Errorf("config: route[%d]: missing 'to' sinks", i)
		}
		for _, pName := range r.Processors {
			if !processorNames[pName] {
				return fmt.Errorf("config: route[%d]: unknown processor %q", i, pName)
			}
		}
	}
	return nil
}
