package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rivergate/rivergate/internal/config"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "rivergate-*.yaml")
	if err != nil {
		t.Fatalf("creating temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("writing temp file: %v", err)
	}
	f.Close()
	return f.Name()
}

func TestLoad_ValidConfig(t *testing.T) {
	yaml := `
sources:
  - name: app-logs
    type: stdin
sinks:
  - name: file-out
    type: file
    options:
      path: /var/log/out.log
routes:
  - from: app-logs
    to: [file-out]
`
	cfg, err := config.Load(writeTemp(t, yaml))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(cfg.Sources) != 1 || cfg.Sources[0].Name != "app-logs" {
		t.Errorf("unexpected sources: %+v", cfg.Sources)
	}
	if len(cfg.Sinks) != 1 || cfg.Sinks[0].Name != "file-out" {
		t.Errorf("unexpected sinks: %+v", cfg.Sinks)
	}
	if len(cfg.Routes) != 1 || cfg.Routes[0].From != "app-logs" {
		t.Errorf("unexpected routes: %+v", cfg.Routes)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := config.Load(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoad_UnknownSourceInRoute(t *testing.T) {
	yaml := `
sources:
  - name: real-source
    type: stdin
sinks:
  - name: real-sink
    type: stdout
routes:
  - from: ghost-source
    to: [real-sink]
`
	_, err := config.Load(writeTemp(t, yaml))
	if err == nil {
		t.Fatal("expected validation error for unknown source, got nil")
	}
}

func TestLoad_MissingToInRoute(t *testing.T) {
	yaml := `
sources:
  - name: src
    type: stdin
sinks:
  - name: snk
    type: stdout
routes:
  - from: src
    to: []
`
	_, err := config.Load(writeTemp(t, yaml))
	if err == nil {
		t.Fatal("expected validation error for empty 'to', got nil")
	}
}
