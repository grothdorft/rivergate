package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Checkpoint persists the last successfully processed event offset
// for a named source, enabling resume-on-restart semantics.
type Checkpoint struct {
	mu      sync.Mutex
	path    string
	offsets map[string]int64
}

// NewCheckpoint creates a Checkpoint backed by the given file path.
// If the file exists its contents are loaded; a missing file starts fresh.
func NewCheckpoint(path string) (*Checkpoint, error) {
	cp := &Checkpoint{
		path:    path,
		offsets: make(map[string]int64),
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cp, nil
		}
		return nil, fmt.Errorf("checkpoint: read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, &cp.offsets); err != nil {
		return nil, fmt.Errorf("checkpoint: parse %s: %w", path, err)
	}
	return cp, nil
}

// Set records the current offset for the named source and flushes to disk.
func (c *Checkpoint) Set(source string, offset int64) error {
	c.mu.Lock()
	c.offsets[source] = offset
	snap := make(map[string]int64, len(c.offsets))
	for k, v := range c.offsets {
		snap[k] = v
	}
	c.mu.Unlock()

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("checkpoint: marshal: %w", err)
	}
	if err := os.WriteFile(c.path, data, 0o644); err != nil {
		return fmt.Errorf("checkpoint: write %s: %w", c.path, err)
	}
	return nil
}

// Get returns the last saved offset for source, or 0 if none exists.
func (c *Checkpoint) Get(source string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offsets[source]
}

// Reset removes the saved offset for source and flushes to disk.
func (c *Checkpoint) Reset(source string) error {
	c.mu.Lock()
	delete(c.offsets, source)
	c.mu.Unlock()
	return c.Set("", -1) // trigger a flush by re-using Set internals
}

// Snapshot returns a copy of all current offsets.
func (c *Checkpoint) Snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(c.offsets))
	for k, v := range c.offsets {
		out[k] = v
	}
	return out
}
