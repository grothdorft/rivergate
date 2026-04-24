package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func tempCheckpointPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "checkpoint.json")
}

func TestCheckpoint_SetAndGet(t *testing.T) {
	cp, err := NewCheckpoint(tempCheckpointPath(t))
	if err != nil {
		t.Fatalf("NewCheckpoint: %v", err)
	}
	if err := cp.Set("stdin", 42); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := cp.Get("stdin"); got != 42 {
		t.Errorf("Get = %d, want 42", got)
	}
}

func TestCheckpoint_GetMissingReturnsZero(t *testing.T) {
	cp, _ := NewCheckpoint(tempCheckpointPath(t))
	if got := cp.Get("nonexistent"); got != 0 {
		t.Errorf("Get missing = %d, want 0", got)
	}
}

func TestCheckpoint_PersistsAcrossReload(t *testing.T) {
	path := tempCheckpointPath(t)
	cp, _ := NewCheckpoint(path)
	_ = cp.Set("kafka", 99)

	cp2, err := NewCheckpoint(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := cp2.Get("kafka"); got != 99 {
		t.Errorf("reloaded offset = %d, want 99", got)
	}
}

func TestCheckpoint_Snapshot(t *testing.T) {
	cp, _ := NewCheckpoint(tempCheckpointPath(t))
	_ = cp.Set("a", 1)
	_ = cp.Set("b", 2)

	snap := cp.Snapshot()
	if snap["a"] != 1 || snap["b"] != 2 {
		t.Errorf("Snapshot = %v, want a=1 b=2", snap)
	}
	// Mutations to snapshot must not affect internal state.
	snap["a"] = 999
	if cp.Get("a") != 1 {
		t.Error("snapshot mutation leaked into checkpoint")
	}
}

func TestCheckpoint_MissingFileIsOK(t *testing.T) {
	_, err := NewCheckpoint(filepath.Join(t.TempDir(), "no_such.json"))
	if err != nil {
		t.Errorf("expected no error for missing file, got %v", err)
	}
}

func TestCheckpoint_CorruptFileReturnsError(t *testing.T) {
	path := tempCheckpointPath(t)
	_ = os.WriteFile(path, []byte("not json{"), 0o644)
	_, err := NewCheckpoint(path)
	if err == nil {
		t.Error("expected error for corrupt checkpoint file")
	}
}
