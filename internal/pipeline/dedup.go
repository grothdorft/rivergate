package pipeline

import (
	"crypto/md5"
	"fmt"
	"sync"
	"time"
)

// DedupWindow holds seen event fingerprints within a TTL window.
type DedupWindow struct {
	mu      sync.Mutex
	seen    map[string]time.Time
	ttl     time.Duration
	fields  []string
}

// NewDedupWindow creates a DedupWindow that deduplicates events based on the
// given fields within the provided TTL. If fields is empty, all fields are
// used via the event's raw field map.
func NewDedupWindow(ttl time.Duration, fields []string) *DedupWindow {
	return &DedupWindow{
		seen:   make(map[string]time.Time),
		ttl:    ttl,
		fields: fields,
	}
}

// IsDuplicate returns true if an identical event fingerprint was seen within
// the TTL window. It also evicts expired entries on each call.
func (d *DedupWindow) IsDuplicate(e *Event) bool {
	key := d.fingerprint(e)
	now := time.Now()

	d.mu.Lock()
	defer d.mu.Unlock()

	// Evict expired entries.
	for k, ts := range d.seen {
		if now.Sub(ts) > d.ttl {
			delete(d.seen, k)
		}
	}

	if ts, ok := d.seen[key]; ok && now.Sub(ts) <= d.ttl {
		return true
	}

	d.seen[key] = now
	return false
}

// Size returns the number of fingerprints currently tracked.
func (d *DedupWindow) Size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}

func (d *DedupWindow) fingerprint(e *Event) string {
	h := md5.New()
	if len(d.fields) == 0 {
		for k, v := range e.Fields {
			fmt.Fprintf(h, "%s=%v;", k, v)
		}
	} else {
		for _, f := range d.fields {
			v, _ := e.Get(f)
			fmt.Fprintf(h, "%s=%v;", f, v)
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
