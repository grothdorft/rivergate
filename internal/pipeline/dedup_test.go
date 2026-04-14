package pipeline

import (
	"testing"
	"time"
)

func makeDedupEvent(fields map[string]interface{}) *Event {
	e := NewEvent("test")
	for k, v := range fields {
		e.Set(k, v)
	}
	return e
}

func TestDedupWindow_NoDuplicate(t *testing.T) {
	d := NewDedupWindow(time.Second, nil)
	e := makeDedupEvent(map[string]interface{}{"msg": "hello"})
	if d.IsDuplicate(e) {
		t.Fatal("expected first event not to be a duplicate")
	}
}

func TestDedupWindow_DetectsDuplicate(t *testing.T) {
	d := NewDedupWindow(time.Second, nil)
	e := makeDedupEvent(map[string]interface{}{"msg": "hello"})
	d.IsDuplicate(e)
	if !d.IsDuplicate(e) {
		t.Fatal("expected second identical event to be a duplicate")
	}
}

func TestDedupWindow_DifferentEventsNotDuplicate(t *testing.T) {
	d := NewDedupWindow(time.Second, nil)
	e1 := makeDedupEvent(map[string]interface{}{"msg": "hello"})
	e2 := makeDedupEvent(map[string]interface{}{"msg": "world"})
	d.IsDuplicate(e1)
	if d.IsDuplicate(e2) {
		t.Fatal("expected different event not to be a duplicate")
	}
}

func TestDedupWindow_ExpiresAfterTTL(t *testing.T) {
	d := NewDedupWindow(10*time.Millisecond, nil)
	e := makeDedupEvent(map[string]interface{}{"msg": "hello"})
	d.IsDuplicate(e)
	time.Sleep(20 * time.Millisecond)
	if d.IsDuplicate(e) {
		t.Fatal("expected expired entry not to be a duplicate")
	}
}

func TestDedupWindow_FieldSubset(t *testing.T) {
	d := NewDedupWindow(time.Second, []string{"host"})
	e1 := makeDedupEvent(map[string]interface{}{"host": "srv1", "msg": "a"})
	e2 := makeDedupEvent(map[string]interface{}{"host": "srv1", "msg": "b"})
	d.IsDuplicate(e1)
	// e2 differs only in msg, but dedup key is only "host" — should be duplicate.
	if !d.IsDuplicate(e2) {
		t.Fatal("expected event with same host to be a duplicate")
	}
}

func TestDedupWindow_Size(t *testing.T) {
	d := NewDedupWindow(time.Second, nil)
	e1 := makeDedupEvent(map[string]interface{}{"msg": "a"})
	e2 := makeDedupEvent(map[string]interface{}{"msg": "b"})
	d.IsDuplicate(e1)
	d.IsDuplicate(e2)
	if d.Size() != 2 {
		t.Fatalf("expected size 2, got %d", d.Size())
	}
}

func TestDedupWindow_EvictsExpiredOnCheck(t *testing.T) {
	d := NewDedupWindow(10*time.Millisecond, nil)
	e1 := makeDedupEvent(map[string]interface{}{"msg": "a"})
	d.IsDuplicate(e1)
	time.Sleep(20 * time.Millisecond)
	e2 := makeDedupEvent(map[string]interface{}{"msg": "b"})
	d.IsDuplicate(e2)
	// e1 should have been evicted; only e2 remains.
	if d.Size() != 1 {
		t.Fatalf("expected size 1 after eviction, got %d", d.Size())
	}
}
