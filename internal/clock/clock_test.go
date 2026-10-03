package clock

import (
	"testing"
	"time"
)

func TestFakeAdvance(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := NewFake(start)
	f.Advance(1500 * time.Millisecond)
	if got := f.Now().Sub(start); got != 1500*time.Millisecond {
		t.Fatalf("advance moved clock by %v", got)
	}
}

func TestFakeNowMillis(t *testing.T) {
	f := NewFake(time.Date(2026, 1, 1, 0, 0, 0, 5_000_000, time.UTC))
	if NowMS(f) != f.Now().UnixMilli() {
		t.Fatal("NowMS disagrees with Now().UnixMilli()")
	}
}

func TestRealMonotonic(t *testing.T) {
	var r Real
	a := r.Now()
	b := r.Now()
	if b.Before(a) {
		t.Fatalf("real clock went backwards: %v then %v", a, b)
	}
}
