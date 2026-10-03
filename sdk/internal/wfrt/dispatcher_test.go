package wfrt

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestDispatcherDeterministicOrder(t *testing.T) {
	for run := 0; run < 1000; run++ {
		d := &dispatcher{}
		var log []string
		flag := false
		d.spawn(func() {
			log = append(log, "a1")
			d.spawn(func() { log = append(log, "b1"); flag = true })
			d.block(func() bool { return flag })
			log = append(log, "a2")
		})
		if err := d.runUntilBlocked(func() bool { return false }); err != nil {
			t.Fatal(err)
		}
		d.close()
		if !reflect.DeepEqual(log, []string{"a1", "b1", "a2"}) {
			t.Fatalf("run %d: %v", run, log)
		}
	}
}

func TestDispatcherInterleaving(t *testing.T) {
	var want []string
	for run := 0; run < 200; run++ {
		d := &dispatcher{}
		var log []string
		flags := make([]int, 3) // flags[i] counts the steps coroutine i has completed
		for i := 0; i < 3; i++ {
			d.spawn(func() {
				for step := 1; step <= 3; step++ {
					log = append(log, fmt.Sprintf("c%d-%d", i, step))
					flags[i] = step
					prev := (i + 2) % 3
					d.block(func() bool { return flags[prev] >= step })
				}
			})
		}
		if err := d.runUntilBlocked(func() bool { return false }); err != nil {
			t.Fatal(err)
		}
		d.close()
		if run == 0 {
			want = log
			if len(want) != 9 {
				t.Fatalf("expected 9 log entries, got %v", log)
			}
			continue
		}
		if !reflect.DeepEqual(log, want) {
			t.Fatalf("run %d diverged:\n got %v\nwant %v", run, log, want)
		}
	}
}

func TestDispatcherStopsWhenAllBlocked(t *testing.T) {
	d := &dispatcher{}
	defer d.close()
	ready, finished := false, false
	d.spawn(func() { d.block(func() bool { return ready }); finished = true })
	if err := d.runUntilBlocked(func() bool { return false }); err != nil {
		t.Fatal(err)
	}
	if finished {
		t.Fatal("coroutine finished while its condition was false")
	}
	ready = true
	if err := d.runUntilBlocked(func() bool { return false }); err != nil {
		t.Fatal(err)
	}
	if !finished {
		t.Fatal("coroutine did not finish after its condition became true")
	}
}

func TestDispatcherPanicCaptured(t *testing.T) {
	d := &dispatcher{}
	defer d.close()
	d.spawn(func() { panic("boom") })
	err := d.runUntilBlocked(func() bool { return false })
	pe, ok := err.(*PanicError)
	if !ok || pe.Value != "boom" || !strings.Contains(pe.Stack, "dispatcher_test.go") {
		t.Fatalf("err = %#v", err)
	}
}

func TestDispatcherNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 100; i++ {
		d := &dispatcher{}
		for j := 0; j < 5; j++ {
			d.spawn(func() { d.block(func() bool { return false }) })
		}
		if err := d.runUntilBlocked(func() bool { return false }); err != nil {
			t.Fatal(err)
		}
		d.close()
	}
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > before+2 {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines: %d before, %d after", before, runtime.NumGoroutine())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDispatcherCloseUnstartedCoroutine(t *testing.T) {
	before := runtime.NumGoroutine()
	d := &dispatcher{}
	d.spawn(func() { t.Error("must never run") })
	d.close()
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatal("unstarted coroutine goroutine leaked")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDispatcherStopFunc(t *testing.T) {
	d := &dispatcher{}
	defer d.close()
	firstDone, secondRan := false, false
	d.spawn(func() { firstDone = true })
	d.spawn(func() { secondRan = true })
	if err := d.runUntilBlocked(func() bool { return firstDone }); err != nil {
		t.Fatal(err)
	}
	if !firstDone || secondRan {
		t.Fatalf("firstDone=%v secondRan=%v", firstDone, secondRan)
	}
}
