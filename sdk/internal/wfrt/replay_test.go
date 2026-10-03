package wfrt

import (
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func act(seq int64, name string) wire.Command {
	return wire.Command{Type: wire.ScheduleActivity, Seq: seq, ActivityType: name}
}

func timer(seq int64, d time.Duration) wire.Command {
	return wire.Command{Type: wire.StartTimer, Seq: seq, DurationMS: d.Milliseconds()}
}

func replay(t *testing.T, b *hist, fn any) []wire.Command {
	t.Helper()
	cmds, err := Replay(b.evs, fn)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return cmds
}

func wantResult[T comparable](t *testing.T, cmds []wire.Command, want T) {
	t.Helper()
	if len(cmds) != 1 || cmds[0].Type != wire.CompleteWorkflow {
		t.Fatalf("commands %+v", cmds)
	}
	var got T
	if err := cmds[0].Result.Decode(&got); err != nil || got != want {
		t.Fatalf("result %v (err %v), want %v", got, err, want)
	}
}

// plusOne calls activity "A" with its input and returns the result plus one.
func plusOne(ctx Context, in int) (int, error) {
	var r int
	if err := ExecuteActivity(ctx, ActivityOptions{}, "A", in).Get(ctx, &r); err != nil {
		return 0, err
	}
	return r + 1, nil
}

func TestReplayFirstStepSchedulesActivity(t *testing.T) {
	cmds := replay(t, newHist("W", 41).task(), plusOne)
	if len(cmds) != 1 || cmds[0].Type != wire.ScheduleActivity || cmds[0].Seq != 1 || cmds[0].ActivityType != "A" {
		t.Fatalf("commands %+v", cmds)
	}
	var in int
	cmds[0].Input.Decode(&in)
	if in != 41 || cmds[0].StartToCloseMS != 10000 || cmds[0].RetryPolicy == nil {
		t.Fatalf("command %+v input %d", cmds[0], in)
	}
}

func TestReplayResolvesActivityAndCompletes(t *testing.T) {
	b := newHist("W", 41).task()
	ids := b.completed(act(1, "A"))
	b.activityDone(ids[0], 41).task()
	wantResult(t, replay(t, b, plusOne), 42)
}

func TestReplaySkipsFailedTaskStep(t *testing.T) {
	b := newHist("W", 41).task()
	ids := b.completed(act(1, "A"))
	b.activityDone(ids[0], 41)
	b.task().failedTask("timeout") // the attempt that never completed
	b.task()
	wantResult(t, replay(t, b, plusOne), 42)

	// a failed first attempt must not run code or shift matching either
	b2 := newHist("W", 41).task().failedTask("panic")
	b2.task()
	cmds := replay(t, b2, plusOne)
	if len(cmds) != 1 || cmds[0].Seq != 1 {
		t.Fatalf("commands %+v", cmds)
	}
}

func TestReplayNondeterminism(t *testing.T) {
	b := newHist("W", 1).task()
	b.completed(act(1, "A"))
	b.signal("x", 1).task()
	callsB := func(ctx Context, in int) (int, error) {
		var r int
		err := ExecuteActivity(ctx, ActivityOptions{}, "B", in).Get(ctx, &r)
		return r, err
	}
	_, err := Replay(b.evs, callsB)
	var nd *NondeterminismError
	if !errors.As(err, &nd) || !strings.Contains(nd.Msg, `"A"`) || !strings.Contains(nd.Msg, `"B"`) {
		t.Fatalf("err = %v", err)
	}
	sleeps := func(ctx Context, in int) (int, error) { return 0, Sleep(ctx, time.Hour) }
	_, err = Replay(b.evs, sleeps)
	if !errors.As(err, &nd) {
		t.Fatalf("Sleep vs ActivityTaskScheduled: err = %v", err)
	}
}

func TestReplayExtraCommandInHistory(t *testing.T) {
	b := newHist("W", 1).task()
	b.completed(act(1, "A"), act(2, "B"))
	b.signal("x", 1).task()
	one := func(ctx Context, in int) (int, error) {
		ExecuteActivity(ctx, ActivityOptions{}, "A", in)
		_ = Sleep(ctx, 0)
		return 0, SleepForever(ctx)
	}
	_, err := Replay(b.evs, one)
	var nd *NondeterminismError
	if !errors.As(err, &nd) {
		t.Fatalf("err = %v", err)
	}
}

// SleepForever parks the workflow on a channel nothing ever signals.
func SleepForever(ctx Context) error {
	var v int
	GetSignalChannel(ctx, "never").Receive(ctx, &v)
	return ErrCanceled
}

func TestNowIsDeterministic(t *testing.T) {
	nowMS := func(ctx Context, in int) (int64, error) { return Now(ctx).UnixMilli(), nil }
	for i := 0; i < 50; i++ {
		wantResult(t, replay(t, newHist("W", 0).taskAt(1_700_000_000_123), nowMS), int64(1_700_000_000_123))
	}
}

func TestSideEffectRecordedValueWins(t *testing.T) {
	calls := 0
	wf := func(ctx Context, in int) (int, error) {
		var n int
		SideEffect(ctx, func() any { calls++; return 1 }).Get(&n)
		var s string
		GetSignalChannel(ctx, "go").Receive(ctx, &s)
		return n, nil
	}
	cmds := replay(t, newHist("W", 0).task(), wf)
	if calls != 1 || len(cmds) != 1 || cmds[0].Type != wire.RecordMarker || cmds[0].Seq != 1 {
		t.Fatalf("first run: calls=%d cmds=%+v", calls, cmds)
	}
	var v int
	cmds[0].Value.Decode(&v)
	if v != 1 {
		t.Fatalf("recorded value %d", v)
	}

	calls = 0
	b := newHist("W", 0).task()
	b.completed(wire.Command{Type: wire.RecordMarker, Seq: 1, Value: wire.MustEncode(99)})
	b.signal("go", "x").task()
	wantResult(t, replay(t, b, wf), 99)
	if calls != 0 {
		t.Fatalf("side effect function called %d times on replay", calls)
	}
}

func TestReplaySignalsAndSelector(t *testing.T) {
	wf := func(ctx Context, in int) (string, error) {
		var got string
		timerFut := NewTimer(ctx, time.Hour)
		ch := GetSignalChannel(ctx, "go")
		fired := ""
		err := NewSelector(ctx).
			AddFuture(timerFut, func(Future) { fired = "timer" }).
			AddReceive(ch, func(c ReceiveChannel) { c.Receive(ctx, &got); fired = "signal" }).
			Select(ctx)
		if err != nil {
			return "", err
		}
		return fired + ":" + got, nil
	}
	first := replay(t, newHist("W", 0).task(), wf)
	if len(first) != 1 || first[0].Type != wire.StartTimer || first[0].Seq != 1 || first[0].DurationMS != time.Hour.Milliseconds() {
		t.Fatalf("first step %+v", first)
	}
	b := newHist("W", 0).task()
	b.completed(timer(1, time.Hour))
	b.signal("go", "hi").task()
	wantResult(t, replay(t, b, wf), "signal:hi")
}

func TestReplayTimerFired(t *testing.T) {
	wf := func(ctx Context, in int) (string, error) { return "woke", Sleep(ctx, time.Second) }
	b := newHist("W", 0).task()
	ids := b.completed(timer(1, time.Second))
	b.timerFired(ids[0]).task()
	wantResult(t, replay(t, b, wf), "woke")
}

func TestReplayCancel(t *testing.T) {
	wf := func(ctx Context, in int) error { return Sleep(ctx, time.Hour) }
	b := newHist("W", 0).task()
	b.completed(timer(1, time.Hour))
	b.cancel().task()
	cmds := replay(t, b, wf)
	if len(cmds) != 1 || cmds[0].Type != wire.CancelWorkflow {
		t.Fatalf("commands %+v", cmds)
	}
}

func TestReplayTrailingCloseAllowed(t *testing.T) {
	wf := func(ctx Context, in int) (string, error) { return "done", nil }
	b := newHist("W", 0).task()
	b.completed() // the close command was dropped by the server because a signal was buffered
	b.signal("late", 1).task()
	wantResult(t, replay(t, b, wf), "done")
}

func TestReplayActivityFailure(t *testing.T) {
	var seen *ActivityError
	wf := func(ctx Context, in int) (int, error) {
		err := ExecuteActivity(ctx, ActivityOptions{}, "A", in).Get(ctx, nil)
		errors.As(err, &seen)
		return 0, err
	}
	b := newHist("W", 0).task()
	ids := b.completed(act(1, "A"))
	b.activityFailed(ids[0], wire.Failure{Type: "Boom", Message: "nope"}).task()
	cmds := replay(t, b, wf)
	if seen == nil || seen.Failure.Type != "Boom" || seen.ActivityType != "A" {
		t.Fatalf("activity error %+v", seen)
	}
	if len(cmds) != 1 || cmds[0].Type != wire.FailWorkflow || cmds[0].Failure.Type != "Boom" {
		t.Fatalf("commands %+v", cmds)
	}
}

func TestReplayPanicIsReported(t *testing.T) {
	wf := func(ctx Context, in int) error { panic("kaboom") }
	_, err := Replay(newHist("W", 0).task().evs, wf)
	var pe *PanicError
	if !errors.As(err, &pe) || pe.Value != "kaboom" {
		t.Fatalf("err = %v", err)
	}
}

func TestReplayGoroutineCount(t *testing.T) {
	wf := func(ctx Context, in int) error {
		for i := 0; i < 3; i++ {
			Go(ctx, func(ctx Context) { SleepForever(ctx) })
		}
		return SleepForever(ctx)
	}
	before := runtime.NumGoroutine()
	for i := 0; i < 100; i++ {
		replay(t, newHist("W", 0).task(), wf)
	}
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > before+2 {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines: %d before, %d after", before, runtime.NumGoroutine())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestValidateWorkflowFunc(t *testing.T) {
	good := []any{
		func(ctx Context, in int) (int, error) { return 0, nil },
		func(ctx Context, in struct{ A string }) error { return nil },
	}
	for _, f := range good {
		if err := ValidateWorkflowFunc(f); err != nil {
			t.Errorf("%T rejected: %v", f, err)
		}
	}
	bad := []any{
		42,
		func(in int) error { return nil },
		func(ctx Context) error { return nil },
		func(ctx Context, in int) int { return 0 },
		func(ctx Context, in int) (int, int) { return 0, 0 },
	}
	for _, f := range bad {
		if err := ValidateWorkflowFunc(f); err == nil {
			t.Errorf("%T accepted", f)
		}
	}
}
