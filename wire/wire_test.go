package wire

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPayloadRoundTrip(t *testing.T) {
	p, err := Encode(map[string]int{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	if p.Encoding != "json" {
		t.Fatalf("encoding %q", p.Encoding)
	}
	var m map[string]int
	if err := p.Decode(&m); err != nil || m["a"] != 1 {
		t.Fatalf("decode %v %v", m, err)
	}
	b, _ := json.Marshal(p)
	if string(b) != `{"encoding":"json","data":{"a":1}}` {
		t.Fatalf("json %s", b)
	}
}

func TestDecodeNilPayloadLeavesTarget(t *testing.T) {
	x := 7
	if err := (*Payload)(nil).Decode(&x); err != nil || x != 7 {
		t.Fatalf("x=%d err=%v", x, err)
	}
}

func TestDecodeUnknownEncoding(t *testing.T) {
	var x int
	err := (&Payload{Encoding: "proto"}).Decode(&x)
	if err == nil || !strings.Contains(err.Error(), "unsupported encoding") {
		t.Fatalf("err %v", err)
	}
}

func TestEventAttrsRoundTrip(t *testing.T) {
	ev, err := NewEvent(5, ActivityTaskScheduled, 1700000000000, ActivityTaskScheduledAttrs{Seq: 3, ActivityType: "Debit"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(ev)
	var back Event
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	var a ActivityTaskScheduledAttrs
	if err := back.DecodeAttrs(&a); err != nil || a.Seq != 3 || a.ActivityType != "Debit" {
		t.Fatalf("attrs %+v %v", a, err)
	}
	if back.EventID != 5 || back.Type != ActivityTaskScheduled || back.Time != 1700000000000 {
		t.Fatalf("event %+v", back)
	}
}

func TestIsCommandEvent(t *testing.T) {
	for _, ty := range []EventType{ActivityTaskScheduled, TimerStarted, MarkerRecorded, WorkflowExecutionCompleted, WorkflowExecutionFailed, WorkflowExecutionCanceled} {
		if !IsCommandEvent(ty) {
			t.Errorf("%s should be a command event", ty)
		}
	}
	for _, ty := range []EventType{WorkflowTaskStarted, TimerFired, WorkflowExecutionSignaled} {
		if IsCommandEvent(ty) {
			t.Errorf("%s should not be a command event", ty)
		}
	}
}

func TestPayloadSize(t *testing.T) {
	if (*Payload)(nil).Size() != 0 {
		t.Fatal("nil size")
	}
	p := MustEncode("abc")
	if p.Size() != len(p.Data) {
		t.Fatal("size")
	}
}

func TestEventTypeFor(t *testing.T) {
	cases := map[CommandType]EventType{
		ScheduleActivity: ActivityTaskScheduled, StartTimer: TimerStarted, RecordMarker: MarkerRecorded,
		CompleteWorkflow: WorkflowExecutionCompleted, FailWorkflow: WorkflowExecutionFailed, CancelWorkflow: WorkflowExecutionCanceled,
	}
	for c, e := range cases {
		if EventTypeFor(c) != e {
			t.Errorf("%s -> %s", c, EventTypeFor(c))
		}
	}
	if !(Command{Type: FailWorkflow}).IsClose() || (Command{Type: StartTimer}).IsClose() {
		t.Fatal("IsClose")
	}
}
