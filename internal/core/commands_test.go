package core

import (
	"strings"
	"testing"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func TestValidateCommands(t *testing.T) {
	cfg := Config{MaxPayloadBytes: 1024}.withDefaults()
	big := wire.MustEncode(strings.Repeat("x", 2000))
	act := wire.Command{Type: wire.ScheduleActivity, Seq: 1, ActivityType: "A"}
	done := wire.Command{Type: wire.CompleteWorkflow}
	bad := map[string][]wire.Command{
		"unknown type":         {{Type: "Nope"}},
		"activity zero seq":    {{Type: wire.ScheduleActivity, ActivityType: "A"}},
		"activity no type":     {{Type: wire.ScheduleActivity, Seq: 1}},
		"negative timer":       {{Type: wire.StartTimer, Seq: 1, DurationMS: -1}},
		"two closes":           {done, {Type: wire.FailWorkflow}},
		"close not last":       {done, act},
		"oversize activity in": {{Type: wire.ScheduleActivity, Seq: 1, ActivityType: "A", Input: big}},
		"oversize result":      {{Type: wire.CompleteWorkflow, Result: big}},
	}
	for name, cmds := range bad {
		err := ValidateCommands(cmds, cfg)
		wantCode(t, err, CodeInvalid)
		_ = name
	}
	ok := []wire.Command{act,
		{Type: wire.StartTimer, Seq: 2, DurationMS: 5},
		{Type: wire.RecordMarker, Seq: 3, MarkerKind: "side_effect", Value: wire.MustEncode(1)},
		done}
	if err := ValidateCommands(ok, cfg); err != nil {
		t.Fatalf("valid list rejected: %v", err)
	}
	if err := ValidateCommands(nil, cfg); err != nil {
		t.Fatalf("empty list rejected: %v", err)
	}
}
