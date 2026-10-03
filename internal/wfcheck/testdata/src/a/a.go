package a

import (
	"math/rand"
	randv2 "math/rand/v2"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/sdk/workflow"
)

func Bad(ctx workflow.Context, in string) error {
	_ = time.Now()          // want `time.Now is not deterministic in workflow code; use workflow.Now`
	time.Sleep(time.Second) // want `time.Sleep is not deterministic in workflow code; use workflow.Sleep`
	_ = rand.Intn(3)        // want `math/rand is not deterministic in workflow code; use workflow.SideEffect`
	go func() {}()          // want `go statement in workflow code; use workflow.Go`
	select {}               // want `select statement in workflow code; use workflow.NewSelector`
}

func Timers(ctx workflow.Context) {
	<-time.After(time.Second)     // want `time.After is not deterministic in workflow code; use workflow.NewTimer`
	_ = time.Tick(time.Second)    // want `time.Tick is not deterministic in workflow code; use workflow.NewTimer`
	_ = time.NewTimer(time.Hour)  // want `time.NewTimer is not deterministic in workflow code; use workflow.NewTimer`
	_ = time.NewTicker(time.Hour) // want `time.NewTicker is not deterministic in workflow code; use workflow.NewTimer`
	_ = time.Since(time.Time{})   // want `time.Since is not deterministic in workflow code; use workflow.Now`
}

func RandV2(ctx workflow.Context) int {
	return randv2.IntN(10) // want `math/rand/v2 is not deterministic in workflow code; use workflow.SideEffect`
}

func RandMethod(ctx workflow.Context) int {
	r := rand.New(rand.NewSource(1)) // want `math/rand is not deterministic in workflow code; use workflow.SideEffect` `math/rand is not deterministic in workflow code; use workflow.SideEffect`
	return r.Intn(5)                 // want `math/rand is not deterministic in workflow code; use workflow.SideEffect`
}

func Nested(ctx workflow.Context) {
	f := func() {
		_ = time.Now() // want `time.Now is not deterministic in workflow code; use workflow.Now`
	}
	f()
}

func Pointer(ctx *workflow.Context) {
	_ = time.Now() // want `time.Now is not deterministic in workflow code; use workflow.Now`
}

var literal = func(ctx workflow.Context, in int) error {
	_ = time.Now() // want `time.Now is not deterministic in workflow code; use workflow.Now`
	return nil
}

// Allowed: not a workflow function (first parameter is not workflow.Context).
func NotWorkflow(in string) {
	_ = time.Now()
	time.Sleep(time.Second)
	go func() {}()
	select {}
}

func NotFirstParam(in string, ctx workflow.Context) {
	_ = time.Now()
}

// Allowed inside workflow code: constants and types from package time.
func Fine(ctx workflow.Context) time.Duration {
	d := 5 * time.Second
	var t time.Time
	_ = t.Add(d)
	return d
}
