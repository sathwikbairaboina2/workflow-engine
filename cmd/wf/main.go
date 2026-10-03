// Command wf is the command-line client for wfd.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/sdk/client"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

const usage = `usage: wf [--server URL] <command> [arguments] [flags]

commands:
  start <type> --id ID [--queue transfers] [--input JSON]   start a workflow, print its run id
  describe <id>                                             show status, result and pending work
  history <id> [--json]                                     show the event history
  signal <id> <name> [--payload JSON]                       send a signal
  cancel <id> [--reason TEXT]                               request cancellation
  result <id> [--timeout 30s]                               wait for the result and print it
  health                                                    check the server

The server defaults to $WF_SERVER or http://localhost:5400.
`

// Exit codes: 0 ok, 1 server or workflow error, 2 usage error.
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

func run(args []string, stdout, stderr io.Writer) int {
	server := os.Getenv("WF_SERVER")
	if server == "" {
		server = "http://localhost:5400"
	}
	for len(args) > 0 && strings.HasPrefix(args[0], "--server") {
		if args[0] == "--server" {
			if len(args) < 2 {
				fmt.Fprint(stderr, usage)
				return exitUsage
			}
			server, args = args[1], args[2:]
		} else if v, ok := strings.CutPrefix(args[0], "--server="); ok {
			server, args = v, args[1:]
		} else {
			break
		}
	}
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return exitUsage
	}
	cmd, args := args[0], args[1:]
	a := &app{out: stdout, err: stderr, server: server}
	switch cmd {
	case "start":
		return a.start(args)
	case "describe":
		return a.describe(args)
	case "history":
		return a.history(args)
	case "signal":
		return a.signal(args)
	case "cancel":
		return a.cancel(args)
	case "result":
		return a.result(args)
	case "health":
		return a.health(args)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return exitOK
	}
	fmt.Fprintf(stderr, "wf: unknown command %q\n%s", cmd, usage)
	return exitUsage
}

type app struct {
	out, err io.Writer
	server   string
}

func (a *app) client(opts ...client.Option) *client.Client { return client.New(a.server, opts...) }

func (a *app) fail(err error) int {
	fmt.Fprintln(a.err, "wf:", err)
	return exitError
}

func (a *app) usageErr(format string, args ...any) int {
	fmt.Fprintf(a.err, "wf: "+format+"\n", args...)
	return exitUsage
}

// parse takes n positional arguments first, then parses flags from the rest, so flags may follow positionals.
func (a *app) parse(name string, n int, args []string, setup func(fs *flag.FlagSet)) ([]string, bool) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(a.err)
	if setup != nil {
		setup(fs)
	}
	if len(args) < n {
		fmt.Fprintf(a.err, "wf %s: needs %d argument(s)\n", name, n)
		return nil, false
	}
	pos := args[:n]
	if err := fs.Parse(args[n:]); err != nil || fs.NArg() > 0 {
		if err == nil {
			fmt.Fprintf(a.err, "wf %s: unexpected argument %q\n", name, fs.Arg(0))
		}
		return nil, false
	}
	return pos, true
}

func rawJSON(flagName, v string) (json.RawMessage, error) {
	if v == "" {
		return nil, nil
	}
	if !json.Valid([]byte(v)) {
		return nil, fmt.Errorf("--%s is not valid JSON: %s", flagName, v)
	}
	return json.RawMessage(v), nil
}

func (a *app) start(args []string) int {
	var id, queue, input string
	pos, ok := a.parse("start", 1, args, func(fs *flag.FlagSet) {
		fs.StringVar(&id, "id", "", "workflow id (required)")
		fs.StringVar(&queue, "queue", "transfers", "task queue")
		fs.StringVar(&input, "input", "", "workflow input as JSON")
	})
	if !ok {
		return exitUsage
	}
	if id == "" {
		return a.usageErr("start: --id is required")
	}
	raw, err := rawJSON("input", input)
	if err != nil {
		return a.usageErr("start: %v", err)
	}
	var in any
	if raw != nil {
		in = raw
	}
	run, err := a.client().Start(context.Background(), client.StartOptions{ID: id, TaskQueue: queue}, pos[0], in)
	if err != nil {
		return a.fail(err)
	}
	fmt.Fprintln(a.out, run)
	return exitOK
}

func (a *app) describe(args []string) int {
	pos, ok := a.parse("describe", 1, args, nil)
	if !ok {
		return exitUsage
	}
	d, err := a.client().Describe(context.Background(), pos[0])
	if err != nil {
		return a.fail(err)
	}
	return a.printJSON(d)
}

func (a *app) printJSON(v any) int {
	enc := json.NewEncoder(a.out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return a.fail(err)
	}
	return exitOK
}

func (a *app) history(args []string) int {
	var asJSON bool
	pos, ok := a.parse("history", 1, args, func(fs *flag.FlagSet) { fs.BoolVar(&asJSON, "json", false, "print raw JSON events") })
	if !ok {
		return exitUsage
	}
	events, err := a.client().History(context.Background(), pos[0], "")
	if err != nil {
		return a.fail(err)
	}
	if asJSON {
		if events == nil {
			events = []wire.Event{}
		}
		return a.printJSON(events)
	}
	tw := tabwriter.NewWriter(a.out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tTYPE\tDETAILS")
	for _, e := range events {
		fmt.Fprintf(tw, "%d\t%s\t%s\n", e.EventID, e.Type, details(e))
	}
	tw.Flush()
	return exitOK
}

// details is a short human summary of an event's attributes.
func details(e wire.Event) string {
	switch e.Type {
	case wire.WorkflowExecutionStarted:
		var x wire.WorkflowExecutionStartedAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("type=%s queue=%s", x.WorkflowType, x.TaskQueue)
	case wire.WorkflowTaskScheduled:
		var x wire.WorkflowTaskScheduledAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("attempt=%d", x.Attempt)
	case wire.WorkflowTaskStarted:
		var x wire.WorkflowTaskStartedAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("scheduled=%d identity=%s", x.ScheduledEventID, x.Identity)
	case wire.WorkflowTaskCompleted:
		var x wire.WorkflowTaskCompletedAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("scheduled=%d started=%d", x.ScheduledEventID, x.StartedEventID)
	case wire.WorkflowTaskFailed:
		var x wire.WorkflowTaskFailedAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("cause=%s", x.Cause)
	case wire.ActivityTaskScheduled:
		var x wire.ActivityTaskScheduledAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("seq=%d activity=%s", x.Seq, x.ActivityType)
	case wire.ActivityTaskStarted:
		var x wire.ActivityTaskStartedAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("scheduled=%d attempt=%d", x.ScheduledEventID, x.Attempt)
	case wire.ActivityTaskCompleted:
		var x wire.ActivityTaskCompletedAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("scheduled=%d", x.ScheduledEventID)
	case wire.ActivityTaskFailed:
		var x wire.ActivityTaskFailedAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("scheduled=%d attempts=%d failure=%s", x.ScheduledEventID, x.Attempts, x.Failure.Type)
	case wire.ActivityTaskTimedOut:
		var x wire.ActivityTaskTimedOutAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("scheduled=%d attempts=%d", x.ScheduledEventID, x.Attempts)
	case wire.TimerStarted:
		var x wire.TimerStartedAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("seq=%d duration=%dms", x.Seq, x.DurationMS)
	case wire.TimerFired:
		var x wire.TimerFiredAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("started=%d", x.StartedEventID)
	case wire.MarkerRecorded:
		var x wire.MarkerRecordedAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("seq=%d kind=%s", x.Seq, x.Kind)
	case wire.WorkflowExecutionSignaled:
		var x wire.WorkflowExecutionSignaledAttrs
		_ = e.DecodeAttrs(&x)
		return "name=" + x.Name
	case wire.WorkflowExecutionCancelRequested:
		var x wire.WorkflowExecutionCancelRequestedAttrs
		_ = e.DecodeAttrs(&x)
		return "reason=" + x.Reason
	case wire.WorkflowExecutionFailed:
		var x wire.WorkflowExecutionFailedAttrs
		_ = e.DecodeAttrs(&x)
		return fmt.Sprintf("failure=%s: %s", x.Failure.Type, x.Failure.Message)
	}
	return ""
}

func (a *app) signal(args []string) int {
	var payload string
	pos, ok := a.parse("signal", 2, args, func(fs *flag.FlagSet) { fs.StringVar(&payload, "payload", "", "signal payload as JSON") })
	if !ok {
		return exitUsage
	}
	raw, err := rawJSON("payload", payload)
	if err != nil {
		return a.usageErr("signal: %v", err)
	}
	var p any
	if raw != nil {
		p = raw
	}
	if err := a.client().Signal(context.Background(), pos[0], pos[1], p); err != nil {
		return a.fail(err)
	}
	return exitOK
}

func (a *app) cancel(args []string) int {
	var reason string
	pos, ok := a.parse("cancel", 1, args, func(fs *flag.FlagSet) { fs.StringVar(&reason, "reason", "", "why the workflow is canceled") })
	if !ok {
		return exitUsage
	}
	if err := a.client().Cancel(context.Background(), pos[0], reason); err != nil {
		return a.fail(err)
	}
	return exitOK
}

func (a *app) result(args []string) int {
	timeout := 30 * time.Second
	pos, ok := a.parse("result", 1, args, func(fs *flag.FlagSet) { fs.DurationVar(&timeout, "timeout", timeout, "how long to wait") })
	if !ok {
		return exitUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var out json.RawMessage
	err := a.client().GetResult(ctx, pos[0], &out)
	var wfe *client.WorkflowFailedError
	switch {
	case err == nil:
		if len(out) == 0 {
			out = json.RawMessage("null")
		}
		fmt.Fprintln(a.out, string(out))
		return exitOK
	case errors.As(err, &wfe):
		fmt.Fprintf(a.err, "wf: workflow failed: %s: %s\n", wfe.Failure.Type, wfe.Failure.Message)
		return exitError
	case errors.Is(err, context.DeadlineExceeded):
		return a.fail(fmt.Errorf("no result within %s", timeout))
	}
	return a.fail(err)
}

func (a *app) health(args []string) int {
	if _, ok := a.parse("health", 0, args, nil); !ok {
		return exitUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.client(client.WithRetry(0)).Health(ctx); err != nil {
		return a.fail(err)
	}
	fmt.Fprintln(a.out, "ok")
	return exitOK
}
