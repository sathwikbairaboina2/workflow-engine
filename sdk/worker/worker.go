// Package worker runs workflow and activity code against wfd: it long-polls tasks, replays workflow
// history through the deterministic runtime and executes activities.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/sdk/activity"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/client"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/internal/naming"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/internal/wfrt"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// NondeterminismError is returned when workflow code diverges from recorded history.
type NondeterminismError = wfrt.NondeterminismError

// Options tune a worker. Zero values take the defaults noted below.
type Options struct {
	Identity        string // default hostname:pid
	WorkflowPollers int    // default 2
	ActivityPollers int    // default 4
}

// Worker serves one task queue.
type Worker struct {
	c     *client.Client
	queue string
	o     Options
	reg   *registry
	log   *slog.Logger
}

// New builds a worker for queue.
func New(c *client.Client, queue string, o Options) *Worker {
	if o.Identity == "" {
		host, _ := os.Hostname()
		o.Identity = fmt.Sprintf("%s:%d", host, os.Getpid())
	}
	if o.WorkflowPollers <= 0 {
		o.WorkflowPollers = 2
	}
	if o.ActivityPollers <= 0 {
		o.ActivityPollers = 4
	}
	return &Worker{c: c, queue: queue, o: o, reg: newRegistry(), log: slog.Default().With("queue", queue, "identity", o.Identity)}
}

// RegisterWorkflow registers a workflow function under its function name; it panics on a bad signature.
func (w *Worker) RegisterWorkflow(fn any) { w.reg.addWorkflow(naming.Name(fn), fn) }

// RegisterWorkflowWithName registers a workflow function under an explicit name.
func (w *Worker) RegisterWorkflowWithName(name string, fn any) { w.reg.addWorkflow(name, fn) }

// RegisterActivity registers an activity function under its function name.
func (w *Worker) RegisterActivity(fn any) { w.reg.addActivity(naming.Name(fn), fn) }

// RegisterActivityWithName registers an activity function under an explicit name.
func (w *Worker) RegisterActivityWithName(name string, fn any) { w.reg.addActivity(name, fn) }

// Run polls until ctx is canceled and returns nil. In-flight tasks are finished first.
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < w.o.WorkflowPollers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); w.pollLoop(ctx, w.pollWorkflow) }()
	}
	for i := 0; i < w.o.ActivityPollers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); w.pollLoop(ctx, w.pollActivity) }()
	}
	wg.Wait()
	return nil
}

// pollLoop repeats poll until ctx ends, backing off 100ms doubling to 2s while the server is unreachable.
func (w *Worker) pollLoop(ctx context.Context, poll func(context.Context) error) {
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		if err := poll(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.log.Warn("poll failed", "err", err, "retry_in", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			backoff = min(backoff*2, 2*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
	}
}

func (w *Worker) pollWorkflow(ctx context.Context) error {
	task, err := w.c.PollWorkflowTask(ctx, w.queue, w.o.Identity)
	if err != nil || task == nil {
		return err
	}
	w.handleWorkflowTask(ctx, task)
	return nil
}

func (w *Worker) handleWorkflowTask(ctx context.Context, task *wire.WorkflowTask) {
	// Replies are sent even while shutting down: the task is already leased and replaying it is done.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	fail := func(cause, msg string) {
		err := w.c.FailWorkflowTask(rctx, wire.FailWorkflowTaskRequest{LeaseToken: task.LeaseToken, Cause: cause, Message: msg})
		w.reply("fail workflow task", task.RunID, err)
	}
	fn, ok := w.reg.workflow(task.WorkflowType)
	if !ok {
		fail("unknown_workflow_type", "no workflow registered as "+task.WorkflowType)
		return
	}
	cmds, err := wfrt.Replay(task.History, fn)
	if err != nil {
		var nd *wfrt.NondeterminismError
		var pe *wfrt.PanicError
		switch {
		case errors.As(err, &nd):
			fail("nondeterminism", nd.Msg)
		case errors.As(err, &pe):
			fail("panic", fmt.Sprintf("%v\n%s", pe.Value, pe.Stack))
		default:
			fail("panic", err.Error())
		}
		return
	}
	err = w.c.CompleteWorkflowTask(rctx, wire.CompleteWorkflowTaskRequest{LeaseToken: task.LeaseToken, Commands: cmds})
	w.reply("complete workflow task", task.RunID, err)
}

func (w *Worker) pollActivity(ctx context.Context) error {
	task, err := w.c.PollActivityTask(ctx, w.queue, w.o.Identity)
	if err != nil || task == nil {
		return err
	}
	w.handleActivityTask(ctx, task)
	return nil
}

func (w *Worker) handleActivityTask(ctx context.Context, task *wire.ActivityTask) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	fail := func(f wire.Failure) {
		err := w.c.FailActivityTask(rctx, wire.FailActivityTaskRequest{LeaseToken: task.LeaseToken, Identity: w.o.Identity, Failure: f})
		w.reply("fail activity task", task.RunID, err)
	}
	a, ok := w.reg.activity(task.ActivityType)
	if !ok {
		fail(wire.Failure{Type: "UnknownActivityType", Message: "no activity registered as " + task.ActivityType, NonRetryable: true})
		return
	}
	deadline := time.UnixMilli(task.DeadlineMS)
	info := activity.Info{WorkflowID: task.WorkflowID, RunID: task.RunID, ActivityType: task.ActivityType,
		ScheduledEventID: task.ScheduledEventID, Attempt: task.Attempt,
		IdempotencyKey: activity.Key(task.RunID, task.ScheduledEventID), Deadline: deadline}
	// An in-flight activity is not cancelled by worker shutdown; only its lease deadline bounds it.
	actCtx, cancelAct := context.WithDeadline(activity.WithInfo(context.WithoutCancel(ctx), info), deadline)
	defer cancelAct()

	res, err := w.invoke(actCtx, a, task.Input)
	if err != nil {
		fail(activity.ToFailure(err))
		return
	}
	err = w.c.CompleteActivityTask(rctx, wire.CompleteActivityTaskRequest{LeaseToken: task.LeaseToken, Identity: w.o.Identity, Result: res})
	w.reply("complete activity task", task.RunID, err)
}

// invoke calls the activity and turns a panic into a failure of type Panic.
func (w *Worker) invoke(ctx context.Context, a activityFn, input *wire.Payload) (res *wire.Payload, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = activity.NewError("Panic", fmt.Sprint(r))
		}
	}()
	return a.call(ctx, input)
}

// reply logs the outcome of a reply to the server. A stale lease is normal after a timeout or a restart.
func (w *Worker) reply(what, runID string, err error) {
	switch {
	case err == nil:
	case client.IsStaleLease(err):
		w.log.Debug("dropped reply for a stale lease", "what", what, "run", runID)
	default:
		w.log.Error("reply failed", "what", what, "run", runID, "err", err)
	}
}
