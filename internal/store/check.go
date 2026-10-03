package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

type checkRun struct {
	id, status string
	next       int64
}

type checkEvent struct {
	id    int64
	typ   wire.EventType
	attrs json.RawMessage
}

// CheckConsistency verifies the cross-table invariants of the engine and returns one string per violation,
// each naming the run. It reads, never writes, and is safe to call while the engine is idle (tests and the
// chaos harness call it after load has stopped).
func CheckConsistency(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT run_id, status, next_event_id FROM workflows ORDER BY created_at, run_id`)
	if err != nil {
		return nil, err
	}
	var runs []checkRun
	known := map[string]bool{}
	for rows.Next() {
		var r checkRun
		if err := rows.Scan(&r.id, &r.status, &r.next); err != nil {
			rows.Close()
			return nil, err
		}
		runs = append(runs, r)
		known[r.id] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var out []string
	for _, r := range runs {
		v, err := checkOneRun(ctx, db, r)
		if err != nil {
			return nil, err
		}
		out = append(out, v...)
	}
	for _, table := range []string{"tasks", "timers", "buffered_events"} {
		orows, err := db.QueryContext(ctx, `SELECT DISTINCT run_id FROM `+table)
		if err != nil {
			return nil, err
		}
		for orows.Next() {
			var id string
			if err := orows.Scan(&id); err != nil {
				orows.Close()
				return nil, err
			}
			if !known[id] {
				out = append(out, fmt.Sprintf("run %s: %s rows exist for an unknown run", id, table))
			}
		}
		orows.Close()
	}
	return out, nil
}

func checkOneRun(ctx context.Context, db *sql.DB, r checkRun) ([]string, error) {
	var out []string
	bad := func(format string, args ...any) { out = append(out, "run "+r.id+": "+fmt.Sprintf(format, args...)) }

	evs, err := queryEvents(ctx, db, `SELECT event_id, event_type, attrs FROM history_events WHERE run_id=? ORDER BY event_id`, r.id)
	if err != nil {
		return nil, err
	}
	if int64(len(evs)) != r.next-1 {
		bad("next_event_id is %d but history has %d events", r.next, len(evs))
	}
	for i, e := range evs {
		if e.id != int64(i+1) {
			bad("event ids are not dense: position %d holds id %d", i+1, e.id)
			break
		}
	}
	if len(evs) == 0 || evs[0].typ != wire.WorkflowExecutionStarted {
		bad("first event is not WorkflowExecutionStarted")
	}

	tasks, err := queryTasks(ctx, db, r.id)
	if err != nil {
		return nil, err
	}
	timers, err := queryInt64s(ctx, db, `SELECT started_event_id FROM timers WHERE run_id=?`, r.id)
	if err != nil {
		return nil, err
	}
	buffered, err := queryEvents(ctx, db, `SELECT seq, event_type, attrs FROM buffered_events WHERE run_id=? ORDER BY seq`, r.id)
	if err != nil {
		return nil, err
	}

	if r.status != "running" {
		if len(tasks)+len(timers)+len(buffered) > 0 {
			bad("closed run (%s) still has %d tasks, %d timers, %d buffered events", r.status, len(tasks), len(timers), len(buffered))
		}
		return out, nil
	}

	// Activities.
	resolvedAct := map[int64]bool{}
	resolvedTimer := map[int64]bool{}
	for _, e := range append(append([]checkEvent{}, evs...), buffered...) {
		switch e.typ {
		case wire.ActivityTaskCompleted, wire.ActivityTaskFailed, wire.ActivityTaskTimedOut:
			var a struct {
				ScheduledEventID int64 `json:"scheduled_event_id"`
			}
			_ = json.Unmarshal(e.attrs, &a)
			resolvedAct[a.ScheduledEventID] = true
		case wire.TimerFired:
			var a wire.TimerFiredAttrs
			_ = json.Unmarshal(e.attrs, &a)
			resolvedTimer[a.StartedEventID] = true
		}
	}
	actTasks := map[int64]int{}
	var wfTasks []checkTask
	for _, t := range tasks {
		if t.kind == "activity" {
			actTasks[t.scheduled]++
		} else {
			wfTasks = append(wfTasks, t)
		}
	}
	timerRows := map[int64]int{}
	for _, id := range timers {
		timerRows[id]++
	}
	scheduledAct := map[int64]bool{}
	startedTimer := map[int64]bool{}
	for _, e := range evs {
		switch e.typ {
		case wire.ActivityTaskScheduled:
			scheduledAct[e.id] = true
			switch n := actTasks[e.id]; {
			case resolvedAct[e.id] && n != 0:
				bad("activity %d already has a terminal event but %d task rows remain", e.id, n)
			case !resolvedAct[e.id] && n != 1:
				bad("activity %d is unresolved and has %d task rows, want 1", e.id, n)
			}
		case wire.TimerStarted:
			startedTimer[e.id] = true
			switch n := timerRows[e.id]; {
			case resolvedTimer[e.id] && n != 0:
				bad("timer %d already fired but %d timer rows remain", e.id, n)
			case !resolvedTimer[e.id] && n != 1:
				bad("timer %d has not fired and has %d timer rows, want 1", e.id, n)
			}
		}
	}
	for id := range actTasks {
		if !scheduledAct[id] {
			bad("activity task row for event %d has no ActivityTaskScheduled event", id)
		}
	}
	for id := range timerRows {
		if !startedTimer[id] {
			bad("timer row for event %d has no TimerStarted event", id)
		}
	}

	// Workflow task: the last WorkflowTaskScheduled needs a task row until a completion or failure follows it.
	lastSched := -1
	for i, e := range evs {
		if e.typ == wire.WorkflowTaskScheduled {
			lastSched = i
		}
	}
	if lastSched < 0 {
		bad("running run has no WorkflowTaskScheduled event")
	} else {
		settled := false
		for _, e := range evs[lastSched+1:] {
			if e.typ == wire.WorkflowTaskCompleted || e.typ == wire.WorkflowTaskFailed {
				settled = true
			}
		}
		want := evs[lastSched].id
		if settled {
			if len(wfTasks) != 0 {
				bad("workflow task row exists although the last WorkflowTaskScheduled (%d) is settled", want)
			}
		} else if len(wfTasks) != 1 || wfTasks[0].scheduled != want {
			bad("want exactly one workflow task row for scheduled event %d, have %d", want, len(wfTasks))
		}
	}
	if len(buffered) > 0 && !(len(wfTasks) == 1 && wfTasks[0].leased) {
		bad("%d buffered events but no leased workflow task", len(buffered))
	}
	return out, nil
}

type checkTask struct {
	kind      string
	scheduled int64
	leased    bool
}

func queryEvents(ctx context.Context, db *sql.DB, q string, run string) ([]checkEvent, error) {
	rows, err := db.QueryContext(ctx, q, run)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []checkEvent
	for rows.Next() {
		var e checkEvent
		var ty, attrs string
		if err := rows.Scan(&e.id, &ty, &attrs); err != nil {
			return nil, err
		}
		e.typ, e.attrs = wire.EventType(ty), json.RawMessage(attrs)
		out = append(out, e)
	}
	return out, rows.Err()
}

func queryTasks(ctx context.Context, db *sql.DB, run string) ([]checkTask, error) {
	rows, err := db.QueryContext(ctx, `SELECT kind, scheduled_event_id, lease_token IS NOT NULL FROM tasks WHERE run_id=?`, run)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []checkTask
	for rows.Next() {
		var t checkTask
		if err := rows.Scan(&t.kind, &t.scheduled, &t.leased); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func queryInt64s(ctx context.Context, db *sql.DB, q string, run string) ([]int64, error) {
	rows, err := db.QueryContext(ctx, q, run)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
