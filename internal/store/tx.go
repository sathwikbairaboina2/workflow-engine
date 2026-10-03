package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// Workflow is one run's row.
type Workflow struct {
	RunID, WorkflowID, WorkflowType, TaskQueue, Status string
	NextEventID                                        int64
	Input, Result                                      *wire.Payload
	Failure                                            *wire.Failure
	CreatedAt, ClosedAt                                int64
}

// NewEvent is an event to append; its id is assigned by AppendEvents.
type NewEvent struct {
	Type  wire.EventType
	Attrs any // marshalled to JSON
	Time  int64
}

// Task is a queued workflow or activity task.
type Task struct {
	TaskID               int64
	Queue, Kind, RunID   string // Kind: "workflow" | "activity"
	ScheduledEventID     int64
	StartedEventID       int64 // workflow tasks only, 0 if not started
	Attempt              int
	TimeoutMS, VisibleAt int64
	LeaseToken           string // "" when not leased
	LeaseExpiresAt       int64
}

// Timer is a durable timer row.
type Timer struct {
	RunID          string
	StartedEventID int64
	FireAt         int64
}

const taskCols = `task_id, queue, kind, run_id, scheduled_event_id, started_event_id, attempt, timeout_ms, visible_at, lease_token, lease_expires_at`

type scanner interface{ Scan(dest ...any) error }

func scanTask(r scanner) (Task, error) {
	var t Task
	var started, expires sql.NullInt64
	var token sql.NullString
	if err := r.Scan(&t.TaskID, &t.Queue, &t.Kind, &t.RunID, &t.ScheduledEventID, &started, &t.Attempt,
		&t.TimeoutMS, &t.VisibleAt, &token, &expires); err != nil {
		return Task{}, err
	}
	t.StartedEventID = started.Int64
	t.LeaseToken = token.String
	t.LeaseExpiresAt = expires.Int64
	return t, nil
}

func nullable(v any) (any, error) {
	switch x := v.(type) {
	case *wire.Payload:
		if x == nil {
			return nil, nil
		}
	case *wire.Failure:
		if x == nil {
			return nil, nil
		}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// InsertWorkflow inserts a run row; ErrDuplicateOpenRun when the workflow id already has a running run.
func (t *Tx) InsertWorkflow(w Workflow) error {
	in, err := nullable(w.Input)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(`INSERT INTO workflows(run_id,workflow_id,workflow_type,task_queue,status,next_event_id,input,created_at)
		VALUES(?,?,?,?,?,?,?,?)`, w.RunID, w.WorkflowID, w.WorkflowType, w.TaskQueue, w.Status, w.NextEventID, in, w.CreatedAt)
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "UNIQUE constraint failed") &&
			(strings.Contains(msg, "workflows.workflow_id") || strings.Contains(msg, "workflows_one_open")) {
			return ErrDuplicateOpenRun
		}
		return fmt.Errorf("store: insert workflow: %w", err)
	}
	return nil
}

const wfCols = `run_id, workflow_id, workflow_type, task_queue, status, next_event_id, input, result, failure, created_at, closed_at`

func scanWorkflow(r scanner) (Workflow, error) {
	var w Workflow
	var in, res, fail sql.NullString
	var closed sql.NullInt64
	if err := r.Scan(&w.RunID, &w.WorkflowID, &w.WorkflowType, &w.TaskQueue, &w.Status, &w.NextEventID,
		&in, &res, &fail, &w.CreatedAt, &closed); err != nil {
		return Workflow{}, err
	}
	w.ClosedAt = closed.Int64
	if in.Valid {
		w.Input = new(wire.Payload)
		if err := json.Unmarshal([]byte(in.String), w.Input); err != nil {
			return Workflow{}, err
		}
	}
	if res.Valid {
		w.Result = new(wire.Payload)
		if err := json.Unmarshal([]byte(res.String), w.Result); err != nil {
			return Workflow{}, err
		}
	}
	if fail.Valid {
		w.Failure = new(wire.Failure)
		if err := json.Unmarshal([]byte(fail.String), w.Failure); err != nil {
			return Workflow{}, err
		}
	}
	return w, nil
}

func (t *Tx) getWorkflow(q string, args ...any) (Workflow, bool, error) {
	w, err := scanWorkflow(t.tx.QueryRow(q, args...))
	if err == sql.ErrNoRows {
		return Workflow{}, false, nil
	}
	if err != nil {
		return Workflow{}, false, fmt.Errorf("store: get workflow: %w", err)
	}
	return w, true, nil
}

// GetRun returns a run by id.
func (t *Tx) GetRun(runID string) (Workflow, bool, error) {
	return t.getWorkflow(`SELECT `+wfCols+` FROM workflows WHERE run_id=?`, runID)
}

// GetOpenRun returns the running run of a workflow id.
func (t *Tx) GetOpenRun(workflowID string) (Workflow, bool, error) {
	return t.getWorkflow(`SELECT `+wfCols+` FROM workflows WHERE workflow_id=? AND status='running'`, workflowID)
}

// GetLatestRun returns the newest run of a workflow id.
func (t *Tx) GetLatestRun(workflowID string) (Workflow, bool, error) {
	return t.getWorkflow(`SELECT `+wfCols+` FROM workflows WHERE workflow_id=? ORDER BY created_at DESC, rowid DESC LIMIT 1`, workflowID)
}

// CloseRun marks a run closed with its outcome.
func (t *Tx) CloseRun(runID, status string, result *wire.Payload, failure *wire.Failure, closedAt int64) error {
	res, err := nullable(result)
	if err != nil {
		return err
	}
	fail, err := nullable(failure)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(`UPDATE workflows SET status=?, result=?, failure=?, closed_at=? WHERE run_id=?`, status, res, fail, closedAt, runID)
	if err != nil {
		return fmt.Errorf("store: close run: %w", err)
	}
	return nil
}

// AppendEvents assigns ids expectedNext, expectedNext+1, ... It first advances next_event_id with a
// compare-and-set and returns ErrConflict if another writer got there first.
func (t *Tx) AppendEvents(runID string, expectedNext int64, evs []NewEvent) (int64, error) {
	res, err := t.tx.Exec(`UPDATE workflows SET next_event_id = ? WHERE run_id=? AND next_event_id=?`,
		expectedNext+int64(len(evs)), runID, expectedNext)
	if err != nil {
		return 0, fmt.Errorf("store: advance next_event_id: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return 0, ErrConflict
	}
	for i, e := range evs {
		b, err := json.Marshal(e.Attrs)
		if err != nil {
			return 0, fmt.Errorf("store: marshal %s attrs: %w", e.Type, err)
		}
		if _, err := t.tx.Exec(`INSERT INTO history_events(run_id,event_id,event_type,attrs,ts) VALUES(?,?,?,?,?)`,
			runID, expectedNext+int64(i), string(e.Type), string(b), e.Time); err != nil {
			return 0, fmt.Errorf("store: insert event: %w", err)
		}
	}
	return expectedNext, nil
}

// LoadHistory returns up to limit events with id >= fromID.
func (t *Tx) LoadHistory(runID string, fromID int64, limit int) ([]wire.Event, error) {
	rows, err := t.tx.Query(`SELECT event_id, event_type, attrs, ts FROM history_events WHERE run_id=? AND event_id>=? ORDER BY event_id LIMIT ?`,
		runID, fromID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: load history: %w", err)
	}
	defer rows.Close()
	var out []wire.Event
	for rows.Next() {
		var e wire.Event
		var ty, attrs string
		if err := rows.Scan(&e.EventID, &ty, &attrs, &e.Time); err != nil {
			return nil, err
		}
		e.Type = wire.EventType(ty)
		e.Attrs = json.RawMessage(attrs)
		out = append(out, e)
	}
	return out, rows.Err()
}

// GetEvent returns one event.
func (t *Tx) GetEvent(runID string, eventID int64) (wire.Event, bool, error) {
	var e wire.Event
	var ty, attrs string
	err := t.tx.QueryRow(`SELECT event_id, event_type, attrs, ts FROM history_events WHERE run_id=? AND event_id=?`, runID, eventID).
		Scan(&e.EventID, &ty, &attrs, &e.Time)
	if err == sql.ErrNoRows {
		return wire.Event{}, false, nil
	}
	if err != nil {
		return wire.Event{}, false, err
	}
	e.Type = wire.EventType(ty)
	e.Attrs = json.RawMessage(attrs)
	return e, true, nil
}

// InsertTask queues a task and returns its id.
func (t *Tx) InsertTask(task Task) (int64, error) {
	res, err := t.tx.Exec(`INSERT INTO tasks(queue,kind,run_id,scheduled_event_id,attempt,timeout_ms,visible_at) VALUES(?,?,?,?,?,?,?)`,
		task.Queue, task.Kind, task.RunID, task.ScheduledEventID, task.Attempt, task.TimeoutMS, task.VisibleAt)
	if err != nil {
		return 0, fmt.Errorf("store: insert task: %w", err)
	}
	return res.LastInsertId()
}

// LeaseNextTask leases the oldest visible, unleased task of the queue and kind.
func (t *Tx) LeaseNextTask(queue, kind string, now int64, token string) (Task, bool, error) {
	row := t.tx.QueryRow(`UPDATE tasks SET lease_token=?, lease_expires_at=?+timeout_ms
		WHERE task_id = (SELECT task_id FROM tasks WHERE queue=? AND kind=? AND visible_at<=? AND lease_token IS NULL
		                 ORDER BY visible_at, task_id LIMIT 1)
		RETURNING `+taskCols, token, now, queue, kind, now)
	task, err := scanTask(row)
	if err == sql.ErrNoRows {
		return Task{}, false, nil
	}
	if err != nil {
		return Task{}, false, fmt.Errorf("store: lease task: %w", err)
	}
	return task, true, nil
}

// SetTaskStarted records the WorkflowTaskStarted event id on a task.
func (t *Tx) SetTaskStarted(taskID, startedEventID int64) error {
	_, err := t.tx.Exec(`UPDATE tasks SET started_event_id=? WHERE task_id=?`, startedEventID, taskID)
	return err
}

func (t *Tx) oneTask(q string, args ...any) (Task, bool, error) {
	task, err := scanTask(t.tx.QueryRow(q, args...))
	if err == sql.ErrNoRows {
		return Task{}, false, nil
	}
	if err != nil {
		return Task{}, false, fmt.Errorf("store: get task: %w", err)
	}
	return task, true, nil
}

// TaskByLease finds the task holding a lease token.
func (t *Tx) TaskByLease(token string) (Task, bool, error) {
	if token == "" {
		return Task{}, false, nil
	}
	return t.oneTask(`SELECT `+taskCols+` FROM tasks WHERE lease_token=?`, token)
}

// WorkflowTaskForRun returns the run's workflow task, if any.
func (t *Tx) WorkflowTaskForRun(runID string) (Task, bool, error) {
	return t.oneTask(`SELECT `+taskCols+` FROM tasks WHERE run_id=? AND kind='workflow'`, runID)
}

// DeleteTask removes a task.
func (t *Tx) DeleteTask(taskID int64) error {
	_, err := t.tx.Exec(`DELETE FROM tasks WHERE task_id=?`, taskID)
	return err
}

// RetryTask resets a task to unleased with a new attempt and visibility time.
func (t *Tx) RetryTask(taskID int64, attempt int, visibleAt int64) error {
	_, err := t.tx.Exec(`UPDATE tasks SET attempt=?, visible_at=?, lease_token=NULL, lease_expires_at=NULL, started_event_id=NULL WHERE task_id=?`,
		attempt, visibleAt, taskID)
	return err
}

func (t *Tx) manyTasks(q string, args ...any) ([]Task, error) {
	rows, err := t.tx.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: query tasks: %w", err)
	}
	defer rows.Close()
	var out []Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	return out, rows.Err()
}

// ExpiredTasks lists leased tasks whose lease has expired.
func (t *Tx) ExpiredTasks(now int64, limit int) ([]Task, error) {
	return t.manyTasks(`SELECT `+taskCols+` FROM tasks WHERE lease_expires_at IS NOT NULL AND lease_expires_at<=? ORDER BY lease_expires_at LIMIT ?`, now, limit)
}

// PendingActivityTasks lists a run's activity tasks.
func (t *Tx) PendingActivityTasks(runID string) ([]Task, error) {
	return t.manyTasks(`SELECT `+taskCols+` FROM tasks WHERE run_id=? AND kind='activity' ORDER BY scheduled_event_id`, runID)
}

// DeleteRunTasksTimersBuffer removes every pending row of a run.
func (t *Tx) DeleteRunTasksTimersBuffer(runID string) error {
	for _, q := range []string{
		`DELETE FROM tasks WHERE run_id=?`,
		`DELETE FROM timers WHERE run_id=?`,
		`DELETE FROM buffered_events WHERE run_id=?`,
	} {
		if _, err := t.tx.Exec(q, runID); err != nil {
			return fmt.Errorf("store: cleanup run: %w", err)
		}
	}
	return nil
}

// InsertTimer adds a durable timer.
func (t *Tx) InsertTimer(tm Timer) error {
	_, err := t.tx.Exec(`INSERT INTO timers(run_id,started_event_id,fire_at) VALUES(?,?,?)`, tm.RunID, tm.StartedEventID, tm.FireAt)
	return err
}

// DeleteTimer removes a timer and reports whether it existed.
func (t *Tx) DeleteTimer(runID string, startedEventID int64) (bool, error) {
	res, err := t.tx.Exec(`DELETE FROM timers WHERE run_id=? AND started_event_id=?`, runID, startedEventID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (t *Tx) timers(q string, args ...any) ([]Timer, error) {
	rows, err := t.tx.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: query timers: %w", err)
	}
	defer rows.Close()
	var out []Timer
	for rows.Next() {
		var tm Timer
		if err := rows.Scan(&tm.RunID, &tm.StartedEventID, &tm.FireAt); err != nil {
			return nil, err
		}
		out = append(out, tm)
	}
	return out, rows.Err()
}

// DueTimers lists timers due at or before now.
func (t *Tx) DueTimers(now int64, limit int) ([]Timer, error) {
	return t.timers(`SELECT run_id, started_event_id, fire_at FROM timers WHERE fire_at<=? ORDER BY fire_at, run_id, started_event_id LIMIT ?`, now, limit)
}

// TimersForRun lists a run's pending timers.
func (t *Tx) TimersForRun(runID string) ([]Timer, error) {
	return t.timers(`SELECT run_id, started_event_id, fire_at FROM timers WHERE run_id=? ORDER BY started_event_id`, runID)
}

// BufferEvent stores an event that arrived while a workflow task was in flight.
func (t *Tx) BufferEvent(runID string, ev NewEvent) error {
	b, err := json.Marshal(ev.Attrs)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(`INSERT INTO buffered_events(run_id,event_type,attrs,ts) VALUES(?,?,?,?)`, runID, string(ev.Type), string(b), ev.Time)
	return err
}

// TakeBufferedEvents returns and deletes a run's buffered events in arrival order.
// Attrs come back as json.RawMessage, so re-appending them is a no-op marshal.
func (t *Tx) TakeBufferedEvents(runID string) ([]NewEvent, error) {
	rows, err := t.tx.Query(`SELECT event_type, attrs, ts FROM buffered_events WHERE run_id=? ORDER BY seq`, runID)
	if err != nil {
		return nil, fmt.Errorf("store: read buffer: %w", err)
	}
	var out []NewEvent
	for rows.Next() {
		var ty, attrs string
		var ts int64
		if err := rows.Scan(&ty, &attrs, &ts); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, NewEvent{Type: wire.EventType(ty), Attrs: json.RawMessage(attrs), Time: ts})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) > 0 {
		if _, err := t.tx.Exec(`DELETE FROM buffered_events WHERE run_id=?`, runID); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// CountRuns counts runs with the status ("" = all).
func (t *Tx) CountRuns(status string) (int, error) {
	var n int
	var err error
	if status == "" {
		err = t.tx.QueryRow(`SELECT count(*) FROM workflows`).Scan(&n)
	} else {
		err = t.tx.QueryRow(`SELECT count(*) FROM workflows WHERE status=?`, status).Scan(&n)
	}
	return n, err
}
