package store

import (
	"context"
	"strings"
	"testing"
)

type evSpec struct{ typ, attrs string }

// buildRun writes a run directly with raw SQL so tests can build both consistent and corrupt databases.
func buildRun(t *testing.T, s *Store, run, status string, next int64, evs []evSpec) {
	t.Helper()
	if _, err := s.DB.Exec(`INSERT INTO workflows(run_id,workflow_id,workflow_type,task_queue,status,next_event_id,created_at) VALUES(?,?,?,?,?,?,0)`,
		run, "wf-"+run, "W", "q", status, next); err != nil {
		t.Fatal(err)
	}
	for i, e := range evs {
		if _, err := s.DB.Exec(`INSERT INTO history_events(run_id,event_id,event_type,attrs,ts) VALUES(?,?,?,?,0)`, run, i+1, e.typ, e.attrs); err != nil {
			t.Fatal(err)
		}
	}
}

func exec(t *testing.T, s *Store, q string, args ...any) {
	t.Helper()
	if _, err := s.DB.Exec(q, args...); err != nil {
		t.Fatal(err)
	}
}

// baseEvents: started, scheduled, wft started, wft completed, activity scheduled (id 5).
func baseEvents() []evSpec {
	return []evSpec{
		{"WorkflowExecutionStarted", `{}`}, {"WorkflowTaskScheduled", `{"attempt":1}`}, {"WorkflowTaskStarted", `{}`},
		{"WorkflowTaskCompleted", `{}`}, {"ActivityTaskScheduled", `{"seq":1}`},
	}
}

func consistentDB(t *testing.T) *Store {
	s := openTemp(t)
	buildRun(t, s, "r1", "running", 6, baseEvents())
	exec(t, s, `INSERT INTO tasks(queue,kind,run_id,scheduled_event_id,timeout_ms,visible_at) VALUES('q','activity','r1',5,1000,0)`)
	return s
}

func violations(t *testing.T, s *Store) []string {
	t.Helper()
	v, err := CheckConsistency(context.Background(), s.DB)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestCheckConsistentDB(t *testing.T) {
	s := consistentDB(t)
	if v := violations(t, s); len(v) != 0 {
		t.Fatalf("violations on a consistent db: %v", v)
	}
}

func TestCheckDetectsViolations(t *testing.T) {
	cases := map[string]func(t *testing.T, s *Store){
		"gap in event ids": func(t *testing.T, s *Store) {
			exec(t, s, `UPDATE workflows SET next_event_id=8 WHERE run_id='r1'`)
		},
		"first event not started": func(t *testing.T, s *Store) {
			exec(t, s, `DROP TRIGGER history_no_update`) // simulate corruption the trigger would normally block
			exec(t, s, `UPDATE history_events SET event_type='MarkerRecorded' WHERE run_id='r1' AND event_id=1`)
		},
		"closed run with task": func(t *testing.T, s *Store) {
			exec(t, s, `UPDATE workflows SET status='completed' WHERE run_id='r1'`)
		},
		"activity without terminal and without task": func(t *testing.T, s *Store) {
			exec(t, s, `DELETE FROM tasks WHERE run_id='r1'`)
		},
		"terminal activity event with task row": func(t *testing.T, s *Store) {
			exec(t, s, `INSERT INTO history_events(run_id,event_id,event_type,attrs,ts) VALUES('r1',6,'ActivityTaskCompleted','{"scheduled_event_id":5}',0)`)
			exec(t, s, `INSERT INTO history_events(run_id,event_id,event_type,attrs,ts) VALUES('r1',7,'WorkflowTaskScheduled','{"attempt":1}',0)`)
			exec(t, s, `INSERT INTO tasks(queue,kind,run_id,scheduled_event_id,timeout_ms,visible_at) VALUES('q','workflow','r1',7,1000,0)`)
			exec(t, s, `UPDATE workflows SET next_event_id=8 WHERE run_id='r1'`)
		},
		"timer without fired and without row": func(t *testing.T, s *Store) {
			exec(t, s, `INSERT INTO history_events(run_id,event_id,event_type,attrs,ts) VALUES('r1',6,'TimerStarted','{"seq":2}',0)`)
			exec(t, s, `UPDATE workflows SET next_event_id=7 WHERE run_id='r1'`)
		},
		"buffered events without leased workflow task": func(t *testing.T, s *Store) {
			exec(t, s, `INSERT INTO buffered_events(run_id,event_type,attrs,ts) VALUES('r1','WorkflowExecutionSignaled','{}',0)`)
		},
		"unsettled workflow task without row": func(t *testing.T, s *Store) {
			exec(t, s, `INSERT INTO history_events(run_id,event_id,event_type,attrs,ts) VALUES('r1',6,'WorkflowTaskScheduled','{"attempt":1}',0)`)
			exec(t, s, `UPDATE workflows SET next_event_id=7 WHERE run_id='r1'`)
		},
		"orphan timer row": func(t *testing.T, s *Store) {
			exec(t, s, `INSERT INTO timers(run_id,started_event_id,fire_at) VALUES('r1',3,0)`)
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			s := consistentDB(t)
			corrupt(t, s)
			v := violations(t, s)
			if len(v) == 0 {
				t.Fatal("corruption not detected")
			}
			for _, line := range v {
				if !strings.Contains(line, "r1") {
					t.Fatalf("violation does not name the run: %q", line)
				}
			}
		})
	}
}
