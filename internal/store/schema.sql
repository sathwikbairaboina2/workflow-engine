CREATE TABLE workflows (
  run_id        TEXT PRIMARY KEY,
  workflow_id   TEXT NOT NULL,
  workflow_type TEXT NOT NULL,
  task_queue    TEXT NOT NULL,
  status        TEXT NOT NULL CHECK (status IN ('running','completed','failed','canceled')),
  next_event_id INTEGER NOT NULL,
  input         TEXT,
  result        TEXT,
  failure       TEXT,
  created_at    INTEGER NOT NULL,
  closed_at     INTEGER
);
CREATE UNIQUE INDEX workflows_one_open ON workflows(workflow_id) WHERE status = 'running';
CREATE INDEX workflows_by_id ON workflows(workflow_id, created_at);

CREATE TABLE history_events (
  run_id     TEXT NOT NULL,
  event_id   INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  attrs      TEXT NOT NULL,
  ts         INTEGER NOT NULL,
  PRIMARY KEY (run_id, event_id)
) WITHOUT ROWID;
CREATE TRIGGER history_no_update BEFORE UPDATE ON history_events
BEGIN SELECT RAISE(ABORT, 'history is append-only'); END;
CREATE TRIGGER history_no_delete BEFORE DELETE ON history_events
BEGIN SELECT RAISE(ABORT, 'history is append-only'); END;

CREATE TABLE tasks (
  task_id            INTEGER PRIMARY KEY,
  queue              TEXT NOT NULL,
  kind               TEXT NOT NULL CHECK (kind IN ('workflow','activity')),
  run_id             TEXT NOT NULL,
  scheduled_event_id INTEGER NOT NULL,
  started_event_id   INTEGER,
  attempt            INTEGER NOT NULL DEFAULT 1,
  timeout_ms         INTEGER NOT NULL,
  visible_at         INTEGER NOT NULL,
  lease_token        TEXT,
  lease_expires_at   INTEGER,
  UNIQUE (run_id, kind, scheduled_event_id)
);
CREATE INDEX tasks_poll ON tasks(queue, kind, visible_at);
CREATE UNIQUE INDEX tasks_lease ON tasks(lease_token) WHERE lease_token IS NOT NULL;
CREATE INDEX tasks_expiry ON tasks(lease_expires_at) WHERE lease_expires_at IS NOT NULL;
CREATE UNIQUE INDEX tasks_one_workflow_task ON tasks(run_id) WHERE kind = 'workflow';

CREATE TABLE timers (
  run_id           TEXT NOT NULL,
  started_event_id INTEGER NOT NULL,
  fire_at          INTEGER NOT NULL,
  PRIMARY KEY (run_id, started_event_id)
);
CREATE INDEX timers_due ON timers(fire_at);

CREATE TABLE buffered_events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id     TEXT NOT NULL,
  event_type TEXT NOT NULL,
  attrs      TEXT NOT NULL,
  ts         INTEGER NOT NULL
);
CREATE INDEX buffered_by_run ON buffered_events(run_id, seq);
