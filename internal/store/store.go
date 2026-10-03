// Package store is the SQLite persistence layer: schema, transactions and row operations.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

//go:embed schema.sql
var schemaSQL string

// dsnParams are appended to the database path. _txlock=immediate makes every BeginTx a BEGIN IMMEDIATE,
// so writers queue on busy_timeout instead of failing when a read lock upgrades (verified by prototype, ADR 0001).
const dsnParams = "?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(10000)&_pragma=foreign_keys(ON)&_txlock=immediate"

// ErrConflict means an optimistic concurrency check failed.
var ErrConflict = errors.New("store: concurrent modification")

// ErrDuplicateOpenRun means the workflow id already has a running run.
var ErrDuplicateOpenRun = errors.New("store: workflow already has an open run")

// Failpoint, when non-nil, is called with "<tx name>/before_commit" and "<tx name>/after_commit".
// Tests set it to panic; production leaves it nil.
var Failpoint func(point string)

// Store wraps the SQLite database.
type Store struct{ DB *sql.DB }

// Open opens (and migrates) the database at path.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+dsnParams)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	db.SetMaxOpenConns(8)
	s := &Store{DB: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.DB.Close() }

func (s *Store) migrate() error {
	ctx := context.Background()
	return s.WithTx(ctx, "migrate", func(t *Tx) error {
		var v int
		if err := t.tx.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
			return err
		}
		if v >= 1 {
			return nil
		}
		if _, err := t.tx.Exec(schemaSQL); err != nil {
			return fmt.Errorf("store: apply schema: %w", err)
		}
		_, err := t.tx.Exec(`PRAGMA user_version = 1`)
		return err
	})
}

// Tx is a handle to one open transaction.
type Tx struct{ tx *sql.Tx }

// WithTx runs fn in one BEGIN IMMEDIATE transaction named name. It rolls back on error or panic
// (the panic is re-raised) and commits otherwise.
func (s *Store) WithTx(ctx context.Context, name string, fn func(*Tx) error) (err error) {
	sqlTx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin %s: %w", name, err)
	}
	committed := false
	defer func() {
		if !committed {
			sqlTx.Rollback()
		}
	}()
	if err := fn(&Tx{tx: sqlTx}); err != nil {
		return err
	}
	if Failpoint != nil {
		Failpoint(name + "/before_commit")
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("store: commit %s: %w", name, err)
	}
	committed = true
	if Failpoint != nil {
		Failpoint(name + "/after_commit")
	}
	return nil
}
