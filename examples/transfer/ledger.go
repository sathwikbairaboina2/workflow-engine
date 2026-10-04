package transfer

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver

	"github.com/sathwikbairaboina2/workflow-engine/sdk/activity"
)

// ledgerDSN matches the engine store's settings: WAL, synchronous=FULL and immediate write transactions,
// so concurrent worker processes sharing one ledger file queue on the busy timeout instead of failing.
const ledgerDSN = "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_txlock=immediate"

const ledgerSchema = `
CREATE TABLE IF NOT EXISTS ledger (
  idempotency_key TEXT PRIMARY KEY,
  run_id          TEXT NOT NULL,
  kind            TEXT NOT NULL,
  account         TEXT NOT NULL,
  amount_cents    INTEGER NOT NULL,
  applied_at      INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS executions (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  idempotency_key TEXT NOT NULL,
  run_id          TEXT NOT NULL,
  kind            TEXT NOT NULL,
  attempt         INTEGER NOT NULL,
  pid             INTEGER NOT NULL,
  at              INTEGER NOT NULL
);`

// Ledger is the side-effect store of the transfer example. Every attempt of an activity writes an
// executions row; the ledger row is keyed by the activity's idempotency key, so a re-executed attempt
// changes nothing. With Unsafe the key is random, which is what a naive at-least-once activity would do.
type Ledger struct {
	// Unsafe uses a random key instead of the activity's idempotency key (the chaos control mode).
	Unsafe bool
	// Delay sleeps after the write commits and before the activity returns: it widens the window in
	// which a kill -9 re-executes an already applied activity.
	Delay time.Duration
	db    *sql.DB
}

// OpenLedger opens (creating if needed) the ledger database at path.
func OpenLedger(path string) (*Ledger, error) {
	db, err := sql.Open("sqlite", "file:"+path+ledgerDSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(ledgerSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("ledger schema: %w", err)
	}
	return &Ledger{db: db}, nil
}

// Close closes the database.
func (l *Ledger) Close() error { return l.db.Close() }

// Debit removes Leg.AmountCents from the account.
func (l *Ledger) Debit(ctx context.Context, leg Leg) (string, error) {
	return l.apply(ctx, "debit", leg.Account, -leg.AmountCents, leg.AmountCents)
}

// Credit adds Leg.AmountCents to the account.
func (l *Ledger) Credit(ctx context.Context, leg Leg) (string, error) {
	return l.apply(ctx, "credit", leg.Account, leg.AmountCents, leg.AmountCents)
}

func (l *Ledger) apply(ctx context.Context, kind, account string, signed, requested int64) (string, error) {
	if requested <= 0 {
		return "", activity.NewNonRetryableError("InvalidAmount", fmt.Sprintf("amount must be positive, got %d", requested))
	}
	info := activity.GetInfo(ctx)
	key := info.IdempotencyKey
	if l.Unsafe || key == "" {
		var b [12]byte
		_, _ = rand.Read(b[:])
		key = hex.EncodeToString(b[:])
	}
	now := time.Now().UnixMilli()
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO executions(idempotency_key, run_id, kind, attempt, pid, at) VALUES(?,?,?,?,?,?)`,
		key, info.RunID, kind, info.Attempt, os.Getpid(), now); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO ledger(idempotency_key, run_id, kind, account, amount_cents, applied_at) VALUES(?,?,?,?,?,?)
		ON CONFLICT(idempotency_key) DO NOTHING`, key, info.RunID, kind, account, signed, now); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	if l.Delay > 0 {
		select {
		case <-time.After(l.Delay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return key, nil
}

// Report summarises the ledger for the chaos checks.
type Report struct {
	LedgerRows, Executions, ReExecutions, DoubleApplied int
}

// Check counts re-executions (attempts beyond the first per run and kind) and double-applied effects
// (more than one ledger row for the same run and kind).
func (l *Ledger) Check(ctx context.Context) (Report, error) {
	var r Report
	if err := l.db.QueryRowContext(ctx, `SELECT count(*) FROM ledger`).Scan(&r.LedgerRows); err != nil {
		return r, err
	}
	if err := l.db.QueryRowContext(ctx, `SELECT count(*) FROM executions`).Scan(&r.Executions); err != nil {
		return r, err
	}
	var distinct int
	if err := l.db.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT DISTINCT run_id, kind FROM executions)`).Scan(&distinct); err != nil {
		return r, err
	}
	r.ReExecutions = r.Executions - distinct
	if err := l.db.QueryRowContext(ctx, `SELECT coalesce(sum(n-1), 0) FROM (SELECT count(*) AS n FROM ledger GROUP BY run_id, kind HAVING n > 1)`).Scan(&r.DoubleApplied); err != nil {
		return r, err
	}
	return r, nil
}

// Applied returns how many debit and credit rows the run has in the ledger.
func (l *Ledger) Applied(ctx context.Context, runID string) (debits, credits int, err error) {
	rows, err := l.db.QueryContext(ctx, `SELECT kind, count(*) FROM ledger WHERE run_id=? GROUP BY kind`, runID)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		var n int
		if err := rows.Scan(&kind, &n); err != nil {
			return 0, 0, err
		}
		switch kind {
		case "debit":
			debits = n
		case "credit":
			credits = n
		}
	}
	return debits, credits, rows.Err()
}

// Entry is one applied ledger row.
type Entry struct {
	Kind, Account string
	AmountCents   int64
}

// Entries lists the ledger rows of a run in application order.
func (l *Ledger) Entries(ctx context.Context, runID string) ([]Entry, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT kind, account, amount_cents FROM ledger WHERE run_id=? ORDER BY applied_at, kind`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.Kind, &e.Account, &e.AmountCents); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
