package transfer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/sathwikbairaboina2/workflow-engine/sdk/activity"
)

func openLedger(t *testing.T) *Ledger {
	t.Helper()
	l, err := OpenLedger(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func actCtx(run string, event int64, attempt int) context.Context {
	return activity.WithInfo(context.Background(), activity.Info{RunID: run, ScheduledEventID: event, Attempt: attempt, IdempotencyKey: activity.Key(run, event)})
}

func TestApplyIsIdempotent(t *testing.T) {
	l := openLedger(t)
	leg := Leg{Account: "acct-1", AmountCents: 500}
	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := l.Debit(actCtx("r1", 5, attempt), leg); err != nil {
			t.Fatal(err)
		}
	}
	r, err := l.Check(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.LedgerRows != 1 || r.Executions != 2 || r.ReExecutions != 1 || r.DoubleApplied != 0 {
		t.Fatalf("report %+v", r)
	}
}

func TestUnsafeLedgerDoubleApplies(t *testing.T) {
	l := openLedger(t)
	l.Unsafe = true
	leg := Leg{Account: "acct-1", AmountCents: 500}
	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := l.Debit(actCtx("r1", 5, attempt), leg); err != nil {
			t.Fatal(err)
		}
	}
	r, _ := l.Check(context.Background())
	if r.LedgerRows != 2 || r.DoubleApplied != 1 || r.ReExecutions != 1 {
		t.Fatalf("report %+v", r)
	}
}

func TestAppliedPerRun(t *testing.T) {
	l := openLedger(t)
	if _, err := l.Debit(actCtx("r1", 5, 1), Leg{Account: "a", AmountCents: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Credit(actCtx("r1", 9, 1), Leg{Account: "b", AmountCents: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Debit(actCtx("r2", 5, 1), Leg{Account: "a", AmountCents: 10}); err != nil {
		t.Fatal(err)
	}
	d, c, err := l.Applied(context.Background(), "r1")
	if err != nil || d != 1 || c != 1 {
		t.Fatalf("applied %d %d %v", d, c, err)
	}
	es, _ := l.Entries(context.Background(), "r1")
	if len(es) != 2 || es[0].AmountCents != -10 || es[1].AmountCents != 10 {
		t.Fatalf("entries %+v", es)
	}
}

func TestInvalidAmountIsNonRetryable(t *testing.T) {
	l := openLedger(t)
	_, err := l.Debit(actCtx("r1", 5, 1), Leg{Account: "a", AmountCents: 0})
	var ae *activity.Error
	if !errors.As(err, &ae) || ae.Type != "InvalidAmount" || !ae.NonRetryable {
		t.Fatalf("err = %v", err)
	}
	if r, _ := l.Check(context.Background()); r.Executions != 0 {
		t.Fatalf("invalid amount wrote rows: %+v", r)
	}
}
