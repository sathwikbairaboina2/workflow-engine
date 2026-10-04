package chaos

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"

	"github.com/sathwikbairaboina2/workflow-engine/examples/transfer"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/client"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/worker"
)

const maxViolations = 20

var receiptRE = regexp.MustCompile(`^[0-9a-f]{32}$`)

// outcome is what the harness learned about one workflow.
type outcome struct {
	id     string
	runID  string
	status string // running | completed | failed | canceled | unknown
	result string
}

// replayAll replays every workflow's history against the current Transfer code and returns the failures.
func replayAll(ctx context.Context, c *client.Client, ids []string) (failures int, msgs []string) {
	for _, id := range ids {
		h, err := c.History(ctx, id, "")
		if err != nil {
			failures++
			msgs = append(msgs, fmt.Sprintf("%s: history: %v", id, err))
			continue
		}
		if err := worker.ReplayHistory(h, transfer.Transfer); err != nil {
			failures++
			msgs = append(msgs, fmt.Sprintf("%s: replay: %v", id, err))
		}
	}
	return failures, msgs
}

// storeChecks opens the finished database and the ledger and runs the invariant checks that need file access.
func storeChecks(ctx context.Context, workDir string, outcomes []outcome, rep *Report) ([]string, error) {
	var msgs []string
	st, err := store.Open(filepath.Join(workDir, "wf.db"))
	if err != nil {
		return nil, fmt.Errorf("open wf.db: %w", err)
	}
	defer st.Close()
	viol, err := store.CheckConsistency(ctx, st.DB)
	if err != nil {
		return nil, fmt.Errorf("consistency check: %w", err)
	}
	rep.ConsistencyViolations = len(viol)
	msgs = append(msgs, viol...)

	l, err := transfer.OpenLedger(filepath.Join(workDir, "ledger.db"))
	if err != nil {
		return nil, fmt.Errorf("open ledger: %w", err)
	}
	defer l.Close()
	lr, err := l.Check(ctx)
	if err != nil {
		return nil, fmt.Errorf("ledger check: %w", err)
	}
	rep.ActivityExecutions, rep.ActivityReExecutions = lr.Executions, lr.ReExecutions
	rep.LedgerRows, rep.DoubleApplied = lr.LedgerRows, lr.DoubleApplied

	for _, o := range outcomes {
		if o.status != "completed" {
			continue
		}
		d, c, err := l.Applied(ctx, o.runID)
		if err != nil {
			return nil, fmt.Errorf("ledger applied: %w", err)
		}
		if d < 1 || c < 1 {
			rep.MissingApplies++
			msgs = append(msgs, fmt.Sprintf("%s (run %s): completed but ledger has %d debits and %d credits", o.id, o.runID, d, c))
		}
		if !receiptRE.MatchString(o.result) {
			rep.Failed++
			msgs = append(msgs, fmt.Sprintf("%s: completed with a malformed receipt %q", o.id, o.result))
		}
	}
	return msgs, nil
}
