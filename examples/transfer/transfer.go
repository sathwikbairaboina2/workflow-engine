// Package transfer is the example workflow: debit one account, wait on a durable timer, credit another.
// Its activities write to an idempotent ledger, which is what makes the chaos soak lose and double-apply nothing.
package transfer

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/sdk/worker"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/workflow"
)

// Input describes one transfer.
type Input struct {
	From        string `json:"from"`
	To          string `json:"to"`
	AmountCents int64  `json:"amount_cents"`
}

// Leg is one side of a transfer, the input of the Debit and Credit activities.
type Leg struct {
	Account     string `json:"account"`
	AmountCents int64  `json:"amount_cents"`
}

var activityOptions = workflow.ActivityOptions{
	StartToClose: 2 * time.Second,
	Retry: workflow.RetryPolicy{Initial: 100 * time.Millisecond, Backoff: 2, Max: time.Second,
		NonRetryable: []string{"InvalidAmount"}},
}

// Transfer debits From, waits 200ms on a durable timer, credits To, and returns a receipt id that is
// generated once through SideEffect, so every replay returns the same receipt.
func Transfer(ctx workflow.Context, in Input) (string, error) {
	if err := workflow.ExecuteActivity(ctx, activityOptions, "Debit", Leg{Account: in.From, AmountCents: in.AmountCents}).Get(ctx, nil); err != nil {
		return "", err
	}
	if err := workflow.Sleep(ctx, 200*time.Millisecond); err != nil {
		return "", err
	}
	if err := workflow.ExecuteActivity(ctx, activityOptions, "Credit", Leg{Account: in.To, AmountCents: in.AmountCents}).Get(ctx, nil); err != nil {
		return "", err
	}
	var receipt string
	if err := workflow.SideEffect(ctx, func() any {
		var b [16]byte
		_, _ = rand.Read(b[:])
		return hex.EncodeToString(b[:])
	}).Get(&receipt); err != nil {
		return "", err
	}
	return receipt, nil
}

// Register registers the Transfer workflow and the ledger's Debit and Credit activities on w.
func Register(w *worker.Worker, l *Ledger) {
	w.RegisterWorkflow(Transfer)
	w.RegisterActivityWithName("Debit", l.Debit)
	w.RegisterActivityWithName("Credit", l.Credit)
}
