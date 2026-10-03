package transfer_test

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/examples/transfer"
	"github.com/sathwikbairaboina2/workflow-engine/internal/testserver"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/client"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/worker"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func startWorker(t *testing.T, srv *testserver.Server, l *transfer.Ledger) {
	t.Helper()
	w := worker.New(client.New(srv.URL), "transfers", worker.Options{Identity: "t"})
	transfer.Register(w, l)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
}

func TestTransferEndToEnd(t *testing.T) {
	srv := testserver.Start(t)
	l, err := transfer.OpenLedger(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	startWorker(t, srv, l)

	c := client.New(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run, err := c.Start(ctx, client.StartOptions{ID: "t1", TaskQueue: "transfers"}, transfer.Transfer,
		transfer.Input{From: "acct-1", To: "acct-2", AmountCents: 1200})
	if err != nil {
		t.Fatal(err)
	}
	var receipt string
	if err := c.GetResult(ctx, "t1", &receipt); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(receipt) {
		t.Fatalf("receipt %q", receipt)
	}
	entries, _ := l.Entries(ctx, run)
	if len(entries) != 2 || entries[0] != (transfer.Entry{Kind: "debit", Account: "acct-1", AmountCents: -1200}) ||
		entries[1] != (transfer.Entry{Kind: "credit", Account: "acct-2", AmountCents: 1200}) {
		t.Fatalf("ledger %+v", entries)
	}
	h, _ := c.History(ctx, "t1", "")
	var marker, timer bool
	for _, e := range h {
		marker = marker || e.Type == wire.MarkerRecorded
		timer = timer || e.Type == wire.TimerStarted
	}
	if !marker || !timer {
		t.Fatalf("marker=%v timer=%v", marker, timer)
	}
	if err := worker.ReplayHistory(h, transfer.Transfer); err != nil {
		t.Fatalf("replay of the finished run: %v", err)
	}
}

func TestInvalidAmountFailsWorkflow(t *testing.T) {
	srv := testserver.Start(t)
	l, _ := transfer.OpenLedger(filepath.Join(t.TempDir(), "ledger.db"))
	defer l.Close()
	startWorker(t, srv, l)
	c := client.New(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c.Start(ctx, client.StartOptions{ID: "bad", TaskQueue: "transfers"}, transfer.Transfer, transfer.Input{From: "a", To: "b", AmountCents: 0})
	var out string
	err := c.GetResult(ctx, "bad", &out)
	var wfe *client.WorkflowFailedError
	if !errors.As(err, &wfe) || wfe.Failure.Type != "InvalidAmount" {
		t.Fatalf("err = %v", err)
	}
}
