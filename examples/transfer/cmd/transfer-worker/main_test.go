package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/examples/transfer"
	"github.com/sathwikbairaboina2/workflow-engine/internal/testserver"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/client"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/worker"
)

func TestTransferReplayCommand(t *testing.T) {
	srv := testserver.Start(t)
	l, err := transfer.OpenLedger(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c := client.New(srv.URL)
	w := worker.New(c, "transfers", worker.Options{Identity: "t"})
	transfer.Register(w, l)
	wctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(wctx) }()
	defer func() { stop(); <-done }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := c.Start(ctx, client.StartOptions{ID: "r1", TaskQueue: "transfers"}, transfer.Transfer,
		transfer.Input{From: "a", To: "b", AmountCents: 5}); err != nil {
		t.Fatal(err)
	}
	var receipt string
	if err := c.GetResult(ctx, "r1", &receipt); err != nil {
		t.Fatal(err)
	}
	h, err := c.History(ctx, "r1", "")
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "h.json")
	b, _ := json.Marshal(h)
	os.WriteFile(file, b, 0o644)

	var out, errOut bytes.Buffer
	if code := run([]string{"replay", "--history", file}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	if !regexp.MustCompile(`^replay ok: \d+ events\n$`).MatchString(out.String()) {
		t.Fatalf("output %q", out.String())
	}
}

func TestReplayCommandRejectsBrokenHistory(t *testing.T) {
	file := filepath.Join(t.TempDir(), "h.json")
	os.WriteFile(file, []byte(`not json`), 0o644)
	var out, errOut bytes.Buffer
	if code := run([]string{"replay", "--history", file}, &out, &errOut); code != 1 {
		t.Fatalf("exit %d", code)
	}
	if code := run([]string{"replay"}, &out, &errOut); code != 2 {
		t.Fatalf("missing flag exit %d", code)
	}
	if code := run(nil, &out, &errOut); code != 2 {
		t.Fatalf("no args exit %d", code)
	}
}
