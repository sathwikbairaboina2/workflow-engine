// Command transfer-worker runs the transfer example's worker, and replays saved histories offline.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/examples/transfer"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/client"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/worker"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

const usage = `usage: transfer-worker <command> [flags]

commands:
  run      poll the transfers queue and execute Transfer, Debit and Credit
  replay   replay a saved history against the current Transfer code (--history FILE)
`

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "run":
		return runWorker(args[1:], stderr)
	case "replay":
		return replay(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n%s", args[0], usage)
		return 2
	}
}

func replay(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	file := fs.String("history", "", "JSON file with a workflow history (wf history --json)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *file == "" {
		fmt.Fprintln(stderr, "replay: --history is required")
		return 2
	}
	events, err := worker.ReadHistoryFile(*file)
	if err != nil {
		fmt.Fprintln(stderr, "replay:", err)
		return 1
	}
	if err := worker.ReplayHistory(events, transfer.Transfer); err != nil {
		fmt.Fprintln(stderr, "replay failed:", err)
		return 1
	}
	fmt.Fprintf(stdout, "replay ok: %d events\n", len(events))
	return 0
}

func runWorker(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	server := fs.String("server", envOr("WF_SERVER", "http://localhost:5400"), "wfd base URL")
	queue := fs.String("queue", "transfers", "task queue")
	ledgerPath := fs.String("ledger", "ledger.db", "ledger SQLite file")
	identity := fs.String("identity", "", "worker identity (default hostname:pid)")
	delay := fs.Duration("activity-delay", 30*time.Millisecond, "sleep after each ledger write, the kill window")
	unsafe := fs.Bool("unsafe-ledger", false, "ignore idempotency keys (chaos control mode)")
	wfPollers := fs.Int("workflow-pollers", 2, "concurrent workflow pollers")
	actPollers := fs.Int("activity-pollers", 4, "concurrent activity pollers")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	l, err := transfer.OpenLedger(*ledgerPath)
	if err != nil {
		fmt.Fprintln(stderr, "ledger:", err)
		return 1
	}
	defer l.Close()
	l.Unsafe, l.Delay = *unsafe, *delay

	w := worker.New(client.New(*server), *queue, worker.Options{Identity: *identity, WorkflowPollers: *wfPollers, ActivityPollers: *actPollers})
	transfer.Register(w, l)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := w.Run(ctx); err != nil {
		fmt.Fprintln(stderr, "worker:", err)
		return 1
	}
	return 0
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
