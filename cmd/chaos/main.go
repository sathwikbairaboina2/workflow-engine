// Command chaos runs the kill -9 harness: real wfd and worker processes, SIGKILLed on a seeded schedule.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/chaos"
)

func main() {
	fs := flag.NewFlagSet("chaos", flag.ExitOnError)
	var c chaos.Config
	fs.StringVar(&c.BinDir, "bin-dir", "", "directory holding the wfd and transfer-worker binaries")
	fs.StringVar(&c.WorkDir, "work-dir", "/tmp/chaos", "scratch directory (recreated); logs and databases land here")
	fs.StringVar(&c.Out, "out", "", "write the JSON report here")
	fs.Int64Var(&c.Seed, "seed", 1, "seed for inputs and the kill schedule")
	fs.IntVar(&c.Workflows, "workflows", 100, "workflows to start")
	fs.IntVar(&c.Kills, "kills", 50, "processes to kill -9")
	fs.IntVar(&c.Workers, "workers", 3, "worker processes")
	fs.DurationVar(&c.KillEvery, "kill-every", 300*time.Millisecond, "mean gap between kills")
	fs.Float64Var(&c.ServerKillShare, "server-kill-share", 0.25, "share of kills aimed at the server")
	fs.DurationVar(&c.RestartDelay, "restart-delay", 200*time.Millisecond, "pause between a kill and the restart")
	fs.DurationVar(&c.ActivityDelay, "activity-delay", 30*time.Millisecond, "worker sleep after each ledger write (the kill window)")
	fs.DurationVar(&c.DrainTimeout, "drain-timeout", 3*time.Minute, "how long to wait for workflows after the last kill")
	fs.BoolVar(&c.UnsafeLedger, "unsafe-ledger", false, "control run: disable idempotency keys")
	fs.Parse(os.Args[1:])
	if c.BinDir == "" {
		fmt.Fprintln(os.Stderr, "chaos: --bin-dir is required")
		os.Exit(2)
	}
	c.Progress = os.Stderr

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	rep, err := chaos.Run(ctx, c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "chaos:", err)
		os.Exit(1)
	}
	fmt.Print(rep.Summary())
	for _, v := range rep.Violations {
		fmt.Println("  violation:", v)
	}
	if !rep.Pass && !rep.UnsafeLedger {
		os.Exit(1)
	}
}
