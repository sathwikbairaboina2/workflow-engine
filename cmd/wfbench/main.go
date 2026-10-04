// Command wfbench measures the engine: workflows per second and per-transition commit latency.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/sathwikbairaboina2/workflow-engine/internal/bench"
)

func main() {
	var c bench.Config
	var out string
	fs := flag.NewFlagSet("wfbench", flag.ExitOnError)
	fs.IntVar(&c.Workflows, "workflows", 2000, "workflows to run")
	fs.IntVar(&c.Concurrency, "concurrency", 64, "concurrent starters")
	fs.IntVar(&c.WorkflowPollers, "workflow-pollers", 4, "workflow pollers")
	fs.IntVar(&c.ActivityPollers, "activity-pollers", 16, "activity pollers")
	fs.StringVar(&c.DBPath, "db", "/tmp/wfbench/wf.db", "SQLite file (recreated)")
	fs.StringVar(&out, "out", "", "write the JSON result here")
	fs.Parse(os.Args[1:])

	if err := os.RemoveAll(filepath.Dir(c.DBPath)); err != nil {
		fmt.Fprintln(os.Stderr, "wfbench:", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(filepath.Dir(c.DBPath), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "wfbench:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	res, err := bench.Run(ctx, c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "wfbench:", err)
		os.Exit(1)
	}
	fmt.Printf("workflows            %d completed of %d in %.2f s (%.0f/s)\n", res.Completed, res.Workflows, res.WallS, res.WorkflowsPerSec)
	fmt.Printf("transitions          %d (%.0f/s)\n", res.Transitions, res.TransitionsPerSec)
	fmt.Printf("transition commit    p50 %.2f ms  p99 %.2f ms\n", res.TransitionP50MS, res.TransitionP99MS)
	fmt.Printf("workflow end-to-end  p50 %.1f ms  p99 %.1f ms\n", res.WorkflowP50MS, res.WorkflowP99MS)
	fmt.Printf("machine              %s/%s, %d CPUs, %s\n", res.GOOS, res.GOARCH, res.NumCPU, res.GoVersion)
	if out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			fmt.Fprintln(os.Stderr, "wfbench:", err)
			os.Exit(1)
		}
		if err := os.WriteFile(out, append(b, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "wfbench:", err)
			os.Exit(1)
		}
	}
}
