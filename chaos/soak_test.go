package chaos

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestKill9Soak runs a short chaos soak against freshly built binaries. It is skipped unless WF_CHAOS=1
// because it spawns real processes and takes about a minute: run it through scripts/gosh.sh (see the README).
func TestKill9Soak(t *testing.T) {
	if os.Getenv("WF_CHAOS") != "1" {
		t.Skip("set WF_CHAOS=1 to run the kill -9 soak")
	}
	bin := t.TempDir()
	build := exec.Command("go", "build", "-o", bin+"/", "./cmd/wfd", "./examples/transfer/cmd/transfer-worker")
	build.Dir = ".." // the module root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rep, err := Run(ctx, Config{BinDir: bin, WorkDir: filepath.Join(t.TempDir(), "work"), Seed: 1, Workflows: 30, Kills: 15, Progress: os.Stderr})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + rep.Summary())
	if !rep.Pass {
		t.Fatalf("soak failed: %+v", rep.Violations)
	}
	if rep.KillsTotal == 0 {
		t.Fatal("the soak injected no kills")
	}
}
