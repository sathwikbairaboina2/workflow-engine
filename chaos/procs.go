package chaos

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// proc is one child process with its own log file.
type proc struct {
	name string
	cmd  *exec.Cmd
	log  *os.File
	done chan struct{}
}

// startProc launches bin with args, appending its output to <workDir>/<name>.log.
func startProc(workDir, name, bin string, args ...string) (*proc, error) {
	logf, err := os.OpenFile(filepath.Join(workDir, name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	p := &proc{name: name, cmd: cmd, log: logf, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(p.done)
	}()
	return p, nil
}

func (p *proc) alive() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// kill sends SIGKILL and waits for the process to be reaped. It reports whether the process was alive.
func (p *proc) kill() bool {
	if !p.alive() {
		return false
	}
	_ = p.cmd.Process.Kill()
	<-p.done
	p.log.Close()
	return true
}

// stop asks the process to exit with SIGTERM and falls back to SIGKILL after timeout.
func (p *proc) stop(timeout time.Duration) {
	if !p.alive() {
		p.log.Close()
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(timeout):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
	p.log.Close()
}

// waitHealthy polls /healthz every 10ms until it answers 200 and returns how long that took.
func waitHealthy(ctx context.Context, baseURL string, timeout time.Duration) (time.Duration, error) {
	begin := time.Now()
	hc := &http.Client{Timeout: time.Second}
	for time.Since(begin) < timeout {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		resp, err := hc.Get(baseURL + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return time.Since(begin), nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, fmt.Errorf("wfd did not become healthy within %s", timeout)
}
