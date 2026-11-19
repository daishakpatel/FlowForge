//go:build e2e

package e2e

import (
	"os/exec"
	"syscall"
	"time"
)

// signal sends sig to p's current incarnation; false when it is not running.
func (p *Proc) signal(sig syscall.Signal) (chan struct{}, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil || p.cmd.Process == nil {
		return nil, false
	}
	select {
	case <-p.done:
		return p.done, false
	default:
	}
	_ = p.cmd.Process.Signal(sig) // it may exit concurrently; done reports that
	return p.done, true
}

// Kill sends SIGKILL (resuming a SIGSTOPped process is unnecessary: SIGKILL ends it) and waits
// for the exit. Killing a stopped process is a no-op.
func (h *Harness) Kill(p *Proc) {
	h.t.Helper()
	if done, ok := p.signal(syscall.SIGKILL); ok {
		h.waitExit(p, done, 30*time.Second)
	}
}

// Stop sends SIGTERM and waits for a graceful exit (SIGKILL after 30 s).
func (h *Harness) Stop(p *Proc) {
	h.t.Helper()
	done, ok := p.signal(syscall.SIGTERM)
	if !ok {
		return
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		h.t.Errorf("e2e: %s ignored SIGTERM for 30 s; killing it", p.Name)
		h.Kill(p)
	}
}

func (h *Harness) waitExit(p *Proc, done chan struct{}, d time.Duration) {
	h.t.Helper()
	select {
	case <-done:
	case <-time.After(d):
		h.t.Fatalf("e2e: %s did not exit within %s", p.Name, d)
	}
}

// Restart kills p if it runs and starts it again with the same ports and environment.
func (h *Harness) Restart(p *Proc) {
	h.t.Helper()
	h.Kill(p)
	h.launch(p)
}

// Pause freezes p with SIGSTOP: it holds its connections but does nothing (a zombie).
func (h *Harness) Pause(p *Proc) {
	h.t.Helper()
	if _, ok := p.signal(syscall.SIGSTOP); !ok {
		h.t.Fatalf("e2e: pause %s: not running", p.Name)
	}
}

// Resume continues a paused p with SIGCONT.
func (h *Harness) Resume(p *Proc) {
	h.t.Helper()
	if _, ok := p.signal(syscall.SIGCONT); !ok {
		h.t.Fatalf("e2e: resume %s: not running", p.Name)
	}
}

// PausePostgres freezes the Postgres container (docker pause) for d, then unpauses it:
// connections stay open but every query stalls, as in a network partition or a long GC.
func (h *Harness) PausePostgres(d time.Duration) {
	h.t.Helper()
	id := h.pg.GetContainerID()
	h.docker("pause", id)
	defer h.docker("unpause", id)
	time.Sleep(d)
}

func (h *Harness) docker(args ...string) {
	h.t.Helper()
	if out, err := exec.CommandContext(h.t.Context(), "docker", args...).CombinedOutput(); err != nil { //nolint:gosec // fixed verbs
		h.t.Fatalf("e2e: docker %v: %v: %s", args, err, out)
	}
}
