package testutil

import (
	"context"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
)

// Proxy forwards TCP connections to a target, and a test can Cut it to make the target look
// down: open connections drop and new ones are refused until Restore. It simulates a database
// outage without stopping a container other tests share (task 03.04).
type Proxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	down  bool
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// NewProxy starts a Proxy to target (host:port) on a loopback port; it stops in t.Cleanup.
func NewProxy(t testing.TB, target string) *Proxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go p.accept()
	t.Cleanup(func() {
		_ = ln.Close() // ends accept
		p.Cut()
		p.wg.Wait()
	})
	return p
}

// ProxyDSN returns dsn with its host:port replaced by the proxy's, and starts that proxy.
func ProxyDSN(t testing.TB, dsn string) (*Proxy, string) {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err) // the DSN is a test container's, without secrets
	}
	p := NewProxy(t, u.Host)
	u.Host = p.ln.Addr().String()
	return p, u.String()
}

// Cut drops every open connection and refuses new ones until Restore.
func (p *Proxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = true
	for c := range p.conns {
		_ = c.Close() // the peer sees the connection drop, which is the point
	}
	clear(p.conns)
}

// Restore accepts connections again.
func (p *Proxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = false
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return // listener closed
		}
		p.mu.Lock()
		down := p.down
		p.mu.Unlock()
		if down {
			_ = c.Close() // refused while cut
			continue
		}
		up, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		if !p.track(c, up) {
			continue
		}
		p.wg.Add(2)
		go p.pipe(c, up)
		go p.pipe(up, c)
	}
}

// track registers both ends unless the proxy was cut meanwhile, in which case it closes them.
func (p *Proxy) track(a, b net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.down {
		_, _ = a.Close(), b.Close()
		return false
	}
	p.conns[a], p.conns[b] = struct{}{}, struct{}{}
	return true
}

func (p *Proxy) pipe(dst, src net.Conn) {
	defer p.wg.Done()
	_, _ = io.Copy(dst, src) // ends when either side closes
	_, _ = dst.Close(), src.Close()
}
