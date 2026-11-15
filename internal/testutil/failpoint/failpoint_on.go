//go:build failpoint

package failpoint

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

// Enabled reports whether this build carries failpoints (build tag `failpoint`).
const Enabled = true

// The armed points are process-wide by nature (a failpoint stands in for a crash of the whole
// process), so this test-only build keeps them in package state, guarded by mu.
var (
	mu     sync.Mutex
	armed  map[string]string  // name → "error" | "panic"
	hits   = map[string]int{} // name → times an armed point fired
	loaded bool
)

// load arms the points listed in FF_FAILPOINTS once.
func load() {
	if loaded {
		return
	}
	loaded = true
	armed = map[string]string{}
	for _, item := range strings.Split(os.Getenv("FF_FAILPOINTS"), ",") {
		if name, action, ok := strings.Cut(strings.TrimSpace(item), "="); ok {
			armed[name] = action
		}
	}
}

// Enable arms name with action "error" or "panic".
func Enable(name, action string) {
	mu.Lock()
	defer mu.Unlock()
	load()
	armed[name] = action
}

// Disable disarms name.
func Disable(name string) {
	mu.Lock()
	defer mu.Unlock()
	load()
	delete(armed, name)
}

// Reset disarms every point.
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	loaded, armed = true, map[string]string{}
}

// Hits reports how many times name fired while armed, so a test can prove its failpoint ran.
func Hits(name string) int {
	mu.Lock()
	defer mu.Unlock()
	return hits[name]
}

// Inject fails as name is armed: an error wrapping ErrInjected, or a panic.
func Inject(name string) error {
	mu.Lock()
	load()
	action := armed[name]
	if action != "" {
		hits[name]++
	}
	mu.Unlock()
	switch action {
	case "error":
		return fmt.Errorf("%w: %s", ErrInjected, name)
	case "panic":
		panic("failpoint: " + name)
	}
	return nil
}
