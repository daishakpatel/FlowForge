//go:build integration

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/kafka"
	"github.com/daishakpatel/flowforge/internal/kafka/kafkatest"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// TestCLI_TopicsEnsureIsIdempotent is `make topics` twice (task 05.01 §8): the first run creates the
// topics, the second reports no changes.
func TestCLI_TopicsEnsureIsIdempotent(t *testing.T) {
	t.Parallel()
	k := kafkatest.Start(t)
	a, b := kafkatest.Name("cli"), kafkatest.Name("cli")
	file := filepath.Join(t.TempDir(), "topics.yaml")
	yaml := "topics:\n" +
		"  - {name: " + a + ", partitions: 3, replication_factor: 1, configs: {retention.ms: \"604800000\"}}\n" +
		"  - {name: " + b + ", partitions: 1, replication_factor: 1}\n"
	if err := os.WriteFile(file, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	adm, err := kafka.NewAdmin(k.Config())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = adm.DeleteTopics(ctx, a, b) // cleanup only; the names are unique
		adm.Close()
	})
	ensure := func() (int, string, string) {
		var out, errb bytes.Buffer
		env := map[string]string{"FF_KAFKA_BROKERS": strings.Join(k.Brokers, ",")}
		code := run(t.Context(), []string{"topics", "ensure", "-f", file}, func(key string) string { return env[key] }, &out, &errb)
		return code, out.String(), errb.String()
	}

	code, out, errOut := ensure()
	if code != 0 || !strings.Contains(out, "created "+a) || !strings.Contains(out, "created "+b) {
		t.Fatalf("first run: exit %d, stdout %q, stderr %q; want both created", code, out, errOut)
	}
	testutil.WaitFor(t, func() bool { // the broker lists new topics a moment after creating them
		d, err := adm.ListTopics(t.Context(), a, b)
		return err == nil && d.Error() == nil
	}, 30*time.Second)
	code, out, errOut = ensure()
	if code != 0 || out != "no changes: all 2 topics exist as declared\n" {
		t.Fatalf("second run: exit %d, stdout %q, stderr %q; want no changes", code, out, errOut)
	}
}
