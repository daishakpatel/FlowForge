//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/kafka"
	"github.com/daishakpatel/flowforge/internal/kafka/kafkatest"
)

// TestRun_CountExitsWithASummary (task 05.03 §9): --count N publishes N ticks, prints a summary and
// exits 0. The command uses the fixed 00E topics, created here on this test binary's broker.
func TestRun_CountExitsWithASummary(t *testing.T) {
	k := kafkatest.Start(t)
	adm, err := kafka.NewAdmin(k.Config())
	if err != nil {
		t.Fatal(err)
	}
	defer adm.Close()
	for _, topic := range []string{"market.ticks", "market.invalid"} {
		resp, err := adm.CreateTopic(t.Context(), 3, 1, nil, topic)
		if err == nil {
			err = resp.Err
		}
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			t.Fatal(err)
		}
	}
	var out, errb bytes.Buffer
	args := []string{"--clock", "simulated", "--count", "300", "--seed", "9", "--inject", "invalid=10%,dup=5%"}
	env := map[string]string{"FF_KAFKA_BROKERS": strings.Join(k.Brokers, ","), "FF_PRODUCER_HTTP_ADDR": "127.0.0.1:0"}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	if code := run(ctx, args, func(key string) (string, bool) { v, ok := env[key]; return v, ok }, &out, &errb, clock.Real{}); code != 0 {
		t.Fatalf("exit %d\nstderr: %s", code, errb.String())
	}
	var sum summary
	if err := json.Unmarshal(out.Bytes(), &sum); err != nil {
		t.Fatalf("summary %q: %v", out.String(), err)
	}
	if sum.Read != 300 || sum.Published+sum.Rejected != 300 || sum.Rejected == 0 || sum.Injected["invalid"] == 0 || sum.Injected["dup"] == 0 {
		t.Errorf("summary %+v", sum)
	}
}
