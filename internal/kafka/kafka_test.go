package kafka_test

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/kafka"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

func TestLoadTopics_DeployFileMatches00E(t *testing.T) {
	t.Parallel()
	f, err := os.Open("../../deploy/kafka/topics.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	topics, err := kafka.LoadTopics(f)
	if err != nil {
		t.Fatal(err)
	}
	const week, fortnight = "604800000", "1209600000"
	// 00E §7, plus flowforge.dlq (00A amendment, 05.01) and market.aggregates' compaction (task file).
	want := map[string]struct {
		partitions int32
		retention  string
		cleanup    string
	}{
		"market.ticks":             {6, week, "delete"},
		"market.invalid":           {3, fortnight, "delete"},
		"market.aggregates":        {6, week, "compact,delete"},
		events.TopicWorkflowEvents: {3, week, "delete"},
		events.TopicTaskEvents:     {3, week, "delete"},
		"flowforge.dlq":            {3, fortnight, "delete"},
	}
	if len(topics) != len(want) {
		t.Fatalf("got %d topics, want %d", len(topics), len(want))
	}
	for _, tp := range topics {
		w, ok := want[tp.Name]
		switch {
		case !ok:
			t.Errorf("unexpected topic %s", tp.Name)
		case tp.Partitions != w.partitions || tp.Configs["retention.ms"] != w.retention || tp.Configs["cleanup.policy"] != w.cleanup:
			t.Errorf("%s = %d partitions, %v; want %d, retention %s, cleanup %s", tp.Name, tp.Partitions, tp.Configs, w.partitions, w.retention, w.cleanup)
		case tp.ReplicationFactor != 1:
			t.Errorf("%s replication factor %d, want 1 (single dev broker)", tp.Name, tp.ReplicationFactor)
		}
	}
}

func TestLoadTopics_Rejects(t *testing.T) {
	t.Parallel()
	ok := "topics:\n  - {name: a.b, partitions: 1, replication_factor: 1}\n"
	if _, err := kafka.LoadTopics(strings.NewReader(ok)); err != nil {
		t.Fatalf("valid file rejected: %v", err)
	}
	for name, in := range map[string]string{
		"unknown field":      "topics:\n  - {name: a, partitions: 1, replication_factor: 1, partition: 2}\n",
		"duplicate":          "topics:\n  - {name: a, partitions: 1, replication_factor: 1}\n  - {name: a, partitions: 2, replication_factor: 1}\n",
		"no partitions":      "topics:\n  - {name: a, replication_factor: 1}\n",
		"no replication":     "topics:\n  - {name: a, partitions: 1}\n",
		"no name":            "topics:\n  - {partitions: 1, replication_factor: 1}\n",
		"invalid name":       "topics:\n  - {name: 'a b', partitions: 1, replication_factor: 1}\n",
		"no topics":          "topics: []\n",
		"not yaml":           "topics: [\n",
		"top-level unknown":  "topic: []\n",
		"negative partition": "topics:\n  - {name: a, partitions: -1, replication_factor: 1}\n",
	} {
		if _, err := kafka.LoadTopics(strings.NewReader(in)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestPlan(t *testing.T) {
	t.Parallel()
	want := []kafka.Topic{
		{Name: "a", Partitions: 6, ReplicationFactor: 1, Configs: map[string]string{"retention.ms": "100", "cleanup.policy": "delete"}},
		{Name: "b", Partitions: 3, ReplicationFactor: 1},
	}
	same := kafka.ExistingTopic{Partitions: 6, ReplicationFactor: 1, Configs: map[string]string{"retention.ms": "100", "cleanup.policy": "delete", "segment.ms": "9"}}
	cases := []struct {
		name     string
		have     map[string]kafka.ExistingTopic
		create   []string
		warnings []string // substrings, one per expected warning
	}{
		{"nothing exists", nil, []string{"a", "b"}, nil},
		{"all as declared", map[string]kafka.ExistingTopic{"a": same, "b": {Partitions: 3, ReplicationFactor: 1}}, nil, nil},
		{"one deleted", map[string]kafka.ExistingTopic{"a": same}, []string{"b"}, nil},
		{"fewer partitions", map[string]kafka.ExistingTopic{"a": {Partitions: 2, ReplicationFactor: 1, Configs: same.Configs}, "b": {Partitions: 3, ReplicationFactor: 1}},
			nil, []string{"a has 2 partitions, want 6"}},
		{"more partitions", map[string]kafka.ExistingTopic{"a": same, "b": {Partitions: 12, ReplicationFactor: 1}},
			nil, []string{"b has 12 partitions, want 3; never shrunk"}},
		{"config differs", map[string]kafka.ExistingTopic{"a": {Partitions: 6, ReplicationFactor: 1, Configs: map[string]string{"retention.ms": "5", "cleanup.policy": "delete"}}, "b": {Partitions: 3, ReplicationFactor: 1}},
			nil, []string{`a retention.ms is "5", want "100"`}},
		{"replication differs", map[string]kafka.ExistingTopic{"a": same, "b": {Partitions: 3, ReplicationFactor: 3}},
			nil, []string{"b has replication factor 3, want 1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			create, warnings := kafka.Plan(want, tc.have)
			var names []string
			for _, c := range create {
				names = append(names, c.Name)
			}
			if !slices.Equal(names, tc.create) {
				t.Errorf("create = %v, want %v", names, tc.create)
			}
			if len(warnings) != len(tc.warnings) {
				t.Fatalf("warnings = %q, want %d like %q", warnings, len(tc.warnings), tc.warnings)
			}
			for i, w := range tc.warnings {
				if !strings.Contains(warnings[i], w) {
					t.Errorf("warning %q lacks %q", warnings[i], w)
				}
			}
		})
	}
}

func TestTraceparent_InjectAndExtract(t *testing.T) {
	t.Parallel()
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	r := &kgo.Record{Topic: "t"}
	kafka.InjectTrace(context.Background(), r)
	if _, ok := kafka.Header(r, kafka.HeaderTraceparent); ok {
		t.Error("traceparent header set without one in the context")
	}

	kafka.InjectTrace(kafka.WithTraceparent(context.Background(), tp), r)
	if got, _ := kafka.Header(r, kafka.HeaderTraceparent); got != tp {
		t.Errorf("header = %q, want %q", got, tp)
	}
	if got := kafka.TraceparentFrom(kafka.ExtractTrace(context.Background(), r)); got != tp {
		t.Errorf("extracted %q, want %q", got, tp)
	}

	other := "00-11111111111111111111111111111111-2222222222222222-01"
	kafka.InjectTrace(kafka.WithTraceparent(context.Background(), other), r)
	if got, _ := kafka.Header(r, kafka.HeaderTraceparent); got != tp {
		t.Errorf("InjectTrace overwrote the record's own traceparent: %q", got)
	}

	kafka.SetHeader(r, "event_id", "e1")
	kafka.SetHeader(r, "event_id", "e2")
	if got, _ := kafka.Header(r, "event_id"); got != "e2" || len(r.Headers) != 2 {
		t.Errorf("SetHeader: event_id %q, %d headers; want e2, 2", got, len(r.Headers))
	}
	shared := make([]kgo.RecordHeader, 0, 4) // two records sharing one backing array
	r1, r2 := &kgo.Record{Headers: shared}, &kgo.Record{Headers: shared}
	kafka.SetHeader(r1, "reason", "bad_price")
	kafka.SetHeader(r2, "reason", "stale")
	if got, _ := kafka.Header(r1, "reason"); got != "bad_price" {
		t.Errorf("SetHeader on one record changed another's header: r1 reason = %q", got)
	}
	if got := kafka.TraceparentFrom(kafka.ExtractTrace(context.Background(), &kgo.Record{})); got != "" {
		t.Errorf("extracted %q from a record without the header", got)
	}
}

func TestNewClients_RejectBadConfig(t *testing.T) {
	t.Parallel()
	good := kafka.Config{Brokers: []string{"localhost:1"}, ClientID: "t", SecurityProtocol: "PLAINTEXT"}
	for name, cfg := range map[string]kafka.Config{
		"no brokers": {ClientID: "t", SecurityProtocol: "PLAINTEXT"},
		"ssl":        {Brokers: good.Brokers, ClientID: "t", SecurityProtocol: "SSL"},
		"sasl":       {Brokers: good.Brokers, ClientID: "t", SecurityProtocol: "SASL_SSL"},
	} {
		if p, err := kafka.NewProducer(cfg); err == nil {
			p.Close(context.Background())
			t.Errorf("producer %s: accepted", name)
		}
		if c, err := kafka.NewConsumer(cfg, "g", "t"); err == nil {
			c.Close()
			t.Errorf("consumer %s: accepted", name)
		}
	}
	if _, err := kafka.NewConsumer(good, "", "t"); err == nil {
		t.Error("consumer without a group accepted")
	}
	if _, err := kafka.NewConsumer(good, "g"); err == nil {
		t.Error("consumer without topics accepted")
	}
	// Creating clients does not dial; nothing listens on localhost:1.
	p, err := kafka.NewProducer(good)
	if err != nil {
		t.Fatal(err)
	}
	p.Close(context.Background())
	c, err := kafka.NewConsumer(good, "g", "t")
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}
