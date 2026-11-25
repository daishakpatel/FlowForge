package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.yaml.in/yaml/v3"
)

// Topic is one entry of deploy/kafka/topics.yaml.
type Topic struct {
	Name              string            `yaml:"name"`
	Key               string            `yaml:"key"`     // documentation: what producers key records by
	Purpose           string            `yaml:"purpose"` // documentation
	Partitions        int32             `yaml:"partitions"`
	ReplicationFactor int16             `yaml:"replication_factor"`
	Configs           map[string]string `yaml:"configs"` // topic configs, e.g. retention.ms, cleanup.policy
}

// ExistingTopic is what the broker reports for a topic that exists.
type ExistingTopic struct {
	Partitions        int32
	ReplicationFactor int
	Configs           map[string]string
}

// EnsureResult reports what EnsureTopics did.
type EnsureResult struct {
	Created  []string // topics created, in file order
	Existing []string // topics that already existed, in file order
	Warnings []string // differences between existing topics and the file; nothing was changed
}

var topicName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

// LoadTopics reads a topics file, rejecting unknown keys, duplicates and incomplete entries.
func LoadTopics(r io.Reader) ([]Topic, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	var f struct {
		Topics []Topic `yaml:"topics"`
	}
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("topics file: %w", err)
	}
	if len(f.Topics) == 0 {
		return nil, errors.New("topics file: no topics")
	}
	seen := map[string]bool{}
	for i, t := range f.Topics {
		switch {
		case !topicName.MatchString(t.Name):
			return nil, fmt.Errorf("topics file: entry %d: invalid topic name %q", i+1, t.Name)
		case seen[t.Name]:
			return nil, fmt.Errorf("topics file: %s listed twice", t.Name)
		case t.Partitions <= 0 || t.ReplicationFactor <= 0:
			return nil, fmt.Errorf("topics file: %s needs partitions and replication_factor > 0", t.Name)
		}
		seen[t.Name] = true
	}
	return f.Topics, nil
}

// Plan compares the wanted topics with the existing ones (by name). It returns the topics to
// create and a warning for every difference in an existing topic. It never proposes changing an
// existing topic: more partitions would move keys to other partitions, and partitions are never
// removed.
func Plan(want []Topic, have map[string]ExistingTopic) (create []Topic, warnings []string) {
	for _, t := range want {
		h, ok := have[t.Name]
		if !ok {
			create = append(create, t)
			continue
		}
		switch {
		case h.Partitions < t.Partitions:
			warnings = append(warnings, fmt.Sprintf("%s has %d partitions, want %d; not changed (adding partitions moves keys)", t.Name, h.Partitions, t.Partitions))
		case h.Partitions > t.Partitions:
			warnings = append(warnings, fmt.Sprintf("%s has %d partitions, want %d; never shrunk", t.Name, h.Partitions, t.Partitions))
		}
		if h.ReplicationFactor != int(t.ReplicationFactor) {
			warnings = append(warnings, fmt.Sprintf("%s has replication factor %d, want %d; not changed", t.Name, h.ReplicationFactor, t.ReplicationFactor))
		}
		for _, k := range slices.Sorted(maps.Keys(t.Configs)) {
			if got := h.Configs[k]; got != t.Configs[k] {
				warnings = append(warnings, fmt.Sprintf("%s %s is %q, want %q; not changed", t.Name, k, got, t.Configs[k]))
			}
		}
	}
	return create, warnings
}

// EnsureTopics creates the wanted topics that do not exist and reports how the existing ones
// differ (Plan). It is idempotent: a second run creates nothing. The broker may not list a topic
// for a moment after it is created or deleted; such a topic counts as existing, with a warning that
// it was not compared with the file.
func EnsureTopics(ctx context.Context, adm *kadm.Client, want []Topic) (EnsureResult, error) {
	have, err := describe(ctx, adm, want)
	if err != nil {
		return EnsureResult{}, err
	}
	create, warnings := Plan(want, have)
	res := EnsureResult{Warnings: warnings}
	for _, t := range want {
		if _, ok := have[t.Name]; ok {
			res.Existing = append(res.Existing, t.Name)
		}
	}
	for _, t := range create {
		configs := make(map[string]*string, len(t.Configs))
		for k, v := range t.Configs {
			configs[k] = &v
		}
		resp, err := adm.CreateTopic(ctx, t.Partitions, t.ReplicationFactor, configs, t.Name)
		if err == nil {
			err = resp.Err
		}
		switch {
		case errors.Is(err, kerr.TopicAlreadyExists):
			res.Existing = append(res.Existing, t.Name)
			res.Warnings = append(res.Warnings, fmt.Sprintf("%s exists but the broker does not list it yet (just created or deleted); not compared with the file, run again", t.Name))
		case err != nil:
			return res, fmt.Errorf("create topic %s: %w", t.Name, err)
		default:
			res.Created = append(res.Created, t.Name)
		}
	}
	return res, nil
}

// describe returns the partition count, replication factor and configs of the wanted topics that
// exist.
func describe(ctx context.Context, adm *kadm.Client, want []Topic) (map[string]ExistingTopic, error) {
	names := make([]string, len(want))
	for i, t := range want {
		names[i] = t.Name
	}
	details, err := adm.ListTopics(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	have := map[string]ExistingTopic{}
	var existing []string
	for _, n := range names {
		d, ok := details[n]
		switch {
		case !ok || kerr.IsRetriable(d.Err):
			// Unknown, or not ready yet (UNKNOWN_TOPIC_OR_PARTITION, LEADER_NOT_AVAILABLE just after
			// a create or delete): treated as missing, so a topic that does exist ends up as
			// TopicAlreadyExists in EnsureTopics, which warns that it was not compared.
			continue
		case d.Err != nil:
			return nil, fmt.Errorf("describe topic %s: %w", n, d.Err)
		}
		have[n] = ExistingTopic{Partitions: int32(len(d.Partitions)), ReplicationFactor: d.Partitions.NumReplicas()} //nolint:gosec // G115: partition counts are far below 2^31
		existing = append(existing, n)
	}
	if len(existing) == 0 {
		return have, nil
	}
	rcs, err := adm.DescribeTopicConfigs(ctx, existing...)
	if err != nil {
		return nil, fmt.Errorf("describe topic configs: %w", err)
	}
	for _, rc := range rcs {
		if rc.Err != nil {
			return nil, fmt.Errorf("describe configs of %s: %w", rc.Name, rc.Err)
		}
		e := have[rc.Name]
		e.Configs = make(map[string]string, len(rc.Configs))
		for _, c := range rc.Configs {
			if c.Value != nil {
				e.Configs[c.Key] = *c.Value
			}
		}
		have[rc.Name] = e
	}
	return have, nil
}
