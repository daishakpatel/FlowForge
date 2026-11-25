// Package kafkatest gives integration tests a real Kafka broker (task 05.01), the way
// internal/testutil gives them databases:
//
//	//go:build integration
//
//	func TestPublish(t *testing.T) {
//		k := kafkatest.Start(t)
//		topic := k.Topic(t, 3) // a fresh topic, deleted when the test ends
//		p, err := kafka.NewProducer(k.Config())
//		// ...
//	}
//
// The broker is one apache/kafka container in KRaft mode, the image Compose runs, started on first
// use and shared by the whole test binary; tests stay isolated by using their own topics and
// consumer groups. FF_TEST_KAFKA_BROKERS (comma-separated host:port) uses an existing broker
// instead, such as Compose's localhost:19092. Without Docker the test is skipped, unless
// FF_TEST_REQUIRE_DOCKER is set (CI), in which case it fails.
package kafkatest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/daishakpatel/flowforge/internal/kafka"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// Image matches the kafka service in docker-compose.yml; update both together.
const Image = "apache/kafka:4.1.1"

const (
	brokersEnv       = "FF_TEST_KAFKA_BROKERS"
	requireDockerEnv = "FF_TEST_REQUIRE_DOCKER"
	advertisedFile   = "/tmp/advertised"
)

// Broker is a running Kafka broker.
type Broker struct {
	// Brokers are its bootstrap addresses, host:port.
	Brokers []string
}

// server is the broker shared by the test binary. Starting a container per test would cost
// seconds each, so this is the one piece of package-level state (as in internal/testutil).
type server struct {
	mu      sync.Mutex
	brokers []string
}

var shared = &server{}

// Start returns the shared broker, starting it on first use. It skips or fails t when no broker is
// available (see the package documentation).
func Start(t testing.TB) *Broker {
	t.Helper()
	shared.mu.Lock()
	defer shared.mu.Unlock()
	if shared.brokers == nil {
		if env := os.Getenv(brokersEnv); strings.TrimSpace(env) != "" {
			for _, b := range strings.Split(env, ",") {
				if b = strings.TrimSpace(b); b != "" {
					shared.brokers = append(shared.brokers, b)
				}
			}
		} else if addr, err := startContainer(t.Context()); err != nil {
			unavailable(t, err)
		} else {
			shared.brokers = []string{addr}
		}
	}
	return &Broker{Brokers: shared.brokers}
}

// Config returns a client config for the broker.
func (b *Broker) Config() kafka.Config {
	return kafka.Config{Brokers: b.Brokers, ClientID: "flowforge-test", SecurityProtocol: "PLAINTEXT"}
}

// Topic creates a topic with a unique name (test-<hex>) and the given partitions, and deletes it
// when the test ends. It returns once the broker lists the topic with a leader for every partition:
// right after CreateTopic it may not yet, and a produce then fails with UNKNOWN_TOPIC_OR_PARTITION.
func (b *Broker) Topic(t testing.TB, partitions int32) string {
	t.Helper()
	name := Name("test")
	adm, err := kafka.NewAdmin(b.Config())
	if err != nil {
		t.Fatalf("kafkatest: %v", err)
	}
	t.Cleanup(adm.Close)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	resp, err := adm.CreateTopic(ctx, partitions, 1, nil, name)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		t.Fatalf("kafkatest: create topic %s: %v", name, err)
	}
	testutil.WaitFor(t, func() bool { return ready(ctx, adm, name, partitions) }, time.Minute)
	t.Cleanup(func() {
		// t.Context() is already cancelled when cleanups run.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := adm.DeleteTopics(ctx, name); err != nil {
			t.Errorf("kafkatest: delete topic %s: %v", name, err)
		}
	})
	return name
}

// ready reports whether the broker lists topic with the given partitions, each with a leader.
func ready(ctx context.Context, adm *kadm.Client, topic string, partitions int32) bool {
	d, err := adm.ListTopics(ctx, topic)
	if err != nil || d[topic].Err != nil || len(d[topic].Partitions) != int(partitions) {
		return false
	}
	for _, p := range d[topic].Partitions {
		if p.Err != nil || p.Leader < 0 {
			return false
		}
	}
	return true
}

// Name returns prefix-<16 hex chars>, unique per call: for topics and consumer groups.
func Name(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b) // crypto/rand.Read never returns an error
	return prefix + "-" + hex.EncodeToString(b)
}

// startContainer runs a single-node KRaft broker. Its advertised listener must be the host port
// Docker maps, which is known only once the container runs, so the container waits for that
// address in advertisedFile before it starts Kafka.
func startContainer(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var addr string
	c, err := testcontainers.Run(ctx, Image,
		testcontainers.WithExposedPorts("9092/tcp"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://:9092,CONTROLLER://:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		}),
		testcontainers.WithCmd("sh", "-c",
			"while [ ! -f "+advertisedFile+" ]; do sleep 0.1; done; "+
				"export KAFKA_ADVERTISED_LISTENERS=$(cat "+advertisedFile+"); exec /etc/kafka/docker/run"),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{func(ctx context.Context, c testcontainers.Container) error {
				ep, err := c.PortEndpoint(ctx, "9092/tcp", "")
				if err != nil {
					return fmt.Errorf("mapped port: %w", err)
				}
				addr = ep
				if err := c.CopyToContainer(ctx, []byte("PLAINTEXT://"+ep), advertisedFile, 0o644); err != nil {
					return fmt.Errorf("write advertised listener: %w", err)
				}
				return wait.ForLog("Kafka Server started").WithStartupTimeout(2*time.Minute).WaitUntilReady(ctx, c)
			}},
		}),
	)
	if err != nil {
		if c != nil {
			_ = c.Terminate(context.WithoutCancel(ctx)) // best effort; Ryuk removes it when the binary exits anyway
		}
		return "", fmt.Errorf("start %s: %w", Image, err)
	}
	return addr, nil
}

// unavailable skips t, or fails it when FF_TEST_REQUIRE_DOCKER is set, so CI can't pass by
// skipping everything.
func unavailable(t testing.TB, err error) {
	t.Helper()
	if os.Getenv(requireDockerEnv) != "" {
		t.Fatalf("kafkatest: %v (%s is set)", err, requireDockerEnv)
	}
	t.Skipf("kafkatest: skipping, cannot start Kafka (is Docker running?): %v. Set %s to use an existing broker, or %s to fail instead of skip.", err, brokersEnv, requireDockerEnv)
}
