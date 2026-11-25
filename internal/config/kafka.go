package config

// Kafka configures a Kafka client (task 05.01): `flowforgectl topics ensure`, and the producers and
// consumers built with internal/kafka. Services that already carry FF_KAFKA_BROKERS (Scheduler,
// Worker, Processor) read the same variable.
type Kafka struct {
	Brokers  []string `env:"FF_KAFKA_BROKERS" default:"localhost:9092" validate:"required,addr"`
	ClientID string   `env:"FF_KAFKA_CLIENT_ID" default:"flowforge"`
	// SecurityProtocol is a placeholder for TLS and SASL (Kafka's security.protocol): only PLAINTEXT
	// is accepted until task 09.01 adds SSL, SASL_PLAINTEXT and SASL_SSL with their credentials.
	SecurityProtocol string `env:"FF_KAFKA_SECURITY_PROTOCOL" default:"PLAINTEXT" validate:"oneof=PLAINTEXT"`
}
