// Package config loads the event-triage configuration from environment
// variables. Secrets (RabbitMQ password, Peppermint API endpoint, SMTP TLS
// cert paths) are read from env — never committed, never logged.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the full bridge configuration, loaded from environment variables.
type Config struct {
	// RabbitMQ (existing mq.harmonicr.com broker, vhost "messaging").
	RabbitMQURL  string // amqp://mq:<password>@rabbitmq-mq:5672/messaging
	QueuePrefix  string // queue name prefix, e.g. "events"
	ConsumerTag  string
	Prefetch     int
	Reconnect    time.Duration // reconnect backoff between attempts

	// SMTP listener (inbound support mail from Google Workspace routing).
	SMTPListenAddr string // e.g. ":5225" (TLS)
	SMTPTLSCert    string // CA-signed cert path
	SMTPTLSKey     string // private key path

	// Peppermint (ticket desk, called over the internal container network).
	PeppermintURL  string // e.g. "http://peppermint:5003" or "http://127.0.0.1:52171"
	PeppermintKey  string // optional X-API-Key if the create endpoint is gated
	PeppermintUser string // createdBy label, e.g. "event-triage"

	// Triage limits.
	DedupWindow time.Duration // fingerprint dedup window
	RatePerDay  int           // max tickets per source per day

	// HTTP health/readiness endpoint.
	ListenAddr string // e.g. ":8080"
}

// Load reads the config from environment variables. Missing required values
// produce an error so the service fails closed rather than silently running
// with an empty config.
func Load() (*Config, error) {
	cfg := &Config{
		RabbitMQURL:   os.Getenv("EVENT_TRIAGE_RABBITMQ_URL"),
		QueuePrefix:   getenv("EVENT_TRIAGE_QUEUE_PREFIX", "events"),
		ConsumerTag:   getenv("EVENT_TRIAGE_CONSUMER_TAG", "event-triage"),
		Prefetch:      atoiDefault(getenv("EVENT_TRIAGE_PREFETCH", "10"), 10),
		Reconnect:     durDefault(getenv("EVENT_TRIAGE_RECONNECT", "5s"), 5*time.Second),
		SMTPListenAddr: getenv("EVENT_TRIAGE_SMTP_ADDR", ":5225"),
		SMTPTLSCert:    os.Getenv("EVENT_TRIAGE_SMTP_TLS_CERT"),
		SMTPTLSKey:     os.Getenv("EVENT_TRIAGE_SMTP_TLS_KEY"),
		PeppermintURL:  os.Getenv("EVENT_TRIAGE_PEPPERMINT_URL"),
		PeppermintKey:  os.Getenv("EVENT_TRIAGE_PEPPERMINT_KEY"),
		PeppermintUser: getenv("EVENT_TRIAGE_PEPPERMINT_USER", "event-triage"),
		DedupWindow:    durDefault(getenv("EVENT_TRIAGE_DEDUP_WINDOW", "1h"), time.Hour),
		RatePerDay:     atoiDefault(getenv("EVENT_TRIAGE_RATE_DAY", "100"), 100),
		ListenAddr:     getenv("EVENT_TRIAGE_LISTEN_ADDR", ":8080"),
	}

	if cfg.RabbitMQURL == "" {
		return nil, fmt.Errorf("EVENT_TRIAGE_RABBITMQ_URL is required")
	}
	if cfg.SMTPTLSCert == "" || cfg.SMTPTLSKey == "" {
		return nil, fmt.Errorf("EVENT_TRIAGE_SMTP_TLS_CERT and EVENT_TRIAGE_SMTP_TLS_KEY are required")
	}
	if cfg.PeppermintURL == "" {
		return nil, fmt.Errorf("EVENT_TRIAGE_PEPPERMINT_URL is required")
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func durDefault(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return def
	}
	return d
}
