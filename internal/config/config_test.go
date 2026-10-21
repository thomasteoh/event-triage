package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadMissingRequired(t *testing.T) {
	// Clear all env vars relevant to the config.
	for _, k := range []string{
		"EVENT_TRIAGE_RABBITMQ_URL", "EVENT_TRIAGE_SMTP_TLS_CERT", "EVENT_TRIAGE_SMTP_TLS_KEY",
		"EVENT_TRIAGE_PEPPERMINT_URL", "EVENT_TRIAGE_QUEUE_PREFIX", "EVENT_TRIAGE_CONSUMER_TAG",
		"EVENT_TRIAGE_PREFETCH", "EVENT_TRIAGE_RECONNECT", "EVENT_TRIAGE_SMTP_ADDR",
		"EVENT_TRIAGE_PEPPERMINT_USER", "EVENT_TRIAGE_DEDUP_WINDOW", "EVENT_TRIAGE_RATE_DAY",
		"EVENT_TRIAGE_LISTEN_ADDR", "EVENT_TRIAGE_PEPPERMINT_KEY",
		"EVENT_TRIAGE_RABBITMQ_CLIENT_ID", "EVENT_TRIAGE_RABBITMQ_CLIENT_SECRET", "EVENT_TRIAGE_RABBITMQ_TOKEN_URL",
	} {
		os.Unsetenv(k)
	}
	_, err := Load()
	if err == nil {
		t.Fatal("want error when required vars missing")
	}
}

func TestLoadDefaults(t *testing.T) {
	os.Setenv("EVENT_TRIAGE_RABBITMQ_URL", "amqp://event-triage-rabbitmq:5672/messaging")
	os.Setenv("EVENT_TRIAGE_SMTP_TLS_CERT", "/certs/tls.crt")
	os.Setenv("EVENT_TRIAGE_SMTP_TLS_KEY", "/certs/tls.key")
	os.Setenv("EVENT_TRIAGE_PEPPERMINT_URL", "http://peppermint:5003")
	os.Setenv("EVENT_TRIAGE_RABBITMQ_CLIENT_ID", "event-triage-mq")
	os.Setenv("EVENT_TRIAGE_RABBITMQ_CLIENT_SECRET", "test-secret")
	os.Setenv("EVENT_TRIAGE_RABBITMQ_TOKEN_URL", "https://auth.harmonicr.com/oauth/v2/token")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.QueuePrefix != "events" {
		t.Errorf("default queue prefix: %s", cfg.QueuePrefix)
	}
	if cfg.Prefetch != 10 {
		t.Errorf("default prefetch: %d", cfg.Prefetch)
	}
	if cfg.RatePerDay != 100 {
		t.Errorf("default rate: %d", cfg.RatePerDay)
	}
	if cfg.DedupWindow != time.Hour {
		t.Errorf("default dedup: %s", cfg.DedupWindow)
	}
}
