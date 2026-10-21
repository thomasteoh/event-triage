// Command event-triage is the unified event-triage bridge.
//
// It receives inbound support mail from Google Workspace routing on an SMTP
// listener (TLS port 5225), consumes normalized events from the RabbitMQ
// "messaging" vhost (events.email/.pipeline/.log/.health), applies triage
// (normalize, dedup, rate-limit), and creates Peppermint tickets via the
// internal public-create endpoint. It is a receiver/relay — no mailbox.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/thomasteoh/event-triage/internal/config"
	"github.com/thomasteoh/event-triage/internal/mq"
	"github.com/thomasteoh/event-triage/internal/peppermint"
	"github.com/thomasteoh/event-triage/internal/server"
	"github.com/thomasteoh/event-triage/internal/smtp"
	"github.com/thomasteoh/event-triage/internal/triage"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: event-triage <serve> [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	switch cmd {
	case "serve":
		fs := flag.NewFlagSet("serve", flag.ContinueOnError)
		addr := fs.String("addr", ":8080", "HTTP health/readiness listen address")
		smptAddr := fs.String("smtp-addr", "", "SMTP listener address (default from env)")
		if err := fs.Parse(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if err := run(*addr, *smptAddr); err != nil {
			fmt.Fprintln(os.Stderr, "serve:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		os.Exit(2)
	}
}

func run(httpAddr, smtpOverride string) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	smtpAddr := cfg.SMTPListenAddr
	if smtpOverride != "" {
		smtpAddr = smtpOverride
	}

	logger := log.New(os.Stderr, "event-triage: ", log.LstdFlags|log.Lmicroseconds)

	// Triage engine: normalize, dedup, rate-limit.
	engine := triage.NewEngine(cfg.DedupWindow, cfg.RatePerDay)

	// Peppermint client (internal container network only).
	pp := peppermint.New(cfg.PeppermintURL, cfg.PeppermintKey, cfg.PeppermintUser)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// RabbitMQ consumer: every event → triage → ticket. Auth is OAuth2: a
	// client-credentials JWT is fetched from Zitadel and presented as the
	// SASL PLAIN password (no plain password in the URL or env).
	tokenProvider := mq.NewOAuthTokenProvider(
		cfg.RabbitMQClientID, cfg.RabbitMQClientSecret, cfg.RabbitMQTokenURL, cfg.RabbitMQTokenScope)
	mqCfg := mq.Config{
		URL:           cfg.RabbitMQURL,
		ConsumerTag:   cfg.ConsumerTag,
		Prefetch:      cfg.Prefetch,
		Reconnect:     cfg.Reconnect,
		TokenProvider: tokenProvider,
	}
	consumer := mq.NewConsumer(mqCfg, func(ctx context.Context, e triage.Event) error {
		dec, err := engine.Evaluate(&e)
		if err != nil {
			return fmt.Errorf("triage: %w", err)
		}
		switch dec {
		case triage.DecisionDuplicate:
			logger.Printf("mq: duplicate suppressed (source=%s type=%s)", e.Source, e.Type)
			return nil
		case triage.DecisionRateLimit:
			logger.Printf("mq: rate-limited (source=%s)", e.Source)
			return nil
		}
		id, err := pp.CreateTicket(ctx, &e)
		if err != nil {
			return fmt.Errorf("create ticket: %w", err)
		}
		logger.Printf("mq: ticket created id=%s source=%s type=%s", id, e.Source, e.Type)
		return nil
	}, logger)

	go func() {
		if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Printf("mq: consumer stopped: %v", err)
		}
	}()

	// SMTP listener: Google-routed support mail → parsed event → ticket.
	smtpSrv := smtp.New(smtpAddr, cfg.SMTPTLSCert, cfg.SMTPTLSKey, func(ctx context.Context, e triage.Event) error {
		dec, err := engine.Evaluate(&e)
		if err != nil {
			return fmt.Errorf("triage email: %w", err)
		}
		if dec != triage.DecisionCreate {
			logger.Printf("smtp: %s (source=%s)", dec, e.Source)
			return nil
		}
		id, err := pp.CreateTicket(ctx, &e)
		if err != nil {
			return fmt.Errorf("create email ticket: %w", err)
		}
		logger.Printf("smtp: ticket created id=%s from=%s", id, e.From)
		return nil
	}, logger)
	go func() {
		if err := smtpSrv.Listen(ctx); err != nil && ctx.Err() == nil {
			logger.Printf("smtp: listener stopped: %v", err)
		}
	}()

	// HTTP health/readiness.
	health := func() bool { return true }
	ready := func() bool { return consumer.Ready() && pp.Ready() }
	httpSrv := server.New(httpAddr, health, ready, nil, logger)
	if err := httpSrv.Listen(ctx); err != nil {
		return err
	}
	return nil
}
