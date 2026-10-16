// Package triage defines the normalized Event model shared by all sources
// (email, pipeline, log, health) and the triage logic that converts an event
// into a Peppermint ticket payload.
package triage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// Source identifies where an event originated. It maps to the RabbitMQ queue
// the event was consumed from and the ticket type/labels assigned.
type Source string

const (
	SourceEmail    Source = "email"    // inbound support mail (Google routing → SMTP listener)
	SourcePipeline Source = "pipeline" // Databricks webhook (jobs_failed, etc.)
	SourceLog      Source = "log"      // log-service failures / collector
	SourceHealth   Source = "health"   // server health probe / cron monitor
)

// Severity is the triage severity. It maps to Peppermint priority.
type Severity string

const (
	SeverityCritical Severity = "critical"
	SeverityHigh     Severity = "high"
	SeverityMedium   Severity = "medium"
	SeverityLow      Severity = "low"
)

// Event is the normalized event consumed from RabbitMQ. Every source publishes
// this shape (via the bridge publisher); the triage layer reads it.
type Event struct {
	Source    Source    `json:"source"`
	Type      string    `json:"type"`      // e.g. "support_email", "jobs_failed", "log_error"
	Title     string    `json:"title"`     // short human-readable subject
	Detail    string    `json:"detail"`    // body / description
	From      string    `json:"from,omitempty"`    // sender (email)
	Company   string    `json:"company,omitempty"` // company (email)
	Email     string    `json:"email,omitempty"`   // reply-to (email)
	Engineer  string    `json:"engineer,omitempty"`
	Priority  string    `json:"priority,omitempty"` // override (else derived from severity)
	Severity  Severity  `json:"severity,omitempty"`
	Labels    []string  `json:"labels,omitempty"` // triage labels
	CreatedBy string    `json:"created_by,omitempty"`
	Fingerprint string  `json:"fingerprint,omitempty"` // dedup key (optional, set by source)
	CreatedAt  time.Time `json:"created_at"`
}

// FingerprintKey computes a stable dedup key from the event's identifying fields.
// It is used to suppress duplicate tickets within DedupWindow.
func (e *Event) FingerprintKey() string {
	key := fmt.Sprintf("%s|%s|%s|%s", e.Source, e.Type, e.From, e.Title)
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// TicketPriority derives the Peppermint priority from severity when not overridden.
func (e *Event) TicketPriority() string {
	if e.Priority != "" {
		return e.Priority
	}
	switch e.Severity {
	case SeverityCritical:
		return "high"
	case SeverityHigh:
		return "high"
	case SeverityMedium:
		return "medium"
	case SeverityLow:
		return "low"
	default:
		return "low"
	}
}

// TicketType maps a source to the Peppermint ticket type field.
func (e *Event) TicketType() string {
	if e.Type != "" {
		return e.Type
	}
	return string(e.Source)
}

// Normalize trims whitespace from free-text fields and ensures required ones
// are populated. It returns an error when the event is unusable (no title).
func (e *Event) Normalize() error {
	e.Title = strings.TrimSpace(e.Title)
	e.Detail = strings.TrimSpace(e.Detail)
	e.From = strings.TrimSpace(e.From)
	e.Company = strings.TrimSpace(e.Company)
	e.Email = strings.TrimSpace(e.Email)
	if e.Title == "" {
		return fmt.Errorf("event title is required")
	}
	if e.Source == "" {
		return fmt.Errorf("event source is required")
	}
	if e.Severity == "" {
		e.Severity = SeverityMedium
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now().UTC()
	}
	return nil
}
