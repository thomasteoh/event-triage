package triage

import (
	"testing"
	"time"
)

func TestEvaluateCreate(t *testing.T) {
	eng := NewEngine(time.Hour, 100)
	ev := &Event{Source: SourceEmail, Type: "support_email", Title: "Help with pipeline"}
	dec, err := eng.Evaluate(ev)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if dec != DecisionCreate {
		t.Fatalf("want create, got %s", dec)
	}
}

func TestEvaluateDuplicate(t *testing.T) {
	eng := NewEngine(time.Hour, 100)
	ev := &Event{Source: SourceEmail, Type: "support_email", Title: "Same issue"}
	_, _ = eng.Evaluate(ev)
	dec, err := eng.Evaluate(ev)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if dec != DecisionDuplicate {
		t.Fatalf("want duplicate, got %s", dec)
	}
}

func TestEvaluateDuplicateExpires(t *testing.T) {
	eng := NewEngine(time.Minute, 100)
	ev := &Event{Source: SourceEmail, Type: "support_email", Title: "Expiring"}
	_, _ = eng.Evaluate(ev)
	// Simulate window expiry by directly manipulating the seen map.
	eng.mu.Lock()
	eng.seen[ev.FingerprintKey()] = time.Now().UTC().Add(-2 * time.Minute)
	eng.mu.Unlock()
	dec, err := eng.Evaluate(ev)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if dec != DecisionCreate {
		t.Fatalf("want create after expiry, got %s", dec)
	}
}

func TestEvaluateRateLimit(t *testing.T) {
	eng := NewEngine(time.Hour, 1)
	// Distinct titles → distinct fingerprints, so both reach the rate limiter.
	_, _ = eng.Evaluate(&Event{Source: SourcePipeline, Type: "jobs_failed", Title: "job-1"})
	dec, err := eng.Evaluate(&Event{Source: SourcePipeline, Type: "jobs_failed", Title: "job-2"})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if dec != DecisionRateLimit {
		t.Fatalf("want rate_limit, got %s", dec)
	}
}

func TestEvaluateNormalize(t *testing.T) {
	eng := NewEngine(time.Hour, 100)
	// Missing title → error (fail closed).
	ev := &Event{Source: SourceEmail}
	_, err := eng.Evaluate(ev)
	if err == nil {
		t.Fatal("want error for missing title")
	}
}

func TestTicketPriority(t *testing.T) {
	cases := []struct {
		sev      Severity
		override string
		want     string
	}{
		{SeverityCritical, "", "high"},
		{SeverityHigh, "", "high"},
		{SeverityMedium, "", "medium"},
		{SeverityLow, "", "low"},
		{SeverityLow, "urgent", "urgent"},
	}
	for _, c := range cases {
		e := &Event{Severity: c.sev, Priority: c.override}
		if got := e.TicketPriority(); got != c.want {
			t.Errorf("severity=%s override=%s: want %s, got %s", c.sev, c.override, c.want, got)
		}
	}
}
