// Package triage also provides the decision engine that normalizes, dedups,
// and rate-limits events before they become Peppermint tickets. It is the
// single place where event → ticket decisions are made.
package triage

import (
	"sync"
	"time"
)

// Engine applies dedup (fingerprint within window) and per-source rate limits
// to events. It returns a decision: create the ticket, skip as duplicate, or
// skip as rate-limited.
type Engine struct {
	window   time.Duration // dedup window
	rateDay  int           // max events per source per day
	mu       sync.Mutex
	seen     map[string]time.Time // fingerprint → first-seen time
	rate     map[Source]int       // source → count today
	rateDate string               // current date (resets rate on change)
}

// Decision is the outcome of triage.
type Decision string

const (
	DecisionCreate   Decision = "create"   // create a ticket
	DecisionDuplicate Decision = "duplicate" // suppress (already seen in window)
	DecisionRateLimit Decision = "rate_limit" // suppress (source over daily cap)
)

// NewEngine creates an Engine.
func NewEngine(window time.Duration, rateDay int) *Engine {
	return &Engine{
		window:  window,
		rateDay: rateDay,
		seen:    make(map[string]time.Time),
		rate:    make(map[Source]int),
	}
}

// Evaluate decides whether an event becomes a ticket. It is safe for
// concurrent use. The dedup fingerprint and daily rate are tracked in-memory
// (the bridge is a single instance; a restart resets them, acceptable for
// triage — duplicate suppression is best-effort, not durable).
func (e *Engine) Evaluate(ev *Event) (Decision, error) {
	if err := ev.Normalize(); err != nil {
		return "", err
	}
	fp := ev.FingerprintKey()

	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now().UTC()
	today := now.Format("2006-01-02")
	if e.rateDate != today {
		e.rateDate = today
		e.rate = make(map[Source]int)
	}

	// Dedup: same fingerprint within window → duplicate.
	if first, ok := e.seen[fp]; ok && now.Sub(first) <= e.window {
		return DecisionDuplicate, nil
	}
	e.seen[fp] = now
	// Bound the seen map so it doesn't grow unbounded.
	if len(e.seen) > 10000 {
		for k, t := range e.seen {
			if now.Sub(t) > e.window {
				delete(e.seen, k)
			}
		}
	}

	// Rate limit: per-source daily cap.
	if e.rate[ev.Source] >= e.rateDay {
		return DecisionRateLimit, nil
	}
	e.rate[ev.Source]++

	return DecisionCreate, nil
}
