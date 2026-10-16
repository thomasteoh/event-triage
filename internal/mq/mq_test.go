package mq

import (
	"context"
	"testing"

	"github.com/thomasteoh/event-triage/internal/triage"
)

func TestAllQueues(t *testing.T) {
	want := []string{"events.email", "events.pipeline", "events.log", "events.health"}
	got := AllQueues
	if len(got) != len(want) {
		t.Fatalf("queue count: want %d, got %d", len(want), len(got))
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("queue[%d]: want %s, got %s", i, w, got[i])
		}
	}
}

func TestPublisherRejectsUnnormalizableEvent(t *testing.T) {
	// No live broker needed: the publish path normalizes first and fails
	// closed on a missing title before dialing.
	p := NewPublisher("amqp://mq:pw@127.0.0.1:5672/messaging")
	err := p.Publish(context.Background(), QueueEmail, triage.Event{Source: triage.SourceEmail})
	if err == nil {
		t.Fatal("want error for event with empty title (fail closed before dial)")
	}
}

func TestPublisherDialFailure(t *testing.T) {
	// Unreachable broker → publish fails closed with a dial error.
	p := NewPublisher("amqp://mq:pw@127.0.0.1:1/messaging")
	err := p.Publish(context.Background(), QueueEmail, triage.Event{
		Source: triage.SourceEmail,
		Title:  "test",
	})
	if err == nil {
		t.Fatal("want dial error for unreachable broker")
	}
}
