// Package mq is the RabbitMQ integration for the event-triage bridge.
// It consumes normalized events from the existing mq.harmonicr.com broker
// (vhost "messaging") on the events.* queues and publishes parsed emails
// back onto the same bus. It uses amqp091-go, the maintained RabbitMQ client.
package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/thomasteoh/event-triage/internal/triage"
)

// Queue names. Each source publishes to its own queue; the bridge consumes all
// of them and routes to Peppermint.
const (
	QueueEmail    = "events.email"
	QueuePipeline = "events.pipeline"
	QueueLog      = "events.log"
	QueueHealth   = "events.health"
)

// AllQueues lists every queue the bridge consumes.
var AllQueues = []string{QueueEmail, QueuePipeline, QueueLog, QueueHealth}

// Config holds RabbitMQ connection settings.
type Config struct {
	URL         string        // amqp://<user>:<pass>@<host>:<port>/<vhost>
	ConsumerTag string        // e.g. "event-triage"
	Prefetch    int
	Reconnect   time.Duration // backoff between reconnect attempts
}

// Consumer reads events from the queues and calls handle for each message.
// It auto-reconnects on channel/connection errors and re-queues on handler
// failure so no event is silently lost.
type Consumer struct {
	cfg     Config
	handle  func(context.Context, triage.Event) error
	logger  *log.Logger
	conn    *amqp.Connection
	ch      *amqp.Channel
	closed  chan struct{}
	mu      sync.RWMutex // guards conn/ch for Ready()
}

// NewConsumer creates a Consumer. handle is called for every consumed event.
func NewConsumer(cfg Config, handle func(context.Context, triage.Event) error, logger *log.Logger) *Consumer {
	return &Consumer{
		cfg:    cfg,
		handle: handle,
		logger: logger,
		closed: make(chan struct{}),
	}
}

// Run connects and consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		if err := c.connectAndConsume(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Printf("mq: consumer error, reconnecting in %s: %v", c.cfg.Reconnect, err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.cfg.Reconnect):
			}
			continue
		}
		select {
		case <-ctx.Done():
			c.close()
			return ctx.Err()
		case <-c.closed:
			c.close()
			return nil
		}
	}
}

func (c *Consumer) connectAndConsume(ctx context.Context) error {
	conn, err := amqp.Dial(c.cfg.URL)
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Qos(c.cfg.Prefetch, 0, false); err != nil {
		_ = conn.Close()
		return fmt.Errorf("qos: %w", err)
	}
	// Declare the queues (idempotent) so the bridge works against a fresh
	// vhost without manual queue creation.
	for _, q := range AllQueues {
		if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			_ = conn.Close()
			return fmt.Errorf("declare %s: %w", q, err)
		}
	}

	deliveries := make(chan amqp.Delivery)
	for _, q := range AllQueues {
		d, err := ch.Consume(q, c.cfg.ConsumerTag, false, false, false, false, nil)
		if err != nil {
			_ = conn.Close()
			return fmt.Errorf("consume %s: %w", q, err)
		}
		// Fan deliveries from all queues into one channel.
		go func() {
			for msg := range d {
				deliveries <- msg
			}
		}()
	}

	c.mu.Lock()
	c.conn = conn
	c.ch = ch
	c.mu.Unlock()
	defer c.close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closed:
			return nil
		case msg, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}
			if err := c.process(ctx, msg); err != nil {
				c.logger.Printf("mq: process failed (requeue): %v", err)
				if nackErr := msg.Nack(false, true); nackErr != nil {
					c.logger.Printf("mq: nack failed: %v", nackErr)
				}
				continue
			}
			if err := msg.Ack(false); err != nil {
				c.logger.Printf("mq: ack failed: %v", err)
			}
		}
	}
}

func (c *Consumer) process(ctx context.Context, msg amqp.Delivery) error {
	var e triage.Event
	if err := json.Unmarshal(msg.Body, &e); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}
	if err := e.Normalize(); err != nil {
		return fmt.Errorf("normalize event: %w", err)
	}
	return c.handle(ctx, e)
}

// Publisher publishes an event to a queue. It opens a fresh connection per
// publish (the bridge publishes parsed emails and test events; low volume).
type Publisher struct {
	url string
}

// NewPublisher creates a Publisher for the given broker URL.
func NewPublisher(url string) *Publisher {
	return &Publisher{url: url}
}

// Publish sends an event to the named queue. It fails closed on any error.
func (p *Publisher) Publish(ctx context.Context, queue string, e triage.Event) error {
	if err := e.Normalize(); err != nil {
		return fmt.Errorf("publish normalize: %w", err)
	}
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", queue, err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         b,
	}); err != nil {
		return fmt.Errorf("publish %s: %w", queue, err)
	}
	return nil
}

func (c *Consumer) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

// Ready reports whether the consumer currently has a live connection. It is
// used by the HTTP readiness probe; false means the broker is unreachable and
// the service should not advertise ready.
func (c *Consumer) Ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil && !c.conn.IsClosed()
}
