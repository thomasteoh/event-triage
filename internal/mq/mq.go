// Package mq is the RabbitMQ integration for the event-triage bridge.
// It consumes normalized events from the existing mq.harmonicr.com broker
// (vhost "messaging") on the events.* queues and publishes parsed emails
// back onto the same bus. It uses amqp091-go, the maintained RabbitMQ client.
package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
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

// Config holds RabbitMQ connection settings. The broker URL carries no
// credentials; authentication uses an OAuth2 JWT fetched from TokenProvider
// and presented as the SASL PLAIN password.
type Config struct {
	URL          string        // amqp://<host>:<port>/<vhost> (no credentials)
	ConsumerTag  string        // e.g. "event-triage"
	Prefetch     int
	Reconnect    time.Duration // backoff between reconnect attempts
	TokenProvider TokenProvider // fetches/refreshes the OAuth2 JWT
}

// TokenProvider supplies an OAuth2 client-credentials JWT for the broker. The
// JWT is presented as the SASL PLAIN password (RabbitMQ's oauth2 backend reads
// it from the password field). It must refresh the token when it expires.
type TokenProvider interface {
	// Token returns a current, valid JWT. Implementations must refresh it
	// when it nears expiry so reconnect loops never present a stale token.
	Token() (string, error)
}

// OAuthTokenProvider fetches a client-credentials JWT from a Zitadel token
// endpoint (or any OAuth2 issuer) and caches it until it nears expiry.
type OAuthTokenProvider struct {
	ClientID     string
	ClientSecret string
	TokenURL     string
	Scope        string
	HTTPClient   *http.Client

	mu      sync.Mutex
	token   string
	expiry  time.Time
}

// NewOAuthTokenProvider builds a token provider for the given credentials.
// The client secret is held in memory only — never logged, never written.
func NewOAuthTokenProvider(clientID, clientSecret, tokenURL, scope string) *OAuthTokenProvider {
	return &OAuthTokenProvider{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     tokenURL,
		Scope:        scope,
		HTTPClient:   &http.Client{Timeout: 15 * time.Second},
	}
}

// Token returns a valid JWT, fetching and caching one when needed.
func (p *OAuthTokenProvider) Token() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Refresh when the token is missing or within 60s of expiry.
	if p.token != "" && time.Now().Before(p.expiry.Add(-60*time.Second)) {
		return p.token, nil
	}
	tok, exp, err := p.fetch()
	if err != nil {
		return "", err
	}
	p.token = tok
	p.expiry = exp
	return tok, nil
}

func (p *OAuthTokenProvider) fetch() (string, time.Time, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", p.ClientID)
	form.Set("client_secret", p.ClientSecret)
	if p.Scope != "" {
		form.Set("scope", p.Scope)
	}
	req, err := http.NewRequest(http.MethodPost, p.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HTTPClient.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("fetch token: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("read token response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("token endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var tr struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", time.Time{}, fmt.Errorf("decode token response: %w", err)
	}
	if tr.AccessToken == "" {
		return "", time.Time{}, fmt.Errorf("token response missing access_token")
	}
	exp := time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	return tr.AccessToken, exp, nil
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
	conn, err := c.dial()
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
		// RabbitMQ requires a unique consumer tag per queue on a channel.
		// Derive one from the configured tag + queue name so reconnects do
		// not collide with a tag already in use on another queue.
		tag := fmt.Sprintf("%s-%s", c.cfg.ConsumerTag, q)
		d, err := ch.Consume(q, tag, false, false, false, false, nil)
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
	url           string
	tokenProvider TokenProvider
}

// NewPublisher creates a Publisher for the given broker URL and token provider.
// The token provider supplies the OAuth2 JWT used as the SASL PLAIN password.
func NewPublisher(url string, tokenProvider TokenProvider) *Publisher {
	return &Publisher{url: url, tokenProvider: tokenProvider}
}

// Publish sends an event to the named queue. It fails closed on any error.
func (p *Publisher) Publish(ctx context.Context, queue string, e triage.Event) error {
	if err := e.Normalize(); err != nil {
		return fmt.Errorf("publish normalize: %w", err)
	}
	tok, err := p.tokenProvider.Token()
	if err != nil {
		return fmt.Errorf("fetch oauth token: %w", err)
	}
	conn, err := amqp.DialConfig(p.url, amqp.Config{
		SASL: []amqp.Authentication{
			&amqp.PlainAuth{Username: tok, Password: tok},
		},
	})
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

// dial opens a RabbitMQ connection authenticated with an OAuth2 JWT. The JWT
// is fetched from the token provider and presented as the SASL PLAIN password
// (RabbitMQ's oauth2 backend reads the token from the password field).
func (c *Consumer) dial() (*amqp.Connection, error) {
	tok, err := c.cfg.TokenProvider.Token()
	if err != nil {
		return nil, fmt.Errorf("fetch oauth token: %w", err)
	}
	return amqp.DialConfig(c.cfg.URL, amqp.Config{
		SASL: []amqp.Authentication{
			&amqp.PlainAuth{Username: tok, Password: tok},
		},
	})
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
