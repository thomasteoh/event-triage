# harmonicr event-triage

A unified event-triage bridge for the harmonicr infrastructure. It receives
support email and pipeline/log/health events, normalizes and dedups them, and
creates Peppermint tickets so all incidents land in one helpdesk.

It is a receiver/relay only — it holds no mailbox and no event store. Every
inbound event becomes (or is suppressed as) a ticket; nothing is persisted
beyond the Peppermint ticket.

## Why

Harmonicr runs several sources that generate operational noise — support mail,
Databricks pipeline failures, log-service errors, server health probes. Without
a triage layer each source needs its own alerting and its own human to notice.
event-triage funnels them into one ticket desk (Peppermint) with dedup and
rate limiting so humans see signal, not spam.

## Sources

| Source | RabbitMQ queue | Peppermint type | Notes |
|---|---|---|---|
| Support email | `events.email` | `support_email` | Inbound SMTP (TLS 5225) from Google Workspace routing |
| Databricks pipeline | `events.pipeline` | `jobs_failed` | Webhook → publisher |
| Log service | `events.log` | `log_error` | Collector → publisher |
| Server health | `events.health` | `health` | Probe → publisher |

All sources publish the normalized `Event` JSON shape to their queue on the
bridge's own `messaging` vhost. The bridge consumes all four queues, runs
triage (normalize → dedup → rate-limit), and calls Peppermint's public create
endpoint over the internal container network.

> **Broker note:** the bridge uses a **dedicated plain-auth RabbitMQ broker**
> (`event-triage-rabbitmq`, vhost `messaging`, user `mq`) deployed alongside it
> on the `zitadel` network. The Zulip broker is OAuth2-only (via Zitadel) and
> its `rabbitmq_auth_backend_oauth2` plugin forces `auth_backends = [oauth2]`,
> so a plain backend cannot coexist there. This broker is isolated from Zulip.

## Build

```sh
go build ./...
go test ./...
go vet ./...
```

## Deploy

The image is built by GH Actions (`build-and-publish`) and pushed to GHCR on
tag. Never build/upload locally. Pull the image on the solo VM and run it as a
Podman container (see `docs/projects/infrastructure/event-triage-service-plan.md`).

## Config

All configuration is via environment variables (secrets never committed):

| Var | Required | Default | Meaning |
|---|---|---|---|
| `EVENT_TRIAGE_RABBITMQ_URL` | yes | — | `amqp://mq:<pw>@rabbitmq-mq:5672/messaging` |
| `EVENT_TRIAGE_SMTP_TLS_CERT` | yes | — | CA-signed cert path (SMTP 5225) |
| `EVENT_TRIAGE_SMTP_TLS_KEY` | yes | — | Private key path |
| `EVENT_TRIAGE_PEPPERMINT_URL` | yes | — | `http://peppermint:5003` (internal only) |
| `EVENT_TRIAGE_PEPPERMINT_KEY` | no | — | Optional X-API-Key for the create endpoint |
| `EVENT_TRIAGE_PEPPERMINT_USER` | no | `event-triage` | createdBy label |
| `EVENT_TRIAGE_QUEUE_PREFIX` | no | `events` | Queue prefix |
| `EVENT_TRIAGE_DEDUP_WINDOW` | no | `1h` | Dedup window |
| `EVENT_TRIAGE_RATE_DAY` | no | `100` | Max tickets per source per day |
| `EVENT_TRIAGE_SMTP_ADDR` | no | `:5225` | SMTP listener address |
| `EVENT_TRIAGE_LISTEN_ADDR` | no | `:8080` | Health/readiness address |

## Endpoints

- `GET /healthz` — process liveness.
- `GET /readyz` — readiness (RabbitMQ + Peppermint reachable).
- `GET /admin` — OIDC-gated (Zitadel). Fails closed (403) without OIDC config.
