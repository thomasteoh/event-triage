# event-triage — event triage service
# Multi-stage Go build, minimal scratch runtime (mirrors smtp-router).
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /event-triage ./cmd/event-triage

FROM scratch
# CA certificates are REQUIRED: the bridge verifies OIDC ID tokens against
# https://auth.harmonicr.com (Let's Encrypt cert) and calls Peppermint over
# HTTPS; without a CA bundle Go's x509 verifier rejects those certs.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /event-triage /event-triage
# Run as non-root (uid 10001 to match the harmonicr rootless pattern).
USER 10001:10001
EXPOSE 5225 8080
ENTRYPOINT ["/event-triage"]
