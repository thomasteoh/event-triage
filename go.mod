module github.com/thomasteoh/event-triage

// Pin a patch release that fixes known stdlib CVEs (GO-2026-6218, 6090,
// 6089, 5972, 5026, 6809). The Go toolchain directive ensures builds use >= 1.26.10.
go 1.26.10

require (
	github.com/coreos/go-oidc/v3 v3.21.0
	github.com/rabbitmq/amqp091-go v1.14.0
)

require (
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
)
