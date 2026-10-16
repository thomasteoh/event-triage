.PHONY: build test vet race fmt run

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

race:
	go test -race ./...

fmt:
	go fmt ./...

run: build
	./event-triage serve
