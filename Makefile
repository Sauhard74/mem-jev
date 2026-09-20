.PHONY: generate test lint verify

BUF := go run github.com/bufbuild/buf/cmd/buf@v1.47.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.0.2

generate:
	$(BUF) generate

test:
	go test -race ./...

lint:
	$(BUF) lint
	go vet ./...
	$(GOLANGCI_LINT) run ./...

verify:
	$(BUF) lint
	./scripts/check-generated.sh
	go test -race ./...
	go vet ./...
	$(GOLANGCI_LINT) run ./...
