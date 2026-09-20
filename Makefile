.PHONY: generate test lint security verify jev-smoke production-preflight

BUF := go run github.com/bufbuild/buf/cmd/buf@v1.47.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2
GOVULNCHECK := go run golang.org/x/vuln/cmd/govulncheck@v1.8.0

generate:
	$(BUF) generate

test:
	go test -race ./...

lint:
	$(BUF) lint
	go vet ./...
	$(GOLANGCI_LINT) run ./...

security:
	$(GOVULNCHECK) ./...

verify:
	$(BUF) lint
	./scripts/check-generated.sh
	go test -race ./...
	go vet ./...
	$(GOLANGCI_LINT) run ./...
	$(GOVULNCHECK) ./...

jev-smoke:
	@test -n "$${MEMJEV_JEV_API_KEY_FILE:-}" || (echo "MEMJEV_JEV_API_KEY_FILE is required" >&2; exit 2)
	@test -n "$${MEMJEV_JEV_MODEL:-}" || (echo "MEMJEV_JEV_MODEL is required" >&2; exit 2)
	go run ./cmd/jev-smoke -key-file "$${MEMJEV_JEV_API_KEY_FILE}" -model "$${MEMJEV_JEV_MODEL}"

production-preflight:
	./scripts/production-preflight.sh
