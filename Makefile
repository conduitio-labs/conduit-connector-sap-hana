.PHONY: build test lint paramgen mockgen download install-tools

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-sap-hana.version=${VERSION}'" -o conduit-connector-sap-hana cmd/connector/main.go


test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	golangci-lint run --config .golangci.yml

paramgen:
	paramgen -path=./destination -output=destination_params.go Config
	paramgen -path=./source -output=source_params.go Config

mockgen:
	mockgen -package mock -source destination/interface.go -destination destination/mock/destination.go
	mockgen -package mock -source source/interface.go -destination source/mock/iterator.go

download:
	@echo Download go.mod dependencies
	@go mod download

install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy
