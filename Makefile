.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-sap-hana.version=${VERSION}'" -o conduit-connector-sap-hana cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) ./...

lint:
	golangci-lint run --config .golangci.yml

paramgen:
	paramgen -path=./destination -output=destination_params.go Config
	paramgen -path=./source -output=source_params.go Config
