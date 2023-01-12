.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -o conduit-connector-sap-hana cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) ./...

lint:
	golangci-lint run --config .golangci.yml

mockgen:
	mockgen -package mock -source destination/interface.go -destination destination/mock/destination.go
