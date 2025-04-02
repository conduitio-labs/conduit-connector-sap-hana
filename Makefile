VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build 
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-sap-hana.version=${VERSION}'" -o conduit-connector-sap-hana cmd/connector/main.go

.PHONY: test
test:
	go test $(GOTEST_FLAGS) -race ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: generate
generate: paramgen mockgen
	go generate ./...

.PHONY: paramgen
paramgen:
	paramgen -path=./destination -output=destination_params.go Config
	paramgen -path=./source -output=source_params.go Config

.PHONY: mockgen
mockgen:
	mockgen -package mock -source destination/interface.go -destination destination/mock/destination.go
	mockgen -package mock -source source/interface.go -destination source/mock/iterator.go

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools/go.mod
	@go list -modfile=tools/go.mod tool | xargs -I % go list -modfile=tools/go.mod -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy
