.PHONY: all build clean install test proto

BINARY_NAME=protoc-gen-eventapi
BIN_DIR=./bin
CMD_DIR=./cmd/protoc-gen-eventapi

all: build

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BIN_DIR)
	go build  -ldflags="-s -w" -o $(BIN_DIR)/$(BINARY_NAME) $(CMD_DIR)
	@echo "Build complete: $(BIN_DIR)/$(BINARY_NAME)"

clean:
	@echo "Cleaning..."
	@rm -rf $(BIN_DIR)
	@echo "Clean complete"

install: build
	@echo "Installing $(BINARY_NAME) to GOPATH/bin..."
	@cp $(BIN_DIR)/$(BINARY_NAME) $(GOPATH)/bin/
	@echo "Install complete"

test:
	@echo "Running tests..."
	go test ./...

proto:
	@echo "Generating Go code from proto files..."
	protoc --go_out=. --go_opt=paths=source_relative \
		-I. proto/eventapi/v1/eventapi.proto

generate: build
	@echo "Generating test client code..."
	@mkdir -p example/user
	$(BIN_DIR)/$(BINARY_NAME) \
		--plugin=./bin/$(BINARY_NAME) \
		--eventapi_out=example/user \
		--eventapi_opt=paths=source_relative \
		--go_out=example/user \
		--go_opt=paths=source_relative \
		-I. \
		./cmd/protoc-gen-eventapi/internal/test/testdata/user.proto
	@echo "Generation complete"

fmt:
	goimports -w . .
	gofmt -w -s .
	buf format proto -w -v
