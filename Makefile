.PHONY: all build clean install test proto generate fmt

BIN_DIR=./bin
CMD_DIR=./cmd

all: build

build:
	@echo "Building ..."
	@mkdir -p $(BIN_DIR)
	go build  -ldflags="-s -w" -o $(BIN_DIR) $(CMD_DIR)/...
	@echo "Build complete: $(BIN_DIR)"

clean:
	@echo "Cleaning..."
	@rm -rf $(BIN_DIR)
	@echo "Clean complete"

test:
	@echo "Running tests..."
	go test ./...

install:
	go install -ldflags="-s -w" $(CMD_DIR)/protoc-gen-eventapi@latest
	go install -ldflags="-s -w" $(CMD_DIR)/protoc-gen-eventapi-sink@latest

generate: build
	export PATH="$(BIN_DIR):$$PATH" && buf generate

	fmt:
	goimports -w . .
	gofmt -w -s .
	buf format proto examples/proto -w -v
