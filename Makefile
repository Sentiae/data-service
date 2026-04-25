GO ?= go

.PHONY: build
build:
	$(GO) build ./cmd/... ./internal/...

.PHONY: test
test:
	$(GO) test ./...

# Proto: regenerate gRPC code from proto/data/v1/*.proto using buf.
# Output lands in gen/data/v1/ and is committed to the repo so callers
# don't need a buf toolchain to compile data-service.
.PHONY: proto
proto:
	buf generate proto
