.PHONY: proto build test lint up down rootfs

PROTO_DIR := proto
PROTO_FILE := $(PROTO_DIR)/aegis/v1/sandbox.proto
GO_PB_DIR := control-go/pkg/pb
GO_MODULE_PB := github.com/aegisrun/aegisrun/control-go/pkg/pb

proto:
	@mkdir -p $(GO_PB_DIR)
	protoc -I $(PROTO_DIR) \
		--go_out=$(GO_PB_DIR) \
		--go_opt=Maegis/v1/sandbox.proto=$(GO_MODULE_PB) \
		--go_opt=module=$(GO_MODULE_PB) \
		--go-grpc_out=$(GO_PB_DIR) \
		--go-grpc_opt=Maegis/v1/sandbox.proto=$(GO_MODULE_PB) \
		--go-grpc_opt=module=$(GO_MODULE_PB) \
		$(PROTO_FILE)
	cargo check --manifest-path daemon-rs/Cargo.toml

build:
	cargo build --manifest-path daemon-rs/Cargo.toml
	cd control-go && go build ./...

test:
	cargo test --manifest-path daemon-rs/Cargo.toml
	cd control-go && go test -v -race ./...

lint:
	cargo fmt --manifest-path daemon-rs/Cargo.toml -- --check
	cargo clippy --manifest-path daemon-rs/Cargo.toml -- -D warnings
	cd control-go && go vet ./...

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down

rootfs:
	sudo ./deploy/scripts/build-rootfs.sh

