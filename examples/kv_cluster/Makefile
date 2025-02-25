all: build_clerk build_kv_node

.PHONY: build_clerk
build_clerk: ./cmd/clerk/clerk.go
	go build ./cmd/clerk

.PHONY: build_kv_node
build_kv_node: ./cmd/kv_node/kv_node.go
	go build ./cmd/kv_node

.PHONY: clean
clean:
	rm clerk kv_node
