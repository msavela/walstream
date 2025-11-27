# Go gRPC Client Example

This example demonstrates a simple gRPC client in Go that connects to the stream server.

## Prerequisites

- Go (golang.org/dl/)
- Protocol Buffers compiler (protoc)

## Running the client

1. Compile the Protocol Buffers definitions:
   ```bash
   ./compile_protobuf.sh
   ```

2. Run the client:
   ```bash
   go run main.go
   ```
