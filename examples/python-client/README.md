# Python gRPC Client Example

This example demonstrates a simple gRPC client in Python.

## Prerequisites

- Python 3.9+
- [uv](https://github.com/astral-sh/uv)

## Running

1.  Setup environment and install dependencies:
    ```bash
    uv venv
    source .venv/bin/activate
    uv pip install grpcio grpcio-tools protobuf
    ```

2.  Compile protobufs:
    ```bash
    ./compile_protobuf.sh
    ```

3.  Run:
    ```bash
    python main.py
    ```