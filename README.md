# walstream

`walstream` is a Change Data Capture (CDC) tool for PostgreSQL that captures changes from the Write-Ahead Log (WAL) and streams them over a gRPC connection.

## Architecture

```
+------------------+      +---------------+      +----------------+
|                  |      |               |      |                |
|    PostgreSQL    +----->+   walstream   +----->+  gRPC Clients  |
| (Logical Decode) |      | (gRPC Server) |      |    (Any app)   |
|                  |      |               |      |                |
+------------------+      +---------------+      +----------------+
```

`walstream` connects to a PostgreSQL replication slot, receives logical replication messages, and exposes them as a gRPC stream.

## Prerequisites

To run `walstream`, you need a PostgreSQL database with the following:

1.  **`wal_level` set to `logical`**: This setting enables logical decoding. You can set this in your `postgresql.conf` file or as a command-line flag when starting PostgreSQL. The provided `docker-compose.yml` handles this automatically.

    ```yml
    services:
      db:
        image: postgres
        command: ["postgres", "-c", "wal_level=logical"]
        # ...
    ```

2.  **A replication publication**: You need to create a publication on the tables you want to monitor. To capture changes from all tables, you can run:

    ```sql
    CREATE PUBLICATION my_publication FOR ALL TABLES;
    ```

    The `docker-entrypoint-initdb.d/create-publication.sh` script in this repository creates a publication named `publication` when using the provided Docker Compose setup.

## Getting Started

The easiest way to get started is by using the provided Docker Compose file:

```bash
docker-compose up -d
```

This will start a PostgreSQL database and the `walstream` service. The `walstream` service will be available on port `50051`.

## Running with Docker

Pre-built Docker images are available on GitHub Container Registry.

You can pull the latest image with:

```bash
docker pull ghcr.io/msavela/walstream:latest
```

Then, you can run it with the following command. Make sure to replace the connection string with your own.

```bash
docker run -p 50051:50051 ghcr.io/msavela/walstream:latest \
  --connection "host=my_postgres_host user=my_user password=my_password dbname=my_db" \
  start --publication my_publication --slot my_slot
```

## CLI Commands

`walstream` provides a CLI for managing replication slots and starting the gRPC server.

```
USAGE:
    walstream [OPTIONS] <COMMAND>

OPTIONS:
    --connection <CONNECTION>  Connection string [env: CONNECTION]

COMMANDS:
    start   Start/create replication slot and gRPC server
    list    List replication slots
    delete  Delete a permanent replication slot
```

### `start`

Starts the gRPC server and begins streaming WAL changes.

```
USAGE:
    walstream start [OPTIONS] --publication <PUBLICATION> --slot <SLOT>

OPTIONS:
        --publication <PUBLICATION>    Publication name
        --slot <SLOT>                  Replication slot name
    -p, --port <PORT>                  Custom port [default: 50051]
    -h, --host <HOST>                  Custom host [default: 0.0.0.0]
    -t, --temporary <TEMPORARY>        Use a temporary replication slot [default: true]
```

**Examples:**

- **Temporary slot (default behavior):** This creates a slot that is automatically dropped when the `walstream` client disconnects. This is useful for ephemeral consumers that care about real-time events only.

  ```bash
  walstream start --publication publication --slot my_temporary_slot
  ```

- **Permanent slot:** A permanent slot persists even if `walstream` disconnects and clients are guaranteed to receive all events. **Warning:** If data is not consumed from a permanent slot, it will accumulate on the PostgreSQL server, potentially filling up disk space. Ensure you have a consumer actively reading from a permanent slot.

  ```bash
  walstream start --publication publication --slot my_permanent_slot --temporary false
  ```

### `list`

Lists existing replication slots on the PostgreSQL server.

### `delete`

Deletes a permanent replication slot.

```
USAGE:
    walstream delete --slot <SLOT>
```

### Client Acknowledgment

To ensure that PostgreSQL can recycle WAL (Write-Ahead Log) files and to prevent the server's disk from filling up, `walstream` requires clients to acknowledge the messages they have processed.

When a client receives and processes a message, it must send a `ClientAck` message back to the server, containing the `pg_lsn` (PostgreSQL Log Sequence Number) of the message it just handled.

This is especially critical for **permanent replication slots**. If a client using a permanent slot stops consuming messages or fails to send acknowledgments, the WAL files will accumulate on the PostgreSQL server indefinitely, which can lead to running out of disk space.

While less critical for temporary slots (as they are removed on disconnect), sending acknowledgments is a best practice for all clients. All provided client examples in this repository demonstrate how to implement this acknowledgment mechanism.

## Client Examples

This repository includes several client examples in different languages to demonstrate how to connect to the `walstream` gRPC server:

- [Rust](./examples/rust-client/)
- [Go](./examples/go-client/)
- [TypeScript](./examples/ts-client/)
- [Python](./examples/python-client/)
- [JavaScript](./examples/js-client/)

Each example directory contains a `README.md` with instructions on how to run it.

## License

This project is licensed under the MIT License.
