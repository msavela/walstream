FROM rust:1-bullseye AS builder

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libpq-dev \
    protobuf-compiler \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./

# Empty main.rs to build dependencies on a separate layer
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

# Copy actual source code
COPY src ./src
COPY proto ./proto
COPY build.rs ./

RUN cargo build --release

RUN strip target/release/walstream

RUN cp target/release/walstream /walstream-$TARGETARCH

FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y \
    libpq5 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /walstream-$TARGETARCH /usr/local/bin/walstream

ENTRYPOINT ["/usr/local/bin/walstream"]
CMD ["--help"]
