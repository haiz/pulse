# Contributing

How to set up the dev environment, understand the code, and submit changes.

## Dev Setup

```bash
# Clone and build
git clone <repository-url> pulse
cd pulse
cargo build --workspace

# Run tests (should all pass)
cargo test --workspace

# Run lints
cargo clippy --workspace -- -D warnings
cargo fmt --all -- --check
```

## Project Structure

```
pulse/
├── crates/                    # Rust workspace crates
│   ├── pulse-protocol/        # Wire protocol (no internal deps)
│   ├── pulse-broker/          # Broker server
│   │   ├── src/
│   │   │   ├── server/        # TCP/TLS listener, connection handler
│   │   │   ├── pipeline/      # Ingest: dedup, WAL, batch processing
│   │   │   ├── routing/       # TopicTrie, content filters, transforms
│   │   │   ├── delivery/      # ACK tracker, retry, DLQ, consumer groups
│   │   │   ├── storage/       # WAL segments, state DB, compaction
│   │   │   ├── auth/          # API key auth, permissions
│   │   │   ├── metrics/       # Prometheus exporter
│   │   │   └── namespace/     # Namespace isolation
│   │   ├── tests/             # Integration tests
│   │   └── benches/           # Criterion benchmarks
│   ├── pulse-cluster/         # Gossip, consistent hash, replication
│   ├── pulse-sdk/             # Rust client SDK
│   ├── pulse-gateway/         # HTTP/WS gateway
│   ├── pulse-admin/           # Admin CLI
│   ├── pulse-ffi/             # C ABI
│   └── pulse-demo/            # E2E demo system
├── sdks/                      # Foreign language SDKs
│   ├── python/                # PyO3 bindings
│   ├── typescript/            # TypeScript/Node HTTP+WS client
│   └── go/                    # Go HTTP client
├── config/                    # Example config files
├── demo/                      # Multi-language demo scripts
├── docker/                    # Dockerfile + docker-compose
└── docs/                      # Documentation
```

## Development Workflow

### Running the system locally

```bash
# Start broker + gateway + subscribers (demo mode)
cargo run -p pulse-demo

# Or start components separately
cargo run -p pulse-broker
cargo run -p pulse-gateway -- --broker 127.0.0.1:4222
```

### Running tests

```bash
# All tests
cargo test --workspace

# Specific crate
cargo test -p pulse-protocol
cargo test -p pulse-broker

# Single test
cargo test -p pulse-broker -- routing::engine::tests::exact_match

# Integration tests only
cargo test -p pulse-broker --test integration
```

### Running benchmarks

```bash
# All benchmarks
cargo bench -p pulse-broker

# Specific benchmark group
cargo bench -p pulse-broker -- "ingest"
cargo bench -p pulse-broker -- "bloom_filter"
cargo bench -p pulse-broker -- "topic_trie"
cargo bench -p pulse-broker -- "batch_pipeline"
```

### Running the demo

```bash
# Start system
cargo run -p pulse-demo

# In another terminal, run multi-language services
./demo/run.sh

# Or run load test
python3 demo/load_test.py
```

## Code Conventions

### Module size

Keep modules under 1000 LOC with single responsibility. Current largest: `wal.rs` (~900 LOC). If a file grows beyond 1000 LOC, split it.

### Error handling

- Use `BrokerError` enum for broker internals
- Use `PulseError` enum for SDK
- Use `thiserror` for error types
- Return `Result<T, E>` — no panics in library code

### Testing

- Every public function should have at least one test
- Integration tests in `tests/` directory
- Use `tempfile::tempdir()` for test state — never write to fixed paths
- Use `BrokerConfig::for_testing()` to get sane test defaults

### Naming

- Crates: `pulse-{name}` (kebab-case)
- Modules: `snake_case`
- Types: `PascalCase`
- Topic patterns in tests: `"test.topic"`, `"order.*"`, etc.

## CI Checks

All of these must pass before merge:

```bash
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --all -- --check
```

## Adding a New Feature

1. Create or update tests first
2. Implement the feature
3. Run `cargo test --workspace && cargo clippy --workspace -- -D warnings`
4. Update docs if the feature is user-facing
5. Add a benchmark if the feature is performance-sensitive

## Adding a New Language SDK

SDKs use the HTTP/WS gateway — no protocol implementation needed:

1. Create `sdks/{language}/` directory
2. Implement: `publish(topic, data)`, `subscribe(topic, handler)`, health check
3. Publish and subscribe use the gateway at `http://localhost:8080`
4. Add demo script in `demo/`
5. Add section to `docs/integration-guide.md`

For native SDKs (FFI-based), wrap `pulse-ffi` and expose language-idiomatic API.
