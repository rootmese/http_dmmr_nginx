# DMMR Roadmap

## Current Status

### Core Features

- Native Nginx module
- Binary protocol between Nginx module and cache daemon
- Berkeley DB persistence
- TCP transport
- Unix Socket transport
- Configurable cache address via `dmmr_cache_addr`
- Persistent connection reuse
- Automatic reconnection
- API Key authentication
- Dynamic routing
- Rate limiting

---

# Phase 1 — Stabilization (Highest Priority)

## Memory Safety

- Eliminate buffer overflow risks
- Validate all packet lengths
- Strict bounds checking
- Safe string termination

---

## Binary Protocol Robustness

- Handle partial `recv()`
- Handle partial `send()`
- Correct header parsing
- Correct payload length handling
- Protect against malformed packets
- Gracefully reject invalid frames
- Never crash on malformed input

---

## Nginx Module Stability

- Validate persistent sockets before reuse
- Improve reconnection logic
- Remove blocking operations from the Nginx worker context
- Prepare an event-driven communication layer

---

## Authentication

- Remove static fallback authentication
- Externalize credentials
- Add optional shared secret between Nginx module and daemon

---

# Phase 2 — Runtime Architecture

## Runtime Separation

Separate runtime management from server implementation.

### Core Server

- Initialize memory pools
- Initialize Berkeley DB
- Start worker threads
- Start control threads
- Create listening sockets
- Execute listener loop
- Perform graceful shutdown

### Runtime Layer

Support multiple execution modes:

- Foreground
- Daemon (Unix double-fork)
- Container (Docker, systemd, Supervisor)

---

## Listener Abstraction

Provide interchangeable listener implementations.

### Default

- POSIX `select()`

### Optional

- Linux `epoll`
- BSD `kqueue`

The server core must remain independent of the underlying event notification mechanism.

---

## Connection Model

- Per-worker connections
- Optional connection pool
- Socket validation before reuse
- Idle timeout
- Keep-alive support

---

# Phase 3 — Performance

## Rate Limiter Refactor

Replace the current RBTree implementation.

Possible implementations:

- Hash table
- Time buckets
- Sliding window

Target complexity:

- O(1) average lookup
- O(1) eviction

---

## Cache Runtime Optimization

- Reduce socket churn
- Keep client connections persistent
- Idle connection cleanup
- Lower syscall overhead

---

## Binary Protocol Optimization

- Reduce memory copies
- Buffer reuse
- Batch operations where appropriate

---

# Phase 4 — Configuration

## Runtime Configuration

Support configuration through:

- Environment variables
- Configuration file (`dmmr_cache.conf`)

Configuration precedence:

1. Environment variables
2. Configuration file
3. Default values

Supported settings:

- TCP port
- Cluster port
- Unix socket path
- Berkeley DB path
- TTL
- Log level
- Worker count

---

# Phase 5 — Observability

## Health Interface

Commands:

- `PING`
- `STATUS`
- `STATS`

Requirements:

- Non-blocking
- Low latency
- Lightweight implementation

---

## Metrics

### Cache Daemon

Expose:

- Request count
- Request latency
- Cache hit ratio
- Cache miss ratio
- Error count
- Rate limit triggers
- Queue depth
- Active workers

### Nginx Module

Consume daemon metrics only.

The Nginx module must **never compute metrics itself**.

---

## Structured Logging

Support log levels:

- DEBUG
- INFO
- WARN
- ERROR

Optional:

- JSON logging

---

# Phase 6 — Cluster

## Peer Management

- Heartbeat mechanism
- Peer discovery
- Failure detection
- Retry with exponential backoff

Peer states:

- Active
- Degraded
- Offline

---

## Replication

Future replication support:

- Asynchronous replication
- Write propagation
- Replication acknowledgements
- Conflict handling

---

# Phase 7 — Protocol Evolution

## Frame Fragmentation (Chunking)

Support payloads larger than a single protocol frame.

Features:

- Message ID
- Sequence number
- FIRST / MORE / LAST flags
- Reassembly buffer
- Timeout cleanup
- Memory limits

---

## Protocol Versioning

Support multiple protocol versions.

Example:

- v1
- v2
- Future extensions

Maintain backward compatibility whenever possible.

---

# Phase 8 — Security

## Security Hardening

- Remove hardcoded API keys
- External authentication storage
- Shared secret between module and daemon
- Optional TLS support for TCP mode
- Trusted client validation
- Binary protocol fuzz hardening

---

# Phase 9 — Testing & Validation

## Integration Testing

Validate:

- TCP mode
- Unix Socket mode
- Daemon restart
- Automatic reconnection
- Peer failures
- Cache fallback behavior

---

## Load Testing

Tools:

- wrk
- vegeta

Measure:

- Throughput
- Latency
- CPU usage
- Memory usage

---

## Security Testing

- Binary protocol fuzzing
- Malformed packet injection
- Authentication bypass attempts
- Invalid frame handling

---

## Regression Testing

Maintain an automated functional test suite covering:

- Binary protocol
- Routing
- Authentication
- Persistence
- Rate limiting
- TCP mode
- Unix Socket mode

---

# Long-Term Goals

- Cluster replication
- Multi-node synchronization
- Hot reload
- Zero-downtime upgrades
- Pluggable storage backends
- HTTP/3 support
- Prometheus / OpenMetrics exporter
- Distributed rate limiting

---

# Recommended Implementation Order

## Phase 1 — Stabilization

1. Memory safety
2. Binary protocol correctness
3. Authentication hardening
4. Socket validation

---

## Phase 2 — Foundation

5. Runtime architecture
6. Connection model
7. Configuration system

---

## Phase 3 — Observability

8. Health interface
9. Metrics
10. Structured logging

---

## Phase 4 — Performance

11. Rate limiter optimization
12. Keep-alive improvements

---

## Phase 5 — Advanced Features

13. Frame fragmentation (chunking)
14. Cluster management
15. Replication

---

## Phase 6 — Validation

16. Benchmarking
17. Fuzz testing
18. Continuous regression pipeline

---

# Design Principles

- The cache daemon is the **single source of truth**.
- Observability is always a **dual-plane feature**:
  - The daemon exposes data.
  - The Nginx module consumes and optionally forwards it.
- The Nginx module should never calculate metrics.
- The binary protocol must remain backward compatible whenever possible.
- Security and robustness take precedence over new features.
- Performance optimizations must never compromise protocol correctness.