# DMMR Roadmap

## Current Status

### Core Features

- Native Nginx module
- Binary protocol between Nginx module and cache daemon
- Berkeley DB persistence
- TCP transport
- Unix Socket transport
- Configurable cache address via `dmmr_cache_addr`
- Retry-based cache reconnection
- API Key authentication
- Dynamic routing
- Rate limiting

> Status note: persistent connection reuse currently exists for cluster peers.
> The Nginx module opens a cache connection per request and closes it after the
> response; persistent cache connections in Nginx remain planned work.

---

# Phase 1 — Stabilization (Highest Priority)

Validation baseline: the `0.1.0-beta` integration suite completed 16 tests and
112 checks with no failures. Items below describe implementation status, not
only test coverage. The beta retains synchronous cache I/O with bounded
timeouts and retry; the event-driven replacement is the first priority after
the beta release.

## Memory Safety

- [x] Validate modern-frame key and value lengths against `MAX_KEY_LEN` and
  `MAX_VALUE_LEN` before reading the payload.
- [x] Bound Nginx request and response buffers before copying data.
- [x] Reject invalid modern frames and oversized payload declarations.
- [x] Check Unix socket path length before copying it into `sockaddr_un`.
- [ ] Audit every legacy-protocol and cluster-path field with explicit limits
  and add focused regression tests for each rejection path.

---

## Binary Protocol Robustness

- [x] Cache daemon handles partial `recv()` and partial `send()` through
  `recv_full()` and `send_full()`.
- [x] Nginx reads response headers and bodies in loops, handling partial
  `recv()` results.
- [x] Modern-frame header parsing and payload-length validation are covered by
  direct protocol and malformed-payload integration tests.
- [x] Invalid opcodes and malformed frames are rejected without crashing the
  cache daemon.
- [x] Nginx sends complete cache requests through a full-write loop that
  handles partial sends and `EINTR`.
- [ ] Add automated tests that deliberately split both requests and responses
  across multiple writes.

---

## Nginx Module Stability

- [x] Retry a failed cache connection once and return `503` when the cache is
  unavailable.
- [x] Use one-second socket send/receive timeouts to bound blocking failures.
- [ ] Implement and validate persistent cache sockets before reuse in Nginx.
- [ ] **Next-release priority:** replace synchronous `connect()`, `send()` and
  `recv()` calls in the Nginx request path with Nginx event-driven I/O.
- [ ] Add connection idle timeout, health checking and reconnection telemetry.

---

## Authentication

- [x] Validate API keys through the cache daemon backed by Berkeley DB.
- [ ] Remove the development-only static fallback keys from the Nginx module.
- [ ] Externalize credential and authorization metadata from source code.
- [ ] Add an optional shared secret between the Nginx module and cache daemon.
- [ ] Add key rotation, secret-rotation and authorization regression tests.

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
