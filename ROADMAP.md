# DMMR Roadmap and TODO (Updated)

## Current Status

The project already includes:

- Configurable cache address via `dmmr_cache_addr`
- Support for Unix socket and TCP modes
- Persistent connection reuse in the Nginx module
- Binary protocol between Nginx module and cache daemon
- Basic reconnection logic on broken sockets

---

# 1. Critical Bug Fixes (HIGH PRIORITY)

## Memory Safety
- Fix buffer overflow risk in `handle_binary_request`
  - Ensure strict bounds: `key_len <= sizeof(req_buf) - 5`
  - Ensure safe null termination

---

## Protocol Correctness
- Fix incorrect payload length handling with `htons(payload_len)`
  - `send()` must use raw length
  - network-order only for protocol headers

- Handle partial `recv()` / `send()` correctly
  - Implement full read/write loops
  - Prevent fragmented frame parsing

- Fix header fragmentation assumption (first 4 bytes)
  - Buffer until full header is received before parsing

---

## Nginx Module Stability
- Remove blocking I/O in Nginx worker context (`send/recv`)
  - Risk: event loop blocking (~500ms)
  - Fix options:
    - ngx event-driven non-blocking model
    - worker thread offload

- Validate persistent socket before reuse
  - `SO_ERROR` check
  - Optional heartbeat mechanism

---

## Authentication Safety
- Remove or disable static fallback authentication
  - Prevent bypass if backend is unavailable
  - Make fallback explicit and opt-in only

---

# 2. Planned Improvements

## 2.1 Rate Limiter Refactor
- Replace RBTree-based limiter with hash-based structure
- Remove full-tree traversal per request
- Introduce:
  - time-bucket strategy
  - or sliding-window hash model
- Target: O(1) average lookup and eviction

---

## 2.2 Cache Daemon Keep-Alive
- Keep client connections persistent across requests
- Detect disconnect via zero-byte reads
- Add idle timeout to avoid orphan connections
- Reduce syscall overhead and socket churn

---

## 2.3 Connection Model Hardening
- Remove global shared connection state in the module
- Introduce:
  - per-worker state OR
  - connection pool abstraction
- Prepare system for future concurrency scaling

---

## 2.4 Configuration & Deployment
- Make runtime parameters configurable:
  - Unix socket path
  - database path
  - TTL values

- Introduce optional config file:
  - `dmmr_cache.conf`

- Improve Nginx directive documentation:
  - `dmmr_enable`
  - `dmmr_service`
  - `dmmr_route`
  - `dmmr_cache_addr`

---

## 2.5 Observability (DUAL-PLANE REQUIREMENT)

### Important rule
Any observability feature requires changes in both:
- cache daemon
- Nginx module (consumer layer)

---

### Metrics to expose

#### Cache daemon
- request latency (p50/p95 optional)
- request count
- error rate
- cache hit/miss ratio
- rate limit triggers
- control queue size
- active workers

#### Nginx module
- consume metrics from daemon via:
  - internal socket command
  - status endpoint
  - or lightweight binary protocol

- optionally expose metrics to Nginx variables / headers / internal endpoints

---

### Status / control interface
- `/status` endpoint or internal socket commands:
  - `PING`
  - `STATUS`
  - `STATS`

- Must be low-latency and non-blocking

---

# 3. Security Hardening

- Remove hardcoded API keys from source code
- Externalize authentication storage (config or secure store)
- Add optional shared secret between Nginx module and daemon
- Optional TLS support for TCP mode
- Ensure rate limiting uses trusted IP only (avoid spoofable headers)

---

# 4. NEW FEATURES (ADDED REQUIREMENTS)

## 4.1 Environment-Based Configuration
Support configuration via environment variables:

Examples:
- `DMMR_TCP_PORT`
- `DMMR_CLUSTER_PORT`
- `DMMR_TTL_SECONDS`
- `DMMR_CACHE_PATH`
- `DMMR_LOG_LEVEL`

### Impact:
- Cache daemon
- Nginx module (config loader + precedence rules)

---

## 4.2 Health Check Endpoint
- Internal endpoint or socket command:
  - `PING`
  - `STATUS`

### Requirements:
- Extremely fast response (<1ms ideal)
- Must not block request path
- Must be consumable by Nginx module

---

## 4.3 Control Queue Monitoring
- Expose internal queue metrics:
  - queue depth
  - pending tasks
  - dequeue rate
  - worker backlog

### Impact:
- Cache daemon instrumentation
- Nginx module optional consumption layer

---

## 4.4 Peer Failure Handling
- Detect non-responsive peers:
  - timeout-based detection
  - heartbeat failure detection

- Mark peers as:
  - inactive
  - degraded

- Retry with backoff strategy

### Impact:
- control thread in cache daemon
- optional exposure via `/status`
- Nginx routing decisions may avoid dead peers

---

## 4.5 Persistence (Berkeley DB)
- Persistence already implemented via Berkeley DB

### Deployment requirement:
- Mount persistent volume in container
- Ensure:
  - data durability across restarts
  - safe recovery on startup

---

# 5. Testing & Validation Plan

## Integration Tests
- Unix socket mode
- TCP mode
- daemon restart + reconnection
- peer failure simulation
- fallback behavior when cache is unavailable

---

## Load Testing
- Tools: `wrk`, `vegeta`
- Measure:
  - latency
  - throughput
  - baseline vs optimized versions

---

## Security Testing
- Fuzz binary protocol parser
- Malformed packet injection
- Authentication bypass attempts

---

## Observability Validation
- Ensure consistency between:
  - daemon metrics
  - Nginx exposed metrics

---

# 6. Suggested Implementation Order (Revised)

1. Fix memory safety + protocol correctness
2. Remove static authentication fallback
3. Fix partial I/O handling + header parsing
4. Add socket liveness validation
5. Implement environment-based configuration
6. Implement health check endpoint (daemon + Nginx consumer)
7. Add control queue monitoring
8. Implement peer failure handling
9. Refactor rate limiter to hash-based model
10. Implement keep-alive connection model
11. Harden connection model (pool / per-worker)
12. Add persistence deployment layer (volumes)
13. Implement metrics + structured logging (dual-plane)
14. Full testing, benchmarking, and fuzzing pipeline

---

# Notes

- Observability is always a dual-plane feature:
  - daemon exposes data
  - Nginx consumes and optionally forwards it

- Nginx must never “compute” metrics itself
  - it only acts as a consumer layer

- Cache daemon is the single source of truth