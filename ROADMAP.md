# DMMR Roadmap and TODO

## Current Status

The project already includes the following capabilities:

- Configurable cache address via `dmmr_cache_addr`
- Support for both Unix socket and TCP modes
- Persistent connection reuse in the Nginx module
- Binary protocol between Nginx module and cache daemon
- Basic reconnection logic on broken sockets

---

## 1. Critical Bug Fixes (Priority: HIGH)

### Memory Safety
- Fix buffer overflow risk in `handle_binary_request`
  - Ensure strict bounds: `key_len <= sizeof(req_buf) - 5`
  - Guarantee safe null termination

### Protocol Correctness
- Fix incorrect payload length handling with `htons(payload_len)`
  - Prevent corrupted or truncated responses
  - Use raw length for `send()`, network-order only for protocol headers

- Handle partial `recv()` / `send()` correctly
  - Implement full read/write loops
  - Prevent fragmented frame interpretation

- Fix header fragmentation assumption (first 4 bytes)
  - Buffer until full header is received before parsing

### Nginx Module Stability
- Remove blocking I/O in Nginx worker context (`send/recv`)
  - Risk: event loop blocking up to 500ms
  - Fix: move to non-blocking ngx event model or worker thread offload

- Validate persistent socket before reuse
  - Add `SO_ERROR` check or heartbeat mechanism

### Authentication Safety
- Remove or disable static fallback authentication
  - Prevent unauthorized access if cache backend is unavailable
  - Make fallback explicit and opt-in only

---

## 2. Planned Improvements

### 2.1 Rate Limiter Refactor
- Replace RBTree-based limiter with hash-based structure
- Remove full-tree traversal/purging per request
- Introduce time-bucket or sliding-window hash strategy
- Target: O(1) average lookup and eviction

---

### 2.2 Cache Daemon Keep-Alive
- Keep client connections open across multiple requests
- Detect disconnect via zero-byte reads
- Add idle timeout to avoid orphan connections
- Reduce socket churn and syscall overhead

---

### 2.3 Connection Model Hardening
- Remove global shared connection state in module
- Introduce:
  - per-worker connection state OR
  - connection pool abstraction
- Prepare system for future concurrency scaling

---

### 2.4 Configuration & Deployment
- Make runtime paths configurable:
  - Unix socket path
  - database path
- Introduce optional config file (`dmmr_cache.conf`)
- Improve documentation for Nginx directives:
  - `dmmr_enable`
  - `dmmr_service`
  - `dmmr_route`
  - `dmmr_cache_addr`

---

### 2.5 Observability
- Add runtime metrics:
  - latency
  - request count
  - error rate
  - cache hit/miss ratio
  - rate limit triggers
- Structured logging (JSON or leveled logs)
- Add `/status` endpoint for health and diagnostics

---

## 3. Security Hardening (Required Improvements)

- Remove static API keys from source code
- Externalize authentication storage (config or secure store)
- Add optional shared secret between Nginx module and cache daemon
- Consider TLS for TCP mode (even on localhost if needed in hardened deployments)
- Ensure rate limiting is based on trusted client IP only (avoid spoofable headers)

---

## 4. Testing & Validation Plan

- Integration tests:
  - Unix socket mode
  - TCP mode
  - daemon restart + reconnection
  - rate limiter under burst load
  - fallback behavior when cache is unavailable

- Load testing:
  - Benchmark latency and throughput (before/after changes)
  - Tools: `wrk`, `vegeta`

- Security testing:
  - Fuzz binary protocol parser
  - Malformed packet injection tests

---

## 5. Suggested Implementation Order

1. Fix all critical bugs (memory + protocol correctness)
2. Remove or disable static authentication fallback
3. Fix partial I/O handling (recv/send + header parsing)
4. Add socket liveness validation
5. Refactor rate limiter to hash-based model
6. Implement cache daemon keep-alive
7. Harden connection model (pool or per-worker state)
8. Add configuration system (paths + conf file)
9. Implement metrics and structured logging
10. Add full testing + benchmarking pipeline

---

## Notes

These changes are intentionally staged to preserve system simplicity while progressively increasing robustness and production readiness.

Each phase should be validated with regression and load testing to avoid destabilizing the core cache engine.