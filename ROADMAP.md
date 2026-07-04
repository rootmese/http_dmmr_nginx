# Roadmap and TODO

## Current status

The project already has the following improvements in place:
- configurable cache address via dmmr_cache_addr;
- support for Unix socket and TCP;
- persistent cache connection reuse in the Nginx module;
- binary protocol between Nginx and the cache daemon;
- basic reconnection on broken sockets.

## Planned work

### 1. Rate limiter improvements
- Replace the current RBTree-based limiter with a hash-based structure for faster lookup.
- Avoid full-tree pruning on every request.
- Use a time-based eviction strategy with a lightweight queue or bucket list.
- Target: reduce lookup complexity from O(log n) / O(n) pruning to near O(1) in common cases.

### 2. True keep-alive on the cache daemon
- Change the daemon loop so it can serve multiple requests over a single persistent connection.
- Avoid closing the client socket after each request.
- Keep the connection open until the client closes it or the daemon detects a broken socket.

### 3. Thread safety for shared connection state
- Remove or isolate the global cache connection state if the module is later used in threaded contexts.
- Consider per-worker or per-request connection handling, or a connection pool.
- Keep this as a future hardening step rather than an immediate requirement.

### 4. Validation and testing plan
- Add basic integration tests for:
  - Unix socket mode;
  - TCP mode;
  - reconnection after daemon restart;
  - rate limiter behavior under burst traffic.
- Measure latency and throughput before and after each architectural change.

## Suggested implementation order

1. Rate limiter rewrite to hash-based structure.
2. Keep-alive loop in the cache daemon.
3. Connection pooling or worker-local connection state.
4. Integration tests and performance benchmarks.

## Notes

These changes are intentionally staged so the project can evolve without introducing unnecessary complexity too early.
