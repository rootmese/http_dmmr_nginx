# Conceptual Documentation for the DMMR Project

This project implements a lightweight API gateway in C, composed of a native module for Nginx and a cache daemon for authentication and key persistence.

## Overview

The goal is to provide a simple flow of:

1. receiving requests in Nginx;
2. routing them to upstream services;
3. authenticating them using API keys;
4. validating keys through a local cache service;
5. returning `503 Service Unavailable` when the cache cannot be reached;
6. persisting keys in Berkeley DB.

## Components

### 1. Nginx Module

The C module is responsible for:
- intercepting HTTP requests;
- deciding which upstream the request should go to;
- applying authentication;
- applying rate limiting with configurable windows and token-based authentication;
- forwarding the request to its final destination.

### 2. Cache Daemon

The process named http_dmmr_cache acts as the local access layer for keys.
It receives requests from the Nginx module, queries the Berkeley DB store, and returns the result.

- Unix sockets;
- TCP;
- or both simultaneously.

It also features a distributed peer-to-peer broadcast & auto-discovery mechanism:
- Automatic peer discovery via seed list (`--seeds` / `DMMR_CLUSTER_SEEDS`);
- Data synchronization via `OP_SYNC` and tombstone propagation;
- Cluster isolation using `DMMR_CLUSTER_NAME` (or `--cluster-name`), preventing unauthorized node synchronization.

## Recent Implementation Highlights

The cache service has been hardened in several areas:

- DELETE requests now persist tombstones in Berkeley DB so removals are propagated in a consistent way across the cluster.
- GET handling now returns the correct protocol status for missing keys, instead of surfacing them as generic errors.
- Peer management includes a reaper for stale or dead nodes, helping avoid leaked resources and orphaned state.
- Cluster communication validates cluster identity and supports discovery from both seeds and already-known peers.

### Cluster Security (Authentication)

Since version 0.2.0-beta, the cache daemon supports mutual authentication between cluster peers using a shared secret. The handshake is performed immediately after the TCP connection is established and before any synchronization messages are exchanged. New opcodes `OP_AUTH_REQUEST`, `OP_AUTH_RESPONSE`, and `OP_AUTH_OK` were added to the protocol to support this flow.

Authentication uses HMAC-SHA256 with randomly generated nonces and an expiration timestamp. Authenticated sessions are valid for 10 minutes, after which the peer must re-authenticate. If the handshake fails, the connection is closed and a new attempt is scheduled with exponential backoff.

This security layer is optional: if `DMMR_CLUSTER_SECRET` is not set, the behavior remains unchanged from previous versions (no authentication).

## How Authentication Works

When a request arrives with an API key, the Nginx module tries to query the cache daemon.
If the key exists and is valid, the request is authorized. Missing or invalid
keys return `403`; missing credentials return `401`. There is no static-key
fallback: a cache connection failure returns `503`.

## Example Configuration

In Nginx, it is possible to specify the cache address with the directive:

```nginx
location / {
    dmmr_enable on;
    dmmr_cache_addr unix:/tmp/dmmr_cache.sock;  # or tcp:127.0.0.1:9080
}
```

For a global rate limit across Nginx workers, define a shared zone in `http`:

```nginx
dmmr_rate_zone dmmr_limit:10m;
```

If omitted, the module creates a default 10 MiB zone named
`dmmr_rate_limit`. The zone uses a shared rbtree keyed by client IP and a
bounded LRU/expiry queue; all workers therefore apply the same counter.

## Persistence

API keys are stored in Berkeley DB, which provides local persistence without requiring Redis.

DELETE operations are represented as tombstones. Cluster replication preserves
timestamp, node ID, expiry timestamp, and tombstone state, allowing LWW
conflict resolution and propagation of removals.

## Memory pools

Pool entries and queue metadata use stable chunks. Payload buffers and queued
replication values are allocated to the actual request/value size rather than
reserving one MiB per entry. Payload buffers larger than 64 KiB are released
when returned to the pool; smaller buffers may be retained for reuse.

## Running the integration suite

Nginx must be running with matching cache endpoints. Under systemd, use a
shared `/run/dmmr` socket path instead of `/tmp` when `PrivateTmp=true`:

```bash
export DMMR_SOCKET_PATH=/run/dmmr/dmmr_cache.sock
export DMMR_CLUSTER_PORT=9091
cd tests
python3 suite_tests.py
```

The suite currently validates 16 scenarios / 113 checks, including cache and
backend failures, recovery, rate limiting, load, and RSS stability.

## Evolution Points

The project already supports the basic cache integration, but it can still evolve in performance and robustness with:
- non-blocking cache I/O in the Nginx module;
- graceful shutdown of the cluster listener and active client workers;
- optional rate-limit keys based on authenticated user or API key, in addition to client IP.
