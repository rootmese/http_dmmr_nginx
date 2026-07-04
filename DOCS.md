# Conceptual Documentation for the DMMR Project

This project implements a lightweight API gateway in C, composed of a native module for Nginx and a cache daemon for authentication and key persistence.

## Overview

The goal is to provide a simple flow of:

1. receiving requests in Nginx;
2. routing them to upstream services;
3. authenticating them using API keys;
4. validating keys through a local cache service;
5. persisting keys in Berkeley DB.

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

The current implementation supports:
- Unix sockets;
- TCP;
- or both simultaneously.

## How Authentication Works

When a request arrives with an API key, the Nginx module tries to query the cache daemon.
If the key exists and is valid, the request is authorized.
If there is no result, there is a fallback to static keys defined in the module itself.

## Example Configuration

In Nginx, it is possible to specify the cache address with the directive:

```nginx
location / {
    dmmr_enable on;
    dmmr_cache_addr unix:/tmp/dmmr_cache.sock;  # or tcp:127.0.0.1:9080
}
```

## Persistence

API keys are stored in Berkeley DB, which provides local persistence without requiring Redis.

## Evolution Points

The project already supports the basic cache integration, but it can still evolve in performance and robustness with:
- persistent connections;
- a lighter internal protocol;
- in-memory caching;
- shared memory integration for rate limiting.
