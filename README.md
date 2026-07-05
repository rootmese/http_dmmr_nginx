# Nginx DMMR API Gateway (Kong Alternative)

################################################################################
#️ CURRENT VERSION
################################################################################

Version: 0.0.2-alpha

This release has been intentionally downgraded to ALPHA status due to the
introduction of several new core functionalities that are not yet fully
validated in production-like environments.

The system is currently considered EXPERIMENTAL and may contain instability in:

- binary protocol handling
- Nginx module I/O path under load
- observability and control-plane integration
- peer failure handling mechanisms
- configuration via environment variables


################################################################################
#  ALPHA STATUS JUSTIFICATION
################################################################################

Version 0.0.2-alpha reflects a deliberate decision to mark the system as
unstable while the following critical areas are still under validation:

- new control-plane observability model (dual-plane Nginx ↔ daemon)
- health check and status endpoints
- environment-based configuration system
- control queue monitoring instrumentation
- peer failure detection and recovery logic
- persistence layer operational correctness under containerized deployments

These features introduce significant architectural changes and therefore require:

- load testing
- fuzzing
- integration validation
- failure-mode analysis

Until these validations are complete, the project remains in ALPHA stage.


################################################################################
# PROJECT OVERVIEW
################################################################################

This repository contains a lightweight, high-performance API gateway
implementation in C, composed of:

1. nginx-dmmr-module-c
   Native C module for Nginx responsible for:
   - dynamic routing
   - authentication
   - rate limiting
   - reverse proxying

2. http_dmmr_cache
   Cache microservice for API key management using Berkeley DB (BDB).
   The Nginx module communicates with this service via Unix socket or TCP.


################################################################################
# REPOSITORY STRUCTURE
################################################################################

- nginx-dmmr-module-c/
  Source code for the Nginx module

- http_dmmr_cache/
  Cache service and API key persistence backed by Berkeley DB


################################################################################
# 1. CACHE SERVICE (http_dmmr_cache)
################################################################################

The cache service stores keys persistently in:

http_dmmr_cache/apikeys.db


--------------------------------------------------------------------------------
# DEPENDENCIES
--------------------------------------------------------------------------------

Debian/Ubuntu:
sudo apt-get install libmicrohttpd-dev libdb-dev

RHEL/Fedora:
sudo dnf install libmicrohttpd-devel libdb-dev


--------------------------------------------------------------------------------
# BUILD AND RUN
--------------------------------------------------------------------------------

cd http_dmmr_cache
make

./http_dmmr_cache --unix
# or
./http_dmmr_cache --tcp
# or
./http_dmmr_cache --both


By default, the service listens on:

- Unix Domain Socket: /tmp/dmmr_cache.sock
- TCP: 127.0.0.1:9080


--------------------------------------------------------------------------------
# KEY MANAGEMENT EXAMPLES (curl)
--------------------------------------------------------------------------------

Insert or update a key (Unix socket):

curl --unix-socket /tmp/dmmr_cache.sock -X PUT \
-d '{"consumer":"app1","rate_limit":100}' \
http://localhost/keys/abc123


Read a key (Unix socket):

curl --unix-socket /tmp/dmmr_cache.sock \
http://localhost/keys/abc123


Delete a key (Unix socket):

curl --unix-socket /tmp/dmmr_cache.sock -X DELETE \
http://localhost/keys/abc123


Read a key (TCP):

curl http://127.0.0.1:9080/keys/abc123


################################################################################
# 2. NGINX MODULE (nginx-dmmr-module-c)
################################################################################

This module intercepts HTTP requests during the access phase to:

- route traffic dynamically
- authenticate via cache daemon
- enforce rate limiting


--------------------------------------------------------------------------------
# BUILD NGINX WITH MODULE
--------------------------------------------------------------------------------

wget http://nginx.org/download/nginx-1.24.0.tar.gz
tar -xzf nginx-1.24.0.tar.gz
cd nginx-1.24.0

./configure --add-module=/path/to/nginx-dmmr-module-c
make
sudo make install


--------------------------------------------------------------------------------
# EXAMPLE CONFIGURATION (nginx.conf)
--------------------------------------------------------------------------------

load_module /usr/local/nginx/modules/ngx_http_dmmr_module.so;

worker_processes auto;

events {
    worker_connections 1024;
}

http {
    dmmr_enable on;

    dmmr_service api1 localhost:8001;
    dmmr_service api2 localhost:8002;

    dmmr_route /api/v1 api1;
    dmmr_route /api/v2 api2;

    server {
        listen 80;
        server_name _;

        location / {
            dmmr_enable on;
            dmmr_cache_addr unix:/tmp/dmmr_cache.sock;  # or tcp:127.0.0.1:9080
            dmmr_rate_limit 120;
            dmmr_rate_window 60000;

            proxy_pass http://$dmmr_upstream;

            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        }
    }
}


################################################################################
# AUTHENTICATION FLOW
################################################################################

Request arrives with credentials via:

- Authorization: Bearer <token>
- x-api-key
- query parameter (token= or apikey=)


Nginx module sends a binary request to cache daemon using:

- magic
- version
- opcode
- key_len
- timestamp


Cache returns 200 OK on success.

On failure:
- fallback to static keys defined in ngx_http_dmmr_auth.c


Rate limiting is applied using:

- client IP
- dmmr_rate_limit
- dmmr_rate_window


################################################################################
# FUTURE WORK AND ROADMAP
################################################################################

A structured roadmap is available in:

ROADMAP.md

It includes planned improvements for:

- rate limiting refactor
- keep-alive handling
- observability (dual-plane model)
- peer failure detection
- configuration via environment variables
- health check endpoints


################################################################################
# LICENSE
################################################################################

This project is licensed under the BSD 2-Clause License.
See LICENSE for details.