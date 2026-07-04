# Nginx DMMR API Gateway (Kong Alternative)

This repository contains a lightweight, high-performance API gateway implementation in C, composed of:
1. **nginx-dmmr-module-c**: A native C module for Nginx responsible for dynamic routing, authentication, rate limiting, and reverse proxying.
2. **http_dmmr_cache**: A cache microservice for API key management that stores data in Berkeley DB (BDB). The Nginx module connects to this cache over Unix socket or TCP to validate keys dynamically.

---

## Repository Structure

*   [nginx-dmmr-module-c](nginx-dmmr-module-c/): Source code for the Nginx module.
*   [http_dmmr_cache](http_dmmr_cache/): Cache service and API key persistence backed by Berkeley DB.

---

## 1. Cache Service (`http_dmmr_cache`)

The cache service stores keys persistently in a local Berkeley DB file ([http_dmmr_cache/apikeys.db](http_dmmr_cache/apikeys.db)).

### Install Dependencies
*   **Debian/Ubuntu**: `sudo apt-get install libmicrohttpd-dev libdb-dev`
*   **RHEL/Fedora**: `sudo dnf install libmicrohttpd-devel libdb-devel`

### Build and Run
```bash
cd http_dmmr_cache
make
./http_dmmr_cache --unix
# or
./http_dmmr_cache --tcp
# or
./http_dmmr_cache --both
```
By default, the service starts listening on the Unix Domain Socket `/tmp/dmmr_cache.sock`. If started with `--tcp` or `--both`, it also listens on TCP port 9080.

### Key Management Examples (curl)

*   **Insert or Update a Key via Unix socket**:
    ```bash
    curl --unix-socket /tmp/dmmr_cache.sock -X PUT -d '{"consumer":"app1","rate_limit":100}' http://localhost/keys/abc123
    ```
*   **Read a Key via Unix socket**:
    ```bash
    curl --unix-socket /tmp/dmmr_cache.sock http://localhost/keys/abc123
    ```
*   **Delete a Key via Unix socket**:
    ```bash
    curl --unix-socket /tmp/dmmr_cache.sock -X DELETE http://localhost/keys/abc123
    ```
*   **Read a Key via TCP**:
    ```bash
    curl http://127.0.0.1:9080/keys/abc123
    ```

---

## 2. Nginx Module (`nginx-dmmr-module-c`)

This module intercepts HTTP requests during the access phase to route traffic, authenticate via the cache, and enforce rate limits.

### Build Nginx with the Module

1. Download and extract the Nginx source code:
   ```bash
   wget http://nginx.org/download/nginx-1.24.0.tar.gz
   tar -xzf nginx-1.24.0.tar.gz
   cd nginx-1.24.0
   ```

2. Build it with the module enabled dynamically:
   ```bash
   ./configure --add-module=/path/to/nginx-dmmr-module-c
   make
   sudo make install
   ```

### Example Configuration (`nginx.conf`)

```nginx
load_module /usr/local/nginx/modules/ngx_http_dmmr_module.so;

worker_processes auto;

events {
    worker_connections 1024;
}

http {
    dmmr_enable on;

    # Upstream/service definitions
    dmmr_service api1 localhost:8001;
    dmmr_service api2 localhost:8002;

    # Route mappings to services
    dmmr_route /api/v1 api1;
    dmmr_route /api/v2 api2;

    server {
        listen 80;
        server_name _;

        location / {
            dmmr_enable on;
            dmmr_cache_addr unix:/tmp/dmmr_cache.sock;  # or tcp:127.0.0.1:9080
            # The $dmmr_upstream variable is resolved at runtime
            proxy_pass http://$dmmr_upstream;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        }
    }
}
```

---

## Authentication Flow of the Module
1. When a request arrives with an API key (via the `apikey`/`x-api-key` header or the `?apikey=` query parameter), the Nginx module performs a fast synchronous request to the address defined by `dmmr_cache_addr`.
2. The address accepts the formats `unix:/path/to/socket` and `tcp:host:port`.
3. If the cache returns `200 OK`, the request is authenticated with the information contained in the response.
4. If the cache lookup fails, the module falls back to validating against the static keys defined in [nginx-dmmr-module-c/ngx_http_dmmr_auth.c](nginx-dmmr-module-c/ngx_http_dmmr_auth.c).

## Future Work and Roadmap
A structured roadmap with planned improvements for rate limiting, true keep-alive handling, and thread-safety considerations is available in [ROADMAP.md](ROADMAP.md).

## License
This project is licensed under the BSD 2-Clause License. See [LICENSE](LICENSE) for details.
