#include "ngx_http_dmmr_module.h"
#include <ngx_sha1.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>
#include <errno.h>

#define DMMR_PROTO_OP_GET 1
#define DMMR_PROTO_STATUS_OK 0
#define DMMR_PROTO_STATUS_NOT_FOUND 1
#define DMMR_PROTO_STATUS_ERROR 2

static ngx_str_t *ngx_http_dmmr_get_header_value(ngx_http_request_t *r,
                                                 const char *name);

typedef struct {
    int fd;
    ngx_uint_t connected;
    u_char addr[256];
    size_t addr_len;
} ngx_http_dmmr_cache_conn_t;

static ngx_http_dmmr_cache_conn_t ngx_http_dmmr_cache_conn = {
    -1, 0, {0}, 0
};

static void
ngx_http_dmmr_cache_close_conn(ngx_http_dmmr_cache_conn_t *conn)
{
    if (conn->fd >= 0) {
        close(conn->fd);
        conn->fd = -1;
    }
    conn->connected = 0;
    conn->addr_len = 0;
}

static ngx_int_t
ngx_http_dmmr_cache_addr_matches(ngx_http_dmmr_cache_conn_t *conn, ngx_str_t *addr)
{
    return conn->connected
           && conn->addr_len == (size_t) addr->len
           && ngx_memcmp(conn->addr, addr->data, addr->len) == 0;
}

static ngx_int_t
ngx_http_dmmr_cache_connect(ngx_http_request_t *r, ngx_str_t *cache_addr)
{
    struct timeval tv;
    struct addrinfo hints;
    struct addrinfo *res;
    struct addrinfo *rp;
    struct sockaddr_un sun;
    u_char *host_start;
    u_char *host_end;
    u_char *port_start;
    u_char *path;
    size_t path_len;
    ngx_str_t host;
    ngx_str_t port;
    char host_str[256];
    char port_str[16];
    char *addr_prefix;
    int fd;
    int rc;

    if (ngx_http_dmmr_cache_addr_matches(&ngx_http_dmmr_cache_conn, cache_addr)) {
        return NGX_OK;
    }

    ngx_http_dmmr_cache_close_conn(&ngx_http_dmmr_cache_conn);

    if (cache_addr->len == 0 || cache_addr->data == NULL) {
        return NGX_ERROR;
    }

    if (ngx_strncmp(cache_addr->data, "unix:", 5) == 0) {
        path = cache_addr->data + 5;
        path_len = (size_t) (cache_addr->len - 5);

        if (path_len == 0 || path_len >= sizeof(sun.sun_path)) {
            return NGX_ERROR;
        }

        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            return NGX_ERROR;
        }

        tv.tv_sec = 0;
        tv.tv_usec = 500000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const void *) &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const void *) &tv, sizeof(tv));

        ngx_memzero(&sun, sizeof(sun));
        sun.sun_family = AF_UNIX;
        ngx_memcpy((u_char *) sun.sun_path, path, path_len);
        sun.sun_path[path_len] = '\0';

        if (connect(fd, (struct sockaddr *) &sun, sizeof(sun)) < 0) {
            close(fd);
            return NGX_ERROR;
        }
    } else {
        addr_prefix = "tcp:";
        host_start = cache_addr->data;
        if (ngx_strncmp(cache_addr->data, addr_prefix, 4) == 0) {
            host_start = cache_addr->data + 4;
        }

        host_end = cache_addr->data + cache_addr->len;
        port_start = ngx_strlchr(host_start, host_end, ':');
        if (port_start == NULL) {
            return NGX_ERROR;
        }

        host.data = host_start;
        host.len = port_start - host_start;
        port.data = port_start + 1;
        port.len = host_end - (port_start + 1);

        if (host.len == 0 || port.len == 0 || host.len >= sizeof(host_str) || port.len >= sizeof(port_str)) {
            return NGX_ERROR;
        }

        ngx_memcpy((u_char *) host_str, host.data, host.len);
        host_str[host.len] = '\0';
        ngx_memcpy((u_char *) port_str, port.data, port.len);
        port_str[port.len] = '\0';

        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return NGX_ERROR;
        }

        tv.tv_sec = 0;
        tv.tv_usec = 500000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const void *) &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const void *) &tv, sizeof(tv));

        ngx_memzero(&hints, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        rc = getaddrinfo(host_str, port_str, &hints, &res);
        if (rc != 0) {
            close(fd);
            return NGX_ERROR;
        }

        for (rp = res; rp != NULL; rp = rp->ai_next) {
            if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
                break;
            }
        }

        freeaddrinfo(res);
        if (rp == NULL) {
            close(fd);
            return NGX_ERROR;
        }
    }

    ngx_http_dmmr_cache_conn.fd = fd;
    ngx_http_dmmr_cache_conn.connected = 1;
    ngx_http_dmmr_cache_conn.addr_len = cache_addr->len;
    if (cache_addr->len > sizeof(ngx_http_dmmr_cache_conn.addr) - 1) {
        ngx_http_dmmr_cache_conn.addr_len = sizeof(ngx_http_dmmr_cache_conn.addr) - 1;
    }
    ngx_memcpy(ngx_http_dmmr_cache_conn.addr, cache_addr->data, ngx_http_dmmr_cache_conn.addr_len);
    ngx_http_dmmr_cache_conn.addr[ngx_http_dmmr_cache_conn.addr_len] = '\0';

    return NGX_OK;
}

static ngx_int_t
ngx_http_dmmr_send_cache_request(ngx_http_request_t *r, ngx_str_t *api_key, ngx_str_t *user_info)
{
    ngx_http_dmmr_conf_t *kcf;
    ngx_str_t cache_addr;
    unsigned char req_buf[1024];
    unsigned char resp_buf[4096];
    unsigned char *payload;
    size_t total = 0;
    size_t payload_len = 0;
    ssize_t n;
    uint16_t opcode;
    uint16_t key_len;
    uint16_t status;
    uint16_t resp_len;
    int rc;

    kcf = ngx_http_get_module_loc_conf(r, ngx_http_dmmr_module);
    if (kcf == NULL) {
        return NGX_ERROR;
    }

    cache_addr = kcf->cache_addr;
    if (ngx_http_dmmr_cache_connect(r, &cache_addr) != NGX_OK) {
        return NGX_ERROR;
    }

    if (api_key->len > 0xFFFF) {
        return NGX_ERROR;
    }

    opcode = htons(DMMR_PROTO_OP_GET);
    key_len = htons((uint16_t) api_key->len);

    ngx_memzero(req_buf, sizeof(req_buf));
    ngx_memcpy(req_buf, &opcode, sizeof(opcode));
    ngx_memcpy(req_buf + sizeof(opcode), &key_len, sizeof(key_len));
    ngx_memcpy(req_buf + sizeof(opcode) + sizeof(key_len), api_key->data, api_key->len);

    rc = send(ngx_http_dmmr_cache_conn.fd, (const void *) req_buf, sizeof(opcode) + sizeof(key_len) + api_key->len, 0);
    if (rc < 0) {
        ngx_http_dmmr_cache_close_conn(&ngx_http_dmmr_cache_conn);
        if (ngx_http_dmmr_cache_connect(r, &cache_addr) != NGX_OK) {
            return NGX_ERROR;
        }

        rc = send(ngx_http_dmmr_cache_conn.fd, (const void *) req_buf, sizeof(opcode) + sizeof(key_len) + api_key->len, 0);
        if (rc < 0) {
            ngx_http_dmmr_cache_close_conn(&ngx_http_dmmr_cache_conn);
            return NGX_ERROR;
        }
    }

    total = 0;
    while (total < sizeof(resp_buf)) {
        n = recv(ngx_http_dmmr_cache_conn.fd, (char *) resp_buf + total, sizeof(resp_buf) - total, 0);
        if (n <= 0) {
            ngx_http_dmmr_cache_close_conn(&ngx_http_dmmr_cache_conn);
            if (ngx_http_dmmr_cache_connect(r, &cache_addr) != NGX_OK) {
                return NGX_ERROR;
            }
            return NGX_ERROR;
        }
        total += (size_t) n;
        if (total >= 4) {
            break;
        }
    }

    if (total < 4) {
        ngx_http_dmmr_cache_close_conn(&ngx_http_dmmr_cache_conn);
        return NGX_ERROR;
    }

    status = ntohs(*(uint16_t *) resp_buf);
    resp_len = ntohs(*(uint16_t *) (resp_buf + sizeof(uint16_t)));

    if (resp_len > sizeof(resp_buf) - 4) {
        ngx_http_dmmr_cache_close_conn(&ngx_http_dmmr_cache_conn);
        return NGX_ERROR;
    }

    while (total < 4 + resp_len) {
        n = recv(ngx_http_dmmr_cache_conn.fd, (char *) resp_buf + total, (size_t) (4 + resp_len - total), 0);
        if (n <= 0) {
            ngx_http_dmmr_cache_close_conn(&ngx_http_dmmr_cache_conn);
            return NGX_ERROR;
        }
        total += (size_t) n;
    }

    if (status == DMMR_PROTO_STATUS_OK) {
        payload = resp_buf + 4;
        user_info->len = resp_len;
        user_info->data = ngx_pnalloc(r->pool, user_info->len);
        if (user_info->data) {
            ngx_memcpy(user_info->data, payload, user_info->len);
        }
        return NGX_OK;
    }

    return NGX_DECLINED;
}

static ngx_int_t
ngx_http_dmmr_verify_key_api(ngx_http_request_t *r, ngx_str_t *api_key, ngx_str_t *user_info)
{
    return ngx_http_dmmr_send_cache_request(r, api_key, user_info);
}

/* Estrutura para chave API */
typedef struct {
    ngx_str_t  key;
    ngx_str_t  user;
    ngx_str_t  permissions;
} ngx_http_dmmr_api_key_t;

/* Lista de chaves válidas (exemplo estático) */
static ngx_http_dmmr_api_key_t valid_keys[] = {
    { ngx_string("123456"), ngx_string("user1"), ngx_string("read") },
    { ngx_string("abcdef"), ngx_string("user2"), ngx_string("read,write") },
    { ngx_string("987654"), ngx_string("admin"), ngx_string("read,write,admin") }
};

ngx_int_t
ngx_http_dmmr_auth(ngx_http_request_t *r, ngx_http_dmmr_ctx_t *ctx)
{
    ngx_str_t *api_key = NULL;
    ngx_uint_t i;
    ngx_str_t user_info;

    /* Rotas públicas (exemplo) */
    if (r->uri.len >= 7 && ngx_strncmp(r->uri.data, "/public", 7) == 0) {
        ctx->authenticated = 1;
        return NGX_OK;
    }

    /* Tenta extrair API key de headers ou query string */
    api_key = ngx_http_dmmr_get_header_value(r, "apikey");
    if (api_key == NULL) {
        api_key = ngx_http_dmmr_get_header_value(r, "x-api-key");
    }
    if (api_key == NULL) {
        /* Tenta query param */
        ngx_str_t args = r->args;
        if (args.len > 0) {
            /* Procura por apikey=... */
            u_char *p = ngx_strstr(args.data, "apikey=");
            if (p != NULL) {
                p += 7; /* len of "apikey=" */
                u_char *end = p;
                while (*end && *end != '&') end++;
                api_key = ngx_palloc(r->pool, sizeof(ngx_str_t));
                if (api_key) {
                    api_key->data = p;
                    api_key->len = end - p;
                }
            }
        }
    }

    if (api_key == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "dmmr: missing API key");
        return NGX_HTTP_UNAUTHORIZED;
    }

    /* Valida a chave via Cache Server (localhost:9080) */
    if (ngx_http_dmmr_verify_key_api(r, api_key, &user_info) == NGX_OK) {
        ctx->authenticated = 1;
        ctx->auth_user = ngx_palloc(r->pool, sizeof(ngx_str_t));
        if (ctx->auth_user) {
            *ctx->auth_user = user_info;
        }
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                      "dmmr: authenticated user '%V' via http cache service", ctx->auth_user);
        return NGX_OK;
    }

    /* Fallback: Valida a chave estaticamente */
    for (i = 0; i < sizeof(valid_keys)/sizeof(valid_keys[0]); i++) {
        if (api_key->len == valid_keys[i].key.len &&
            ngx_strncmp(api_key->data, valid_keys[i].key.data, api_key->len) == 0) {
            ctx->authenticated = 1;
            ctx->auth_user = &valid_keys[i].user;
            ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                          "dmmr: authenticated user '%V' (static)", ctx->auth_user);
            return NGX_OK;
        }
    }

    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "dmmr: invalid API key '%.*s'", (int)api_key->len, api_key->data);
    return NGX_HTTP_FORBIDDEN;
}

static ngx_str_t *
ngx_http_dmmr_get_header_value(ngx_http_request_t *r,
                               const char *name)
{
    ngx_list_part_t *part = &r->headers_in.headers.part;
    ngx_table_elt_t *h = part->elts;
    ngx_uint_t i;
    size_t len = ngx_strlen(name);

    for (;;) {
        for (i = 0; i < part->nelts; i++) {
            if (h[i].key.len == len &&
                ngx_strncasecmp(h[i].key.data, (u_char *) name, len) == 0) {
                return &h[i].value;
            }
        }

        if (part->next == NULL) {
            break;
        }

        part = part->next;
        h = part->elts;
    }

    return NULL;
}
