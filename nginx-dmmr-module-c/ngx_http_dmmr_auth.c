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
#define DMMR_FRAME_MAGIC 0xD4D4u
#define DMMR_FRAME_VERSION 1

static ngx_str_t *ngx_http_dmmr_get_header_value(ngx_http_request_t *r,
                                                 const char *name);

/* Sends the complete frame, retrying interrupted and partial writes. */
static ssize_t ngx_http_dmmr_send_full(int fd, const void *buf, size_t len);

static ssize_t
ngx_http_dmmr_send_full(int fd, const void *buf, size_t len)
{
    size_t sent = 0;
    ssize_t n;

    while (sent < len) {
        n = send(fd, (const u_char *) buf + sent, len - sent, 0);
        if (n > 0) {
            sent += (size_t) n;
            continue;
        }

        if (n < 0 && errno == EINTR) {
            continue;
        }

        if (n == 0) {
            errno = EPIPE;
        }
        return -1;
    }

    return (ssize_t) sent;
}

/* Função auxiliar para buscar cabeçalho */
static ngx_str_t *
ngx_http_dmmr_get_header_value(ngx_http_request_t *r, const char *name)
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

static uint64_t
ngx_http_dmmr_htonll(uint64_t value)
{
    union {
        uint64_t u64;
        uint32_t u32[2];
    } v;

    v.u64 = value;
    return ((uint64_t) htonl(v.u32[0]) << 32) | htonl(v.u32[1]);
}

static ngx_int_t
ngx_http_dmmr_extract_credential(ngx_http_request_t *r, ngx_str_t *credential)
{
    ngx_str_t *auth_header;
    ngx_str_t *api_key_header;
    ngx_str_t args;
    u_char *p;
    u_char *end;

    credential->data = NULL;
    credential->len = 0;

    auth_header = ngx_http_dmmr_get_header_value(r, "authorization");
    if (auth_header != NULL && auth_header->len > 0) {
        if (auth_header->len > 7 && ngx_strncasecmp(auth_header->data, (u_char *) "Bearer ", 7) == 0) {
            credential->data = auth_header->data + 7;
            credential->len = auth_header->len - 7;
            return NGX_OK;
        }

        if (auth_header->len > 6 && ngx_strncasecmp(auth_header->data, (u_char *) "Token ", 6) == 0) {
            credential->data = auth_header->data + 6;
            credential->len = auth_header->len - 6;
            return NGX_OK;
        }
    }

    api_key_header = ngx_http_dmmr_get_header_value(r, "x-api-key");
    if (api_key_header != NULL && api_key_header->len > 0) {
        *credential = *api_key_header;
        return NGX_OK;
    }

    api_key_header = ngx_http_dmmr_get_header_value(r, "apikey");
    if (api_key_header != NULL && api_key_header->len > 0) {
        *credential = *api_key_header;
        return NGX_OK;
    }

args = r->args;
if (args.len > 0) {
    p = (u_char *) ngx_strstr(args.data, "apikey=");
    if (p == NULL) {
        p = (u_char *) ngx_strstr(args.data, "token=");
    }
    if (p != NULL) {
        p += (p[0] == 'a') ? 7 : 6;
        end = p;
        while (*end && *end != '&') {
            end++;
        }
        credential->data = p;
        credential->len = end - p;
        return NGX_OK;
    }
}

    return NGX_DECLINED;
}

/* Cria uma nova conexão com o cache, retorna fd ou -1 em caso de erro */
static int
ngx_http_dmmr_cache_connect_new(ngx_http_request_t *r, ngx_str_t *cache_addr)
{
    struct timeval tv;
    struct addrinfo hints;
    struct addrinfo *res, *rp;
    struct sockaddr_un sun;
    u_char *host_start, *host_end, *port_start, *path;
    size_t path_len;
    ngx_str_t host, port;
    char host_str[256], port_str[16];
    int fd, rc;

    if (cache_addr->len == 0 || cache_addr->data == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "dmmr: cache_addr is empty");
        return -1;
    }

    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
    "cache_addr len=%uz value='%V'",
    cache_addr->len, cache_addr);

    /* Unix domain socket */
    if (ngx_strncmp(cache_addr->data, "unix:", 5) == 0) {
        path = cache_addr->data + 5;
        path_len = (size_t) (cache_addr->len - 5);

        if (path_len == 0 || path_len >= sizeof(sun.sun_path)) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "dmmr: invalid unix socket path '%.*s'",
                          (int)path_len, path);
            return -1;
        }

        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
        "path len=%uz value='%*s'",
        path_len, (int)path_len, path);

        
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
                          "dmmr: socket(AF_UNIX) failed");
            return -1;
        }
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
              "socket() fd=%d errno=%d", fd, errno);

        tv.tv_sec = 1;
        tv.tv_usec = 0;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const void *) &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const void *) &tv, sizeof(tv));

        ngx_memzero(&sun, sizeof(sun));
        sun.sun_family = AF_UNIX;
        ngx_memcpy((u_char *) sun.sun_path, path, path_len);
        sun.sun_path[path_len] = '\0';

        if (connect(fd, (struct sockaddr *) &sun, sizeof(sun)) < 0) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
                          "dmmr: connect to unix socket '%*s' failed",
                          (size_t) path_len, path);
            close(fd);
            return -1;
        }

        ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0,
                      "dmmr: connected to unix socket '%.*s'",
                      (int)path_len, path);
        return fd;
    }

    /* TCP socket */
    host_start = cache_addr->data;
    if (ngx_strncmp(cache_addr->data, "tcp:", 4) == 0) {
        host_start = cache_addr->data + 4;
    }

    host_end = cache_addr->data + cache_addr->len;
    port_start = ngx_strlchr(host_start, host_end, ':');
    if (port_start == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "dmmr: invalid TCP address '%.*s' (missing port)",
                      (int)cache_addr->len, cache_addr->data);
        return -1;
    }

    host.data = host_start;
    host.len = port_start - host_start;
    port.data = port_start + 1;
    port.len = host_end - (port_start + 1);

    if (host.len == 0 || port.len == 0 ||
        host.len >= sizeof(host_str) || port.len >= sizeof(port_str)) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "dmmr: invalid host or port in '%.*s'",
                      (int)cache_addr->len, cache_addr->data);
        return -1;
    }

    ngx_memcpy((u_char *) host_str, host.data, host.len);
    host_str[host.len] = '\0';
    ngx_memcpy((u_char *) port_str, port.data, port.len);
    port_str[port.len] = '\0';

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
                      "dmmr: socket(AF_INET) failed");
        return -1;
    }

    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const void *) &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const void *) &tv, sizeof(tv));

    ngx_memzero(&hints, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    rc = getaddrinfo(host_str, port_str, &hints, &res);
    if (rc != 0) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "dmmr: getaddrinfo(%s:%s) failed: %s",
                      host_str, port_str, gai_strerror(rc));
        close(fd);
        return -1;
    }

    for (rp = res; rp != NULL; rp = rp->ai_next) {
        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            break;
        }
    }

    freeaddrinfo(res);
    if (rp == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
                      "dmmr: connect to %s:%s failed", host_str, port_str);
        close(fd);
        return -1;
    }

    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0,
                  "dmmr: connected to %s:%s", host_str, port_str);
    return fd;
}

/* Envia requisição ao cache e retorna NGX_OK com user_info preenchido, ou NGX_DECLINED ou NGX_ERROR */
static ngx_int_t
ngx_http_dmmr_send_cache_request(ngx_http_request_t *r, ngx_str_t *api_key, ngx_str_t *user_info)
{
    ngx_http_dmmr_conf_t *kcf;
    ngx_str_t cache_addr;
    unsigned char req_buf[4096];
    unsigned char resp_buf[4096];
    unsigned char *payload;
    struct {
        uint16_t magic;
        uint16_t version;
        uint16_t opcode;
        uint16_t flags;
        uint32_t key_len;
        uint32_t value_len;
        uint64_t timestamp;
    } frame;
    size_t total;
    ssize_t n;
    uint16_t status;
    uint32_t resp_len;
    ssize_t sent;
    int fd = -1;

    kcf = ngx_http_get_module_loc_conf(r, ngx_http_dmmr_module);
    if (kcf == NULL) {
        return NGX_ERROR;
    }

    cache_addr = kcf->cache_addr;

    if (api_key->len > sizeof(req_buf) - sizeof(frame)) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "dmmr: API key too long (%uz)", api_key->len);
        return NGX_ERROR;
    }

    ngx_memzero(&frame, sizeof(frame));
    frame.magic = htons(DMMR_FRAME_MAGIC);
    frame.version = htons(DMMR_FRAME_VERSION);
    frame.opcode = htons(DMMR_PROTO_OP_GET);
    frame.flags = htons(0);
    frame.key_len = htonl((uint32_t) api_key->len);
    frame.value_len = htonl(0);
    frame.timestamp = ngx_http_dmmr_htonll(0);

    ngx_memzero(req_buf, sizeof(req_buf));
    ngx_memcpy(req_buf, &frame, sizeof(frame));
    ngx_memcpy(req_buf + sizeof(frame), api_key->data, api_key->len);

    /* Tenta no máximo 2 vezes */
    for (int retry = 0; retry < 2; retry++) {
        /* Conectar */
        fd = ngx_http_dmmr_cache_connect_new(r, &cache_addr);
        if (fd < 0) {
            continue;
        }

        /* Enviar requisição */
        sent = ngx_http_dmmr_send_full(fd, req_buf, sizeof(frame) + api_key->len);
        if (sent < 0) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
                          "dmmr: send to cache failed");
            close(fd);
            continue;
        }

        /* Ler cabeçalho (8 bytes) */
        total = 0;
        while (total < 8) {
            n = recv(fd, (char *) resp_buf + total, 8 - total, 0);
            if (n <= 0) {
                if (n == 0) {
                    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                                  "dmmr: cache connection closed during header read");
                } else {
                    ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
                                  "dmmr: recv header failed");
                }
                break;
            }
            total += (size_t) n;
        }
        if (total < 8) {
            close(fd);
            continue;
        }

        status = ntohs(*(uint16_t *) resp_buf);
        resp_len = ntohl(*(uint32_t *) (resp_buf + sizeof(uint16_t)));

        if (resp_len > sizeof(resp_buf) - 8) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "dmmr: response length too large (%uD)", resp_len);
            close(fd);
            continue;
        }

        /* Ler corpo */
        while (total < 8 + resp_len) {
            n = recv(fd, (char *) resp_buf + total, (size_t) (8 + resp_len - total), 0);
            if (n <= 0) {
                if (n == 0) {
                    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                                  "dmmr: cache connection closed during body read");
                } else {
                    ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
                                  "dmmr: recv body failed");
                }
                break;
            }
            total += (size_t) n;
        }
        if (total < 8 + resp_len) {
            close(fd);
            continue;
        }

        close(fd);
        fd = -1;

        if (status == DMMR_PROTO_STATUS_OK) {
            payload = resp_buf + 8;
            user_info->len = resp_len;
            user_info->data = ngx_pnalloc(r->pool, user_info->len);
            if (user_info->data) {
                ngx_memcpy(user_info->data, payload, user_info->len);
            }
            ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0,
                          "dmmr: cache returned OK, user_info='%V'", user_info);
            return NGX_OK;
        } else {
            ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0,
                          "dmmr: cache returned status %d (not OK)", status);
            return NGX_DECLINED;
        }
    }

    /* Fecha se ainda estiver aberto */
    if (fd >= 0) close(fd);
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "dmmr: all retries failed");
    return NGX_ERROR;
}

static ngx_int_t
ngx_http_dmmr_verify_key_api(ngx_http_request_t *r, ngx_str_t *api_key, ngx_str_t *user_info)
{
    return ngx_http_dmmr_send_cache_request(r, api_key, user_info);
}

/* Estrutura para chave API (fallback estático) */
typedef struct {
    ngx_str_t  key;
    ngx_str_t  user;
    ngx_str_t  permissions;
} ngx_http_dmmr_api_key_t;

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
    ngx_str_t user_info = ngx_null_string;
    ngx_str_t credential;
    ngx_int_t cache_rc;

    /* Rotas públicas */
    if (r->uri.len >= 7 && ngx_strncmp(r->uri.data, "/public", 7) == 0) {
        ctx->authenticated = 1;
        return NGX_OK;
    }

    if (ngx_http_dmmr_extract_credential(r, &credential) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "dmmr: missing credential (token or API key)");
        return NGX_HTTP_UNAUTHORIZED;
    }

    api_key = ngx_palloc(r->pool, sizeof(ngx_str_t));
    if (api_key == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    *api_key = credential;

    /* Valida via Cache Server */
    cache_rc = ngx_http_dmmr_verify_key_api(r, api_key, &user_info);

    if (cache_rc == NGX_OK) {
        ctx->authenticated = 1;
        ctx->auth_user = ngx_palloc(r->pool, sizeof(ngx_str_t));
        if (ctx->auth_user) {
            *ctx->auth_user = user_info;
        }
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                      "dmmr: authenticated user '%V' via cache service", ctx->auth_user);
        return NGX_OK;
    }

    /* Cache indisponível -> não usa fallback */
    if (cache_rc == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "dmmr: authentication cache unavailable");
        return NGX_HTTP_SERVICE_UNAVAILABLE;
    }

    /*
     * cache_rc == NGX_DECLINED: cache respondeu que a chave não existe.
     * Fallback estático (apenas para desenvolvimento/ambiente controlado).
     */
    for (i = 0; i < sizeof(valid_keys)/sizeof(valid_keys[0]); i++) {
        if (api_key->len == valid_keys[i].key.len &&
            ngx_strncmp(api_key->data, valid_keys[i].key.data, api_key->len) == 0) {
            ctx->authenticated = 1;
            ctx->auth_user = &valid_keys[i].user;
            ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                          "dmmr: authenticated user '%V' (static fallback)", ctx->auth_user);
            return NGX_OK;
        }
    }

    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "dmmr: invalid API key '%.*s'", (int)api_key->len, api_key->data);
    return NGX_HTTP_FORBIDDEN;
}
