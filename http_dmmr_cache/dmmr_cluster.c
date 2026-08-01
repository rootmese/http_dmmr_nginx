#include "dmmr_cluster.h"
#include "dmmr_config.h"
#include "dmmr_db.h"
#include "dmmr_net.h"
#include "dmmr_protocol.h"
#include "dmmr_string.h"
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define DMMR_PEER_ADDR_MAX 255
#define DMMR_SEEDS_MAX 2048
#define DMMR_MAX_DISCOVERED_PEERS 128
#define DMMR_CLUSTER_TIMEOUT_SECONDS 3

#define OUTBOX_MAX_MSG   256          /* máximo de mensagens pendentes por peer */
#define OUTBOX_MSG_TTL   120          /* segundos de vida de uma mensagem na fila (120s) */
#define PEER_RETRY_BASE  1            /* segundos */
#define PEER_RETRY_MAX   30
#define PEER_EVICT_TIMEOUT 300        /* 5 minutos (300s) sem atividade de rede */
#define PEER_HEALTHCHECK_INTERVAL 15  /* segundos */
#define PEER_MAX_STALE 60            /* segundos sem resposta útil */

extern uint64_t my_node_id;
extern volatile sig_atomic_t running;

struct outbox_msg {
    uint16_t opcode;
    uint16_t flags;
    char    *key;
    uint32_t key_len;
    uint8_t *value;
    uint32_t value_len;
    uint64_t timestamp;
    uint64_t node_id;
    uint64_t expire_at;
    time_t   enqueued_at;
    TAILQ_ENTRY(outbox_msg) entry;
};
TAILQ_HEAD(outbox_queue, outbox_msg);

struct peer_conn {
    char                addr[DMMR_PEER_ADDR_MAX + 1];
    uint16_t            port;
    uint64_t            node_id;
    int                 fd;                /* -1 se desconectado */
    pthread_t           sender_thread;
    pthread_mutex_t     outbox_mutex;
    pthread_cond_t      outbox_cond;
    struct outbox_queue outbox;
    int                 outbox_count;
    time_t              last_activity;
    time_t              next_retry;
    time_t              last_healthcheck;
    unsigned            retry_backoff;     /* segundos */
    unsigned            stale_count;
    bool                active;            /* false quando o peer é removido */
    bool                is_seed;           /* true se é peer estático (seed) */
    TAILQ_ENTRY(peer_conn) entries;
};
TAILQ_HEAD(peer_conn_list, peer_conn);

struct peer_endpoint {
    char addr[DMMR_PEER_ADDR_MAX + 1];
    uint16_t port;
    uint64_t node_id;
};

struct peer_endpoint_list {
    struct peer_endpoint items[DMMR_MAX_DISCOVERED_PEERS];
    size_t count;
};

struct snapshot_send_ctx {
    int fd;
};

static int connect_endpoint(const struct peer_endpoint *endpoint);
static int send_hello(const struct peer_endpoint *endpoint,
                      struct peer_endpoint_list *peers_out);

static struct peer_conn_list peers_conn_head = TAILQ_HEAD_INITIALIZER(peers_conn_head);
static pthread_mutex_t peers_mutex = PTHREAD_MUTEX_INITIALIZER;
static char cluster_advertise_address[DMMR_PEER_ADDR_MAX + 1] = "127.0.0.1";
static uint16_t cluster_advertise_port = CLUSTER_PORT;
static char cluster_seeds[DMMR_SEEDS_MAX] = "";
static char cluster_name[DMMR_CLUSTER_NAME_MAX + 1] = "";
static int discovery_interval_seconds = 10;
static pthread_t discovery_thread;
static int discovery_started = 0;
static int bootstrap_completed = 0;

/* Descritor do socket de cluster (GLOBAL, nÃ£o static). */
int cluster_listen_fd = -1;

static int check_cluster_id(const struct dmmr_cluster_frame *frame)
{
    if (cluster_name[0] == '\0') {
        return 0; /* sem nome de cluster configurado: aceita qualquer */
    }
    uint32_t expected_id = dmmr_fnv1a_32(cluster_name, strlen(cluster_name));
    if (frame->cluster_id != expected_id) {
        DMMR_LOG_DEBUG("cluster_id mismatch: expected 0x%08x, got 0x%08x", expected_id, frame->cluster_id);
        return -1;
    }
    return 0;
}

static int authenticate_cluster_frame(const struct dmmr_cluster_frame *frame, const uint8_t *payload)
{
    (void)payload;
    if (cluster_name[0] == '\0') {
        return 0;
    }
    if (frame->cluster_id == 0) {
        return -1;
    }
    return check_cluster_id(frame);
}

static int cluster_send_frame(int fd, uint16_t opcode, uint16_t flags,
                              const void *key, uint32_t key_len,
                              const void *value, uint32_t value_len,
                              uint64_t timestamp, uint64_t node_id,
                              uint64_t expire_at)
{
    struct dmmr_cluster_frame frame;

    if (key_len > MAX_KEY_LEN || value_len > MAX_VALUE_LEN ||
        (key_len > 0 && key == NULL) || (value_len > 0 && value == NULL)) {
        return -1;
    }

    memset(&frame, 0, sizeof(frame));
    frame.magic = htons(DMMR_MAGIC);
    frame.version = htons(DMMR_CLUSTER_VERSION);
    frame.opcode = htons(opcode);
    frame.flags = htons(flags);
    frame.key_len = htonl(key_len);
    frame.value_len = htonl(value_len);
    frame.timestamp = htonll(timestamp);
    frame.node_id = htonll(node_id);
    frame.expire_at = htonll(expire_at);
    uint32_t cid = (cluster_name[0] != '\0') ? dmmr_fnv1a_32(cluster_name, strlen(cluster_name)) : 0;
    frame.cluster_id = htonl(cid);

    if (send_full(fd, &frame, sizeof(frame), 0) != (ssize_t)sizeof(frame)) {
        return -1;
    }
    if (key_len > 0 && send_full(fd, key, key_len, 0) != (ssize_t)key_len) {
        return -1;
    }
    if (value_len > 0 && send_full(fd, value, value_len, 0) != (ssize_t)value_len) {
        return -1;
    }
    return 0;
}

static int cluster_recv_frame(int fd, struct dmmr_cluster_frame *frame,
                              uint8_t **payload_out)
{
    uint8_t prefix[4];
    uint16_t magic;
    uint16_t version;
    size_t total_len;
    uint8_t *payload = NULL;

    *payload_out = NULL;
    if (recv_full(fd, prefix, sizeof(prefix), 0) != (ssize_t)sizeof(prefix)) {
        return -1;
    }
    memcpy(&magic, prefix, sizeof(magic));
    memcpy(&version, prefix + sizeof(magic), sizeof(version));
    if (ntohs(magic) != DMMR_MAGIC || ntohs(version) != DMMR_CLUSTER_VERSION) {
        return -1;
    }

    memset(frame, 0, sizeof(*frame));
    memcpy(frame, prefix, sizeof(prefix));
    if (recv_full(fd, (uint8_t *)frame + sizeof(prefix),
                  sizeof(*frame) - sizeof(prefix), 0) !=
        (ssize_t)(sizeof(*frame) - sizeof(prefix))) {
        return -1;
    }

    frame->magic = ntohs(frame->magic);
    frame->version = ntohs(frame->version);
    frame->opcode = ntohs(frame->opcode);
    frame->flags = ntohs(frame->flags);
    frame->key_len = ntohl(frame->key_len);
    frame->value_len = ntohl(frame->value_len);
    frame->timestamp = ntohll(frame->timestamp);
    frame->node_id = ntohll(frame->node_id);
    frame->expire_at = ntohll(frame->expire_at);
    frame->cluster_id = ntohl(frame->cluster_id);

    if (frame->key_len > MAX_KEY_LEN || frame->value_len > MAX_VALUE_LEN) {
        return -1;
    }
    total_len = (size_t)frame->key_len + frame->value_len;
    if (total_len == 0) {
        return 0;
    }

    payload = malloc(total_len);
    if (payload == NULL) {
        return -1;
    }
    if (recv_full(fd, payload, total_len, 0) != (ssize_t)total_len) {
        free(payload);
        return -1;
    }
    *payload_out = payload;
    return 0;
}

static int endpoint_is_self(const char *addr, uint16_t port, uint64_t node_id)
{
    return (node_id != 0 && node_id == my_node_id) ||
           (port == cluster_advertise_port &&
            strcmp(addr, cluster_advertise_address) == 0);
}

static void free_outbox_msg(struct outbox_msg *msg) {
    if (!msg) return;
    free(msg->key);
    free(msg->value);
    free(msg);
}

static int connect_endpoint_simple(const char *addr, uint16_t port) {
    struct peer_endpoint ep;
    (void)strlcpy(ep.addr, addr, sizeof(ep.addr));
    ep.port = port;
    ep.node_id = 0;
    return connect_endpoint(&ep);
}

static void *peer_sender_thread(void *arg) {
    struct peer_conn *peer = (struct peer_conn *)arg;
    struct outbox_msg *msg;

    while (1) {
        bool active;
        int fd;

        pthread_mutex_lock(&peer->outbox_mutex);
        active = peer->active;
        fd = peer->fd;
        pthread_mutex_unlock(&peer->outbox_mutex);

        if (!active || !running) {
            break;
        }

        if (fd < 0) {
            time_t now = time(NULL);
            pthread_mutex_lock(&peer->outbox_mutex);
            if (now < peer->next_retry) {
                pthread_mutex_unlock(&peer->outbox_mutex);
                sleep(1);
                continue;
            }
            pthread_mutex_unlock(&peer->outbox_mutex);

            int new_fd = connect_endpoint_simple(peer->addr, peer->port);
            if (new_fd < 0) {
                pthread_mutex_lock(&peer->outbox_mutex);
                peer->retry_backoff = (peer->retry_backoff * 2 < PEER_RETRY_MAX) ? peer->retry_backoff * 2 : PEER_RETRY_MAX;
                peer->next_retry = now + peer->retry_backoff;
                pthread_mutex_unlock(&peer->outbox_mutex);
                continue;
            }

            pthread_mutex_lock(&peer->outbox_mutex);
            peer->fd = new_fd;
            peer->retry_backoff = PEER_RETRY_BASE;
            peer->last_activity = now;
            pthread_mutex_unlock(&peer->outbox_mutex);
        }

        pthread_mutex_lock(&peer->outbox_mutex);
        while (peer->outbox_count == 0 && peer->active && running) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            pthread_cond_timedwait(&peer->outbox_cond, &peer->outbox_mutex, &ts);
        }
        if (!peer->active || !running) {
            pthread_mutex_unlock(&peer->outbox_mutex);
            break;
        }

        msg = TAILQ_FIRST(&peer->outbox);
        if (msg) {
            if (time(NULL) - msg->enqueued_at > OUTBOX_MSG_TTL) {
                TAILQ_REMOVE(&peer->outbox, msg, entry);
                peer->outbox_count--;
                pthread_mutex_unlock(&peer->outbox_mutex);
                free_outbox_msg(msg);
                continue;
            }
        } else {
            pthread_mutex_unlock(&peer->outbox_mutex);
            continue;
        }
        pthread_mutex_unlock(&peer->outbox_mutex);

        pthread_mutex_lock(&peer->outbox_mutex);
        fd = peer->fd;
        pthread_mutex_unlock(&peer->outbox_mutex);

        int rc = cluster_send_frame(fd, msg->opcode, msg->flags,
                                    msg->key, msg->key_len,
                                    msg->value, msg->value_len,
                                    msg->timestamp, msg->node_id, msg->expire_at);
        if (rc == 0) {
            pthread_mutex_lock(&peer->outbox_mutex);
            peer->last_activity = time(NULL);
            TAILQ_REMOVE(&peer->outbox, msg, entry);
            peer->outbox_count--;
            pthread_mutex_unlock(&peer->outbox_mutex);
            free_outbox_msg(msg);
        } else {
            pthread_mutex_lock(&peer->outbox_mutex);
            if (peer->fd >= 0) {
                close(peer->fd);
                peer->fd = -1;
            }
            peer->next_retry = time(NULL) + peer->retry_backoff;
            peer->retry_backoff = (peer->retry_backoff * 2 < PEER_RETRY_MAX) ? peer->retry_backoff * 2 : PEER_RETRY_MAX;
            peer->stale_count++;
            pthread_mutex_unlock(&peer->outbox_mutex);
        }
    }

    pthread_mutex_lock(&peer->outbox_mutex);
    if (peer->fd >= 0) {
        close(peer->fd);
        peer->fd = -1;
    }
    pthread_mutex_unlock(&peer->outbox_mutex);
    return NULL;
}

void *peer_reaper_thread(void *arg)
{
    (void)arg;

    while (running) {
        int i;
        for (i = 0; running && i < 30; ++i) {
            sleep(1);
        }
        if (!running) {
            break;
        }

        time_t now = time(NULL);
        struct peer_conn *peer;

        pthread_mutex_lock(&peers_mutex);
        for (peer = TAILQ_FIRST(&peers_conn_head); peer != NULL; ) {
            struct peer_conn *next = TAILQ_NEXT(peer, entries);
            if (peer->is_seed || peer->node_id == my_node_id) {
                peer = next;
                continue;
            }

            pthread_mutex_lock(&peer->outbox_mutex);
            int pending = peer->outbox_count;
            time_t last = peer->last_activity;
            time_t now_health = peer->last_healthcheck;
            unsigned stale = peer->stale_count;
            pthread_mutex_unlock(&peer->outbox_mutex);

            if (pending == 0 && (now - last) > PEER_EVICT_TIMEOUT) {
                struct outbox_msg *msg;

                DMMR_LOG_DEBUG("Reaping idle peer %s:%u", peer->addr, peer->port);
                peer->active = false;
                pthread_mutex_lock(&peer->outbox_mutex);
                pthread_cond_broadcast(&peer->outbox_cond);
                pthread_mutex_unlock(&peer->outbox_mutex);

                if (peer->fd >= 0) {
                    close(peer->fd);
                    peer->fd = -1;
                }

                TAILQ_REMOVE(&peers_conn_head, peer, entries);
                pthread_mutex_unlock(&peers_mutex);

                pthread_join(peer->sender_thread, NULL);

                pthread_mutex_lock(&peer->outbox_mutex);
                while ((msg = TAILQ_FIRST(&peer->outbox)) != NULL) {
                    TAILQ_REMOVE(&peer->outbox, msg, entry);
                    free_outbox_msg(msg);
                }
                pthread_mutex_unlock(&peer->outbox_mutex);

                pthread_mutex_destroy(&peer->outbox_mutex);
                pthread_cond_destroy(&peer->outbox_cond);
                free(peer);

                pthread_mutex_lock(&peers_mutex);
                peer = next;
                continue;
            }

            if (pending == 0 && (now - now_health) > PEER_HEALTHCHECK_INTERVAL && stale > 0) {
                struct peer_endpoint endpoint;
                struct peer_endpoint_list ignored;
                (void)strlcpy(endpoint.addr, peer->addr, sizeof(endpoint.addr));
                endpoint.port = peer->port;
                endpoint.node_id = peer->node_id;
                (void)send_hello(&endpoint, &ignored);
                pthread_mutex_lock(&peer->outbox_mutex);
                peer->last_healthcheck = now;
                pthread_mutex_unlock(&peer->outbox_mutex);
            }

            peer = next;
        }
        pthread_mutex_unlock(&peers_mutex);
    }

    return NULL;
}

void init_peers(void)
{
    TAILQ_INIT(&peers_conn_head);
}

static void add_peer_internal(const char *addr, int port, uint64_t node_id, bool is_seed)
{
    struct peer_conn *peer;
    size_t addr_len;

    if (addr == NULL || port < 1 || port > 65535) {
        return;
    }
    addr_len = strlen(addr);
    if (addr_len == 0 || addr_len > DMMR_PEER_ADDR_MAX ||
        endpoint_is_self(addr, (uint16_t)port, node_id)) {
        return;
    }

    pthread_mutex_lock(&peers_mutex);
    TAILQ_FOREACH(peer, &peers_conn_head, entries) {
        if ((node_id != 0 && peer->node_id == node_id) ||
            (peer->port == (uint16_t)port && strcmp(peer->addr, addr) == 0)) {
            (void)strlcpy(peer->addr, addr, sizeof(peer->addr));
            peer->port = (uint16_t)port;
            if (node_id != 0) {
                peer->node_id = node_id;
            }
            if (is_seed) {
                peer->is_seed = true;
            }
            peer->last_activity = time(NULL);
            pthread_mutex_unlock(&peers_mutex);
            return;
        }
    }

    peer = calloc(1, sizeof(*peer));
    if (peer != NULL) {
        (void)strlcpy(peer->addr, addr, sizeof(peer->addr));
        peer->port = (uint16_t)port;
        peer->node_id = node_id;
        peer->fd = -1;
        peer->active = true;
        peer->is_seed = is_seed;
        peer->retry_backoff = PEER_RETRY_BASE;
        peer->last_activity = time(NULL);
        pthread_mutex_init(&peer->outbox_mutex, NULL);
        pthread_cond_init(&peer->outbox_cond, NULL);
        TAILQ_INIT(&peer->outbox);
        peer->outbox_count = 0;

        pthread_create(&peer->sender_thread, NULL, peer_sender_thread, peer);
        TAILQ_INSERT_TAIL(&peers_conn_head, peer, entries);
    }
    pthread_mutex_unlock(&peers_mutex);
}

void add_peer_with_node(const char *addr, int port, uint64_t node_id)
{
    add_peer_internal(addr, port, node_id, false);
}

void add_peer(const char *addr, int port)
{
    add_peer_internal(addr, port, 0, true);
}

void cluster_configure(const char *advertise_address, int cluster_port,
                       const char *seeds, int interval_seconds,
                       const char *name)
{
    if (advertise_address != NULL && advertise_address[0] != '\0' &&
        strlen(advertise_address) <= DMMR_PEER_ADDR_MAX) {
        (void)strlcpy(cluster_advertise_address, advertise_address,
                      sizeof(cluster_advertise_address));
    }
    if (cluster_port >= 1 && cluster_port <= 65535) {
        cluster_advertise_port = (uint16_t)cluster_port;
    }
    if (seeds != NULL) {
        (void)strlcpy(cluster_seeds, seeds, sizeof(cluster_seeds));
    }
    if (interval_seconds > 0) {
        discovery_interval_seconds = interval_seconds;
    }
    if (name != NULL && strlen(name) <= DMMR_CLUSTER_NAME_MAX) {
        (void)strlcpy(cluster_name, name, sizeof(cluster_name));
    }
}

static int connect_endpoint(const struct peer_endpoint *endpoint)
{
    struct addrinfo hints;
    struct addrinfo *results = NULL;
    struct addrinfo *candidate;
    struct timeval timeout;
    char port_text[6];
    int fd = -1;
    int rc;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    snprintf(port_text, sizeof(port_text), "%u", endpoint->port);
    rc = getaddrinfo(endpoint->addr, port_text, &hints, &results);
    if (rc != 0) {
        return -1;
    }

    timeout.tv_sec = DMMR_CLUSTER_TIMEOUT_SECONDS;
    timeout.tv_usec = 0;
    for (candidate = results; candidate != NULL; candidate = candidate->ai_next) {
        fd = socket(candidate->ai_family, candidate->ai_socktype,
                    candidate->ai_protocol);
        if (fd < 0) {
            continue;
        }
        (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        if (connect(fd, candidate->ai_addr, candidate->ai_addrlen) == 0) {
            break;
        }
        close(fd);
        fd = -1;
    }
    freeaddrinfo(results);
    return fd;
}

static size_t snapshot_peer_endpoints(struct peer_endpoint *out, size_t capacity)
{
    struct peer_conn *peer;
    size_t count = 0;

    pthread_mutex_lock(&peers_mutex);
    TAILQ_FOREACH(peer, &peers_conn_head, entries) {
        if (count == capacity) {
            break;
        }
        (void)strlcpy(out[count].addr, peer->addr, sizeof(out[count].addr));
        out[count].port = peer->port;
        out[count].node_id = peer->node_id;
        ++count;
    }
    pthread_mutex_unlock(&peers_mutex);
    return count;
}

static int send_peer_list(int fd)
{
    struct peer_endpoint peers[DMMR_MAX_DISCOVERED_PEERS];
    size_t peer_count;
    size_t total_len = sizeof(uint16_t);
    size_t offset;
    uint8_t *payload;
    uint16_t net_count;
    size_t i;

    peer_count = snapshot_peer_endpoints(peers, DMMR_MAX_DISCOVERED_PEERS);
    for (i = 0; i < peer_count; ++i) {
        total_len += sizeof(uint64_t) + sizeof(uint16_t) + sizeof(uint8_t) +
                     strlen(peers[i].addr);
    }
    if (total_len > MAX_VALUE_LEN) {
        return -1;
    }

    payload = malloc(total_len);
    if (payload == NULL) {
        return -1;
    }
    net_count = htons((uint16_t)peer_count);
    memcpy(payload, &net_count, sizeof(net_count));
    offset = sizeof(net_count);
    for (i = 0; i < peer_count; ++i) {
        uint64_t net_node_id = htonll(peers[i].node_id);
        uint16_t net_port = htons(peers[i].port);
        uint8_t addr_len = (uint8_t)strlen(peers[i].addr);

        memcpy(payload + offset, &net_node_id, sizeof(net_node_id));
        offset += sizeof(net_node_id);
        memcpy(payload + offset, &net_port, sizeof(net_port));
        offset += sizeof(net_port);
        payload[offset++] = addr_len;
        memcpy(payload + offset, peers[i].addr, addr_len);
        offset += addr_len;
    }

    i = cluster_send_frame(fd, OP_CLUSTER_PEER_LIST, FLAG_NONE, NULL, 0,
                           payload, (uint32_t)total_len, 0, my_node_id, 0);
    free(payload);
    return (int)i;
}

static int parse_peer_list(const uint8_t *payload, size_t payload_len,
                           struct peer_endpoint_list *list)
{
    uint16_t net_count;
    size_t offset = 0;
    uint16_t count;
    size_t i;

    if (payload == NULL || payload_len < sizeof(net_count) || list == NULL) {
        return -1;
    }
    memcpy(&net_count, payload, sizeof(net_count));
    count = ntohs(net_count);
    offset += sizeof(net_count);
    list->count = 0;

    for (i = 0; i < count; ++i) {
        uint64_t net_node_id;
        uint16_t net_port;
        uint8_t addr_len;
        struct peer_endpoint endpoint;

        if (offset + sizeof(net_node_id) + sizeof(net_port) + sizeof(addr_len) >
            payload_len) {
            return -1;
        }
        memcpy(&net_node_id, payload + offset, sizeof(net_node_id));
        offset += sizeof(net_node_id);
        memcpy(&net_port, payload + offset, sizeof(net_port));
        offset += sizeof(net_port);
        addr_len = payload[offset++];
        if (addr_len == 0 || offset + addr_len > payload_len) {
            return -1;
        }

        memset(&endpoint, 0, sizeof(endpoint));
        memcpy(endpoint.addr, payload + offset, addr_len);
        endpoint.addr[addr_len] = '\0';
        offset += addr_len;
        endpoint.port = ntohs(net_port);
        endpoint.node_id = ntohll(net_node_id);
        add_peer_with_node(endpoint.addr, endpoint.port, endpoint.node_id);
        if (!endpoint_is_self(endpoint.addr, endpoint.port, endpoint.node_id) &&
            list->count < DMMR_MAX_DISCOVERED_PEERS) {
            list->items[list->count++] = endpoint;
        }
    }

    return offset == payload_len ? 0 : -1;
}

static int send_hello(const struct peer_endpoint *endpoint,
                      struct peer_endpoint_list *peers_out)
{
    uint16_t net_port;
    struct dmmr_cluster_frame response;
    uint8_t *payload = NULL;
    /* value = [2-byte port][1-byte name_len][name_bytes] */
    uint8_t hello_value[sizeof(uint16_t) + 1 + DMMR_CLUSTER_NAME_MAX];
    uint32_t hello_value_len;
    size_t name_len;
    int fd;
    int rc = -1;

    fd = connect_endpoint(endpoint);
    if (fd < 0) {
        return -1;
    }

    net_port = htons(cluster_advertise_port);
    name_len = strlen(cluster_name);
    memcpy(hello_value, &net_port, sizeof(net_port));
    hello_value[sizeof(net_port)] = (uint8_t)name_len;
    if (name_len > 0) {
        memcpy(hello_value + sizeof(net_port) + 1, cluster_name, name_len);
    }
    hello_value_len = (uint32_t)(sizeof(net_port) + 1 + name_len);

    if (cluster_send_frame(fd, OP_CLUSTER_HELLO, FLAG_NONE,
                           cluster_advertise_address,
                           (uint32_t)strlen(cluster_advertise_address),
                           hello_value, hello_value_len, 0, my_node_id, 0) != 0) {
        goto done;
    }
    if (cluster_recv_frame(fd, &response, &payload) != 0 ||
        response.opcode != OP_CLUSTER_PEER_LIST ||
        parse_peer_list(payload, response.value_len, peers_out) != 0) {
        goto done;
    }
    rc = 0;

done:
    free(payload);
    close(fd);
    return rc;
}

static int send_snapshot_item(const struct dmmr_db_record *record, void *arg)
{
    struct snapshot_send_ctx *ctx = arg;
    uint16_t flags = record->tombstone ? FLAG_TOMBSTONE : FLAG_NONE;

    return cluster_send_frame(ctx->fd, OP_SYNC, flags, record->key,
                              (uint32_t)record->key_len, record->value,
                              (uint32_t)record->value_len, record->timestamp,
                              record->node_id, record->expire_at);
}

static int send_snapshot(int fd)
{
    struct snapshot_send_ctx ctx;

    if (cluster_send_frame(fd, OP_CLUSTER_SYNC_BEGIN, FLAG_NONE, NULL, 0,
                           NULL, 0, 0, my_node_id, 0) != 0) {
        return -1;
    }
    ctx.fd = fd;
    if (db_snapshot_foreach(send_snapshot_item, &ctx) != DMMR_DB_APPLIED) {
        return -1;
    }
    return cluster_send_frame(fd, OP_CLUSTER_SYNC_END, FLAG_NONE, NULL, 0,
                              NULL, 0, 0, my_node_id, 0);
}

static int request_snapshot(const struct peer_endpoint *endpoint)
{
    struct dmmr_cluster_frame frame;
    uint8_t *payload = NULL;
    int fd;
    int saw_begin = 0;
    int rc = -1;

    fd = connect_endpoint(endpoint);
    if (fd < 0) {
        return -1;
    }
    if (cluster_send_frame(fd, OP_CLUSTER_SYNC_REQUEST, FLAG_NONE, NULL, 0,
                           NULL, 0, 0, my_node_id, 0) != 0) {
        goto done;
    }

    while (running) {
        free(payload);
        payload = NULL;
        if (cluster_recv_frame(fd, &frame, &payload) != 0) {
            goto done;
        }
        if (authenticate_cluster_frame(&frame, payload) != 0) {
            goto done;
        }
        if (frame.opcode == OP_CLUSTER_SYNC_BEGIN) {
            saw_begin = 1;
            continue;
        }
        if (frame.opcode == OP_SYNC && saw_begin) {
            int apply_rc = db_apply_record((const char *)payload, frame.key_len,
                                            frame.timestamp, frame.node_id,
                                            frame.expire_at,
                                            (frame.flags & FLAG_TOMBSTONE) != 0,
                                            frame.value_len > 0 ? payload + frame.key_len : NULL,
                                            frame.value_len);
            if (apply_rc == DMMR_DB_ERROR) {
                goto done;
            }
            continue;
        }
        if (frame.opcode == OP_CLUSTER_SYNC_END && saw_begin) {
            rc = 0;
            goto done;
        }
        goto done;
    }

done:
    free(payload);
    close(fd);
    return rc;
}

static int handle_cluster_hello(int fd, const struct dmmr_cluster_frame *frame,
                                const uint8_t *payload)
{
    uint16_t net_port;
    char address[DMMR_PEER_ADDR_MAX + 1];
    uint16_t port;
    uint8_t peer_name_len = 0;
    char peer_name[DMMR_CLUSTER_NAME_MAX + 1];

    /* value_len must have at least the 2-byte port */
    if (frame->node_id == 0 || frame->key_len == 0 ||
        frame->key_len > DMMR_PEER_ADDR_MAX ||
        frame->value_len < sizeof(net_port) ||
        payload == NULL) {
        return -1;
    }

    memcpy(address, payload, frame->key_len);
    address[frame->key_len] = '\0';
    memcpy(&net_port, payload + frame->key_len, sizeof(net_port));
    port = ntohs(net_port);

    /* Extract cluster name if present (new format: port + 1-byte len + name) */
    peer_name[0] = '\0';
    if (frame->value_len > sizeof(net_port)) {
        peer_name_len = payload[frame->key_len + sizeof(net_port)];
        if (peer_name_len > DMMR_CLUSTER_NAME_MAX ||
            frame->value_len < sizeof(net_port) + 1 + (uint32_t)peer_name_len) {
            return -1; /* malformed */
        }
        memcpy(peer_name,
               payload + frame->key_len + sizeof(net_port) + 1,
               peer_name_len);
        peer_name[peer_name_len] = '\0';
    }

    /* Cluster name isolation: if this node has a name set, peer must match */
    if (cluster_name[0] != '\0' && strcmp(cluster_name, peer_name) != 0) {
        DMMR_LOG_DEBUG("cluster hello rejected: name mismatch (want '%s', got '%s')",
                       cluster_name, peer_name);
        return -1;
    }

    add_peer_with_node(address, port, frame->node_id);
    return send_peer_list(fd);
}

static int handle_cluster_sync(const struct dmmr_cluster_frame *frame,
                               const uint8_t *payload)
{
    if (frame->node_id == 0 || frame->key_len == 0 || payload == NULL) {
        return -1;
    }
    return db_apply_record((const char *)payload, frame->key_len,
                           frame->timestamp, frame->node_id, frame->expire_at,
                           (frame->flags & FLAG_TOMBSTONE) != 0,
                           frame->value_len > 0 ? payload + frame->key_len : NULL,
                           frame->value_len) == DMMR_DB_ERROR ? -1 : 0;
}

static void handle_cluster_connection(int fd)
{
    struct dmmr_cluster_frame frame;
    uint8_t *payload = NULL;

    if (cluster_recv_frame(fd, &frame, &payload) == 0) {
        if (authenticate_cluster_frame(&frame, payload) != 0) {
            free(payload);
            close(fd);
            return;
        }
        switch (frame.opcode) {
            case OP_CLUSTER_HELLO:
                (void)handle_cluster_hello(fd, &frame, payload);
                break;
            case OP_CLUSTER_SYNC_REQUEST:
                if (frame.node_id != 0 && frame.key_len == 0 && frame.value_len == 0) {
                    (void)send_snapshot(fd);
                }
                break;
            case OP_SYNC:
                (void)handle_cluster_sync(&frame, payload);
                break;
            default:
                break;
        }
    }
    free(payload);
    close(fd);
}

void broadcast_sync(const char *key, size_t key_len,
                    const void *value, size_t value_len,
                    uint64_t timestamp, uint64_t node_id, uint64_t expire_at,
                    uint16_t flags)
{
    struct peer_conn *peer;

    if (key == NULL || key_len == 0 || key_len > MAX_KEY_LEN ||
        value_len > MAX_VALUE_LEN) {
        return;
    }

    pthread_mutex_lock(&peers_mutex);
    TAILQ_FOREACH(peer, &peers_conn_head, entries) {
        if (peer->node_id != 0 && peer->node_id == my_node_id) {
            continue;
        }

        struct outbox_msg *msg = malloc(sizeof(*msg));
        if (!msg) continue;
        memset(msg, 0, sizeof(*msg));
        msg->opcode = OP_SYNC;
        msg->flags = flags;
        msg->key = malloc(key_len);
        msg->value = value_len ? malloc(value_len) : NULL;
        if ((key_len && !msg->key) || (value_len && !msg->value)) {
            free_outbox_msg(msg);
            continue;
        }
        memcpy(msg->key, key, key_len);
        msg->key_len = (uint32_t)key_len;
        if (value_len) memcpy(msg->value, value, value_len);
        msg->value_len = (uint32_t)value_len;
        msg->timestamp = timestamp;
        msg->node_id = node_id;
        msg->expire_at = expire_at;
        msg->enqueued_at = time(NULL);

        pthread_mutex_lock(&peer->outbox_mutex);
        if (peer->outbox_count < OUTBOX_MAX_MSG) {
            TAILQ_INSERT_TAIL(&peer->outbox, msg, entry);
            peer->outbox_count++;
            pthread_cond_signal(&peer->outbox_cond);
        } else {
            free_outbox_msg(msg);
        }
        pthread_mutex_unlock(&peer->outbox_mutex);
    }
    pthread_mutex_unlock(&peers_mutex);
}

static int parse_seed(const char *text, struct peer_endpoint *endpoint)
{
    const char *host_start = text;
    const char *host_end;
    const char *port_text;
    char *end;
    long port;
    size_t host_len;

    if (text == NULL || text[0] == '\0') {
        return -1;
    }
    if (text[0] == '[') {
        host_start = text + 1;
        host_end = strchr(host_start, ']');
        if (host_end == NULL || host_end[1] != ':') {
            return -1;
        }
        port_text = host_end + 2;
    } else {
        host_end = strrchr(text, ':');
        if (host_end == NULL) {
            return -1;
        }
        port_text = host_end + 1;
    }
    host_len = (size_t)(host_end - host_start);
    if (host_len == 0 || host_len > DMMR_PEER_ADDR_MAX || *port_text == '\0') {
        return -1;
    }
    errno = 0;
    port = strtol(port_text, &end, 10);
    if (errno != 0 || *end != '\0' || port < 1 || port > 65535) {
        return -1;
    }
    memset(endpoint, 0, sizeof(*endpoint));
    memcpy(endpoint->addr, host_start, host_len);
    endpoint->addr[host_len] = '\0';
    endpoint->port = (uint16_t)port;
    return 0;
}

static void discover_seed(const struct peer_endpoint *seed)
{
    struct peer_endpoint_list peers;
    size_t i;

    add_peer(seed->addr, seed->port);
    if (send_hello(seed, &peers) != 0) {
        return;
    }

    /* Every discovered peer receives HELLO, so it can broadcast to us too. */
    for (i = 0; i < peers.count; ++i) {
        struct peer_endpoint_list ignored;
        if (peers.items[i].port != seed->port ||
            strcmp(peers.items[i].addr, seed->addr) != 0) {
            (void)send_hello(&peers.items[i], &ignored);
        }
    }

    if (!bootstrap_completed && request_snapshot(seed) == 0) {
        bootstrap_completed = 1;
        DMMR_LOG_DEBUG("cluster bootstrap completed from %s:%u",
                       seed->addr, seed->port);
    }
}

static void rediscover_from_known_peers(void)
{
    struct peer_conn *peer;
    struct peer_endpoint_list known;
    size_t i;

    memset(&known, 0, sizeof(known));
    pthread_mutex_lock(&peers_mutex);
    TAILQ_FOREACH(peer, &peers_conn_head, entries) {
        if (peer->active && !peer->is_seed && known.count < DMMR_MAX_DISCOVERED_PEERS) {
            known.items[known.count].port = peer->port;
            known.items[known.count].node_id = peer->node_id;
            (void)strlcpy(known.items[known.count].addr, peer->addr, sizeof(known.items[known.count].addr));
            known.count++;
        }
    }
    pthread_mutex_unlock(&peers_mutex);

    for (i = 0; i < known.count; ++i) {
        struct peer_endpoint endpoint = known.items[i];
        struct peer_endpoint_list ignored;
        if (!endpoint_is_self(endpoint.addr, endpoint.port, endpoint.node_id)) {
            (void)send_hello(&endpoint, &ignored);
        }
    }
}

static void discover_once(void)
{
    char seeds_copy[DMMR_SEEDS_MAX];
    char *cursor;
    char *token;

    if (cluster_seeds[0] == '\0') {
        rediscover_from_known_peers();
        return;
    }
    (void)strlcpy(seeds_copy, cluster_seeds, sizeof(seeds_copy));
    cursor = seeds_copy;
    while ((token = strsep(&cursor, ",")) != NULL) {
        struct peer_endpoint seed;
        while (*token == ' ' || *token == '\t') {
            ++token;
        }
        if (parse_seed(token, &seed) == 0 &&
            !endpoint_is_self(seed.addr, seed.port, 0)) {
            discover_seed(&seed);
        }
    }
    rediscover_from_known_peers();
}

static void *discovery_routine(void *arg)
{
    (void)arg;
    while (running) {
        int elapsed;
        discover_once();
        for (elapsed = 0; elapsed < discovery_interval_seconds && running; ++elapsed) {
            sleep(1);
        }
    }
    return NULL;
}

int cluster_start_discovery(void)
{
    if (cluster_seeds[0] == '\0' || discovery_started) {
        return 0;
    }
    if (pthread_create(&discovery_thread, NULL, discovery_routine, NULL) != 0) {
        return -1;
    }
    discovery_started = 1;
    return 0;
}

void cluster_stop_discovery(void)
{
    if (discovery_started) {
        pthread_join(discovery_thread, NULL);
        discovery_started = 0;
    }
}

void cluster_close_listener(void)
{
    if (cluster_listen_fd >= 0) {
        close(cluster_listen_fd);
        cluster_listen_fd = -1;
    }
}

void close_peer_connections(void)
{
    struct peer_conn *peer;

    pthread_mutex_lock(&peers_mutex);
    while ((peer = TAILQ_FIRST(&peers_conn_head)) != NULL) {
        TAILQ_REMOVE(&peers_conn_head, peer, entries);
        pthread_mutex_unlock(&peers_mutex);

        pthread_mutex_lock(&peer->outbox_mutex);
        peer->active = false;
        pthread_cond_broadcast(&peer->outbox_cond);
        pthread_mutex_unlock(&peer->outbox_mutex);

        pthread_join(peer->sender_thread, NULL);

        pthread_mutex_lock(&peer->outbox_mutex);
        struct outbox_msg *msg;
        while ((msg = TAILQ_FIRST(&peer->outbox)) != NULL) {
            TAILQ_REMOVE(&peer->outbox, msg, entry);
            free_outbox_msg(msg);
        }
        pthread_mutex_unlock(&peer->outbox_mutex);

        pthread_mutex_destroy(&peer->outbox_mutex);
        pthread_cond_destroy(&peer->outbox_cond);
        free(peer);

        pthread_mutex_lock(&peers_mutex);
    }
    pthread_mutex_unlock(&peers_mutex);
}

void *cluster_listener(void *arg)
{
    int port = *(int *)arg;
    int listen_fd;
    int opt = 1;
    struct sockaddr_in addr;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("cluster_listener socket");
        return NULL;
    }
    (void)setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
        listen(listen_fd, 16) != 0) {
        perror("cluster_listener bind/listen");
        close(listen_fd);
        return NULL;
    }

    cluster_listen_fd = listen_fd;
    while (running) {
        int peer_fd = accept(listen_fd, NULL, NULL);
        if (peer_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (running) {
                perror("cluster_listener accept");
            }
            break;
        }
        handle_cluster_connection(peer_fd);
    }

    close(listen_fd);
    if (cluster_listen_fd == listen_fd) {
        cluster_listen_fd = -1;
    }
    return NULL;
}
