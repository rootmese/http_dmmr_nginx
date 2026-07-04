#include <arpa/inet.h>
#include <db.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdbool.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define DMMR_MAGIC 0xD4D4u
#define DMMR_VERSION 1
#define DMMR_PROTO_OP_GET 1
#define DMMR_PROTO_OP_SET 2
#define DMMR_PROTO_OP_DEL 3
#define DMMR_PROTO_STATUS_OK 0
#define DMMR_PROTO_STATUS_NOT_FOUND 1
#define DMMR_PROTO_STATUS_ERROR 2

#define PORT 9080
#define CLUSTER_PORT 9081
#define DB_PATH "./apikeys.db"
#define SOCK_PATH "/tmp/dmmr_cache.sock"
#define DEFAULT_WORKERS 4
#define QUEUE_MAX 128
#define MAX_KEY_LEN 1024
#define MAX_VALUE_LEN (1024 * 1024)

enum dmmr_opcode {
    OP_GET = 1,
    OP_SET = 2,
    OP_DEL = 3,
    OP_SYNC = 4,
};

enum dmmr_flags {
    FLAG_NONE = 0,
    FLAG_FROM_PEER = 1 << 0,
};

struct dmmr_frame {
    uint16_t magic;
    uint16_t version;
    uint16_t opcode;
    uint16_t flags;
    uint32_t key_len;
    uint32_t value_len;
    uint64_t timestamp;
};

struct cache_entry {
    uint64_t timestamp;
    uint64_t node_id;
    uint32_t value_len;
};

DB *dbp = NULL;
volatile sig_atomic_t running = 1;

struct job_entry {
    int fd;
    TAILQ_ENTRY(job_entry) entries;
};
TAILQ_HEAD(job_queue, job_entry) queue_head;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;
int queue_size = 0;
int worker_count = DEFAULT_WORKERS;
pthread_t *worker_threads = NULL;

struct peer {
    char addr[64];
    int port;
    TAILQ_ENTRY(peer) entries;
};
TAILQ_HEAD(peer_list, peer) peers_head;
pthread_mutex_t peers_mutex = PTHREAD_MUTEX_INITIALIZER;
uint64_t my_node_id = 0;

static inline uint64_t htonll(uint64_t value) {
    union { uint64_t u64; uint32_t u32[2]; } v;
    v.u64 = value;
    return ((uint64_t) htonl(v.u32[0]) << 32) | htonl(v.u32[1]);
}

static inline uint64_t ntohll(uint64_t value) {
    return htonll(value);
}

static ssize_t recv_full(int fd, void *buf, size_t len, int flags) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = recv(fd, (char *) buf + total, len - total, flags);
        if (n <= 0) {
            if (n == 0) {
                return (ssize_t) total;
            }
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        total += (size_t) n;
    }
    return (ssize_t) total;
}

static ssize_t send_full(int fd, const void *buf, size_t len, int flags) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = send(fd, (const char *) buf + total, len - total, flags);
        if (n <= 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        total += (size_t) n;
    }
    return (ssize_t) total;
}

static int init_db(void) {
    int ret;
    if ((ret = db_create(&dbp, NULL, 0)) != 0) {
        fprintf(stderr, "db_create: %s\n", db_strerror(ret));
        return -1;
    }
    if ((ret = dbp->open(dbp, NULL, DB_PATH, NULL, DB_BTREE, DB_CREATE | DB_THREAD, 0644)) != 0) {
        dbp->err(dbp, ret, "Database open failed");
        return -1;
    }
    return 0;
}

static void close_db(void) {
    if (dbp != NULL) {
        dbp->close(dbp, 0);
        dbp = NULL;
    }
}

static uint64_t now_micros(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ((uint64_t) ts.tv_sec * 1000000ULL) + (uint64_t) (ts.tv_nsec / 1000);
}

static int db_get_with_meta(const char *key, size_t key_len,
                            uint64_t *ts_out, uint64_t *node_id_out,
                            void **value_out, size_t *value_len_out) {
    DBT key_dbt;
    DBT data_dbt;
    memset(&key_dbt, 0, sizeof(key_dbt));
    memset(&data_dbt, 0, sizeof(data_dbt));
    key_dbt.data = (void *) key;
    key_dbt.size = (u_int32_t) key_len;
    data_dbt.flags = DB_DBT_MALLOC;

    int rc = dbp->get(dbp, NULL, &key_dbt, &data_dbt, 0);
    if (rc == DB_NOTFOUND) {
        return -1;
    }
    if (rc != 0) {
        return -2;
    }
    if (data_dbt.size < sizeof(struct cache_entry)) {
        free(data_dbt.data);
        return -3;
    }

    struct cache_entry *entry = (struct cache_entry *) data_dbt.data;
    *ts_out = entry->timestamp;
    *node_id_out = entry->node_id;
    *value_len_out = entry->value_len;

    if (value_out != NULL) {
        *value_out = malloc(entry->value_len);
        if (*value_out == NULL) {
            free(data_dbt.data);
            return -4;
        }
        memcpy(*value_out, (uint8_t *) data_dbt.data + sizeof(struct cache_entry), entry->value_len);
    }

    free(data_dbt.data);
    return 0;
}

static int db_set_with_meta(const char *key, size_t key_len,
                            uint64_t ts, uint64_t node_id,
                            const void *value, size_t value_len) {
    DBT key_dbt;
    DBT data_dbt;
    DBT old_dbt;
    memset(&key_dbt, 0, sizeof(key_dbt));
    memset(&data_dbt, 0, sizeof(data_dbt));
    memset(&old_dbt, 0, sizeof(old_dbt));

    key_dbt.data = (void *) key;
    key_dbt.size = (u_int32_t) key_len;
    old_dbt.flags = DB_DBT_MALLOC;

    int rc = dbp->get(dbp, NULL, &key_dbt, &old_dbt, 0);
    if (rc == 0 && old_dbt.size >= sizeof(struct cache_entry)) {
        struct cache_entry *existing = (struct cache_entry *) old_dbt.data;
        if (existing->timestamp > ts || (existing->timestamp == ts && existing->node_id >= node_id)) {
            free(old_dbt.data);
            return 0;
        }
    } else if (rc != DB_NOTFOUND) {
        if (old_dbt.data != NULL) {
            free(old_dbt.data);
        }
        return -1;
    }

    size_t total_len = sizeof(struct cache_entry) + value_len;
    uint8_t *buf = malloc(total_len);
    if (buf == NULL) {
        if (old_dbt.data != NULL) {
            free(old_dbt.data);
        }
        return -2;
    }

    struct cache_entry *entry = (struct cache_entry *) buf;
    entry->timestamp = ts;
    entry->node_id = node_id;
    entry->value_len = (uint32_t) value_len;
    if (value_len > 0 && value != NULL) {
        memcpy(buf + sizeof(struct cache_entry), value, value_len);
    }

    data_dbt.data = buf;
    data_dbt.size = (u_int32_t) total_len;

    rc = dbp->put(dbp, NULL, &key_dbt, &data_dbt, 0);
    free(buf);
    if (old_dbt.data != NULL) {
        free(old_dbt.data);
    }
    return (rc == 0) ? 0 : -3;
}

static int db_del_key(const char *key, size_t key_len) {
    DBT key_dbt;
    memset(&key_dbt, 0, sizeof(key_dbt));
    key_dbt.data = (void *) key;
    key_dbt.size = (u_int32_t) key_len;
    return dbp->del(dbp, NULL, &key_dbt, 0);
}

static void send_legacy_response(int fd, uint16_t status, uint16_t payload_len, const void *payload) {
    uint8_t resp[4 + 4096];
    uint16_t net_status = htons(status);
    uint16_t net_payload_len = htons(payload_len);
    memcpy(resp, &net_status, sizeof(net_status));
    memcpy(resp + sizeof(net_status), &net_payload_len, sizeof(net_payload_len));
    if (payload_len > 0 && payload != NULL) {
        memcpy(resp + 4, payload, payload_len);
    }
    send_full(fd, resp, 4 + payload_len, 0);
}

static int process_frame(int fd, struct dmmr_frame *frame, const uint8_t *payload,
                         uint64_t source_node_id, bool from_peer) {
    uint16_t opcode = ntohs(frame->opcode);
    uint16_t flags = ntohs(frame->flags);
    uint32_t key_len = ntohl(frame->key_len);
    uint32_t value_len = ntohl(frame->value_len);
    uint64_t ts = ntohll(frame->timestamp);

    if (key_len == 0 || key_len > MAX_KEY_LEN || value_len > MAX_VALUE_LEN) {
        return -1;
    }

    const char *key = (const char *) payload;
    const void *value = (value_len > 0) ? (payload + key_len) : NULL;

    uint16_t status = DMMR_PROTO_STATUS_ERROR;
    uint8_t *response_payload = NULL;
    uint32_t response_len = 0;

    switch (opcode) {
        case OP_GET: {
            uint64_t ts_found = 0;
            uint64_t node_found = 0;
            void *memory = NULL;
            size_t value_len_out = 0;
            int rc = db_get_with_meta(key, key_len, &ts_found, &node_found, &memory, &value_len_out);
            if (rc == 0) {
                status = DMMR_PROTO_STATUS_OK;
                response_payload = (uint8_t *) memory;
                response_len = (uint32_t) value_len_out;
            } else if (rc == -1) {
                status = DMMR_PROTO_STATUS_NOT_FOUND;
            }
            break;
        }
        case OP_SET:
        case OP_SYNC: {
            if (value_len == 0) {
                status = DMMR_PROTO_STATUS_ERROR;
                break;
            }
            uint64_t ts_use = ts;
            uint64_t node_use = source_node_id;
            if (!from_peer) {
                ts_use = now_micros();
                node_use = my_node_id;
            }
            int rc = db_set_with_meta(key, key_len, ts_use, node_use, value, value_len);
            if (rc == 0) {
                status = DMMR_PROTO_STATUS_OK;
            }
            if (opcode == OP_SET && !from_peer) {
                broadcast_sync(key, key_len, value, value_len, ts_use, node_use);
            }
            break;
        }
        case OP_DEL: {
            int rc = db_del_key(key, key_len);
            if (rc == 0) {
                status = DMMR_PROTO_STATUS_OK;
            }
            if (!from_peer) {
                uint64_t ts_del = now_micros();
                broadcast_sync(key, key_len, NULL, 0, ts_del, my_node_id);
            }
            break;
        }
        default:
            status = DMMR_PROTO_STATUS_ERROR;
            break;
    }

    uint8_t header[8];
    uint16_t net_status = htons(status);
    uint32_t net_len = htonl(response_len);
    memcpy(header, &net_status, sizeof(net_status));
    memcpy(header + sizeof(net_status), &net_len, sizeof(net_len));
    memset(header + 6, 0, 2);

    if (send_full(fd, header, sizeof(header), 0) != (ssize_t) sizeof(header)) {
        free(response_payload);
        return -1;
    }
    if (response_len > 0 && response_payload != NULL) {
        if (send_full(fd, response_payload, response_len, 0) != (ssize_t) response_len) {
            free(response_payload);
            return -1;
        }
        free(response_payload);
    }
    return 0;
}

static int read_frame(int fd, struct dmmr_frame *frame, uint8_t **payload,
                      bool *is_legacy, uint16_t *legacy_opcode, uint16_t *legacy_key_len) {
    uint8_t prefix[4];
    if (recv_full(fd, prefix, sizeof(prefix), 0) != (ssize_t) sizeof(prefix)) {
        return -1;
    }

    uint16_t magic = ntohs(*(uint16_t *) prefix);
    uint16_t version = ntohs(*(uint16_t *) (prefix + 2));

    if (magic == DMMR_MAGIC && version == DMMR_VERSION) {
        *is_legacy = false;
        memcpy(frame, prefix, sizeof(prefix));
        if (recv_full(fd, ((uint8_t *) frame) + 4, sizeof(*frame) - 4, 0) != (ssize_t) (sizeof(*frame) - 4)) {
            return -1;
        }
        frame->magic = ntohs(frame->magic);
        frame->version = ntohs(frame->version);
        frame->opcode = ntohs(frame->opcode);
        frame->flags = ntohs(frame->flags);
        frame->key_len = ntohl(frame->key_len);
        frame->value_len = ntohl(frame->value_len);
        frame->timestamp = ntohll(frame->timestamp);

        if (frame->magic != DMMR_MAGIC || frame->version != DMMR_VERSION) {
            return -2;
        }
        if (frame->key_len > MAX_KEY_LEN || frame->value_len > MAX_VALUE_LEN) {
            return -3;
        }

        size_t total_payload = frame->key_len + frame->value_len;
        if (total_payload == 0) {
            *payload = NULL;
            return 0;
        }
        *payload = malloc(total_payload);
        if (*payload == NULL) {
            return -4;
        }
        if (recv_full(fd, *payload, total_payload, 0) != (ssize_t) total_payload) {
            free(*payload);
            *payload = NULL;
            return -5;
        }
        return 0;
    }

    *is_legacy = true;
    *legacy_opcode = ntohs(*(uint16_t *) prefix);
    *legacy_key_len = ntohs(*(uint16_t *) (prefix + 2));

    if (*legacy_opcode != DMMR_PROTO_OP_GET || *legacy_key_len == 0) {
        *payload = NULL;
        return 0;
    }

    *payload = malloc(*legacy_key_len);
    if (*payload == NULL) {
        return -4;
    }
    if (recv_full(fd, *payload, *legacy_key_len, 0) != (ssize_t) *legacy_key_len) {
        free(*payload);
        *payload = NULL;
        return -5;
    }
    return 0;
}

static int process_legacy_request(int fd, uint16_t opcode, uint16_t key_len, const uint8_t *payload) {
    if (opcode != DMMR_PROTO_OP_GET || key_len == 0 || payload == NULL) {
        send_legacy_response(fd, DMMR_PROTO_STATUS_ERROR, 0, NULL);
        return -1;
    }

    uint64_t ts_found = 0;
    uint64_t node_found = 0;
    void *value = NULL;
    size_t value_len = 0;
    int rc = db_get_with_meta((const char *) payload, key_len, &ts_found, &node_found, &value, &value_len);

    if (rc == 0) {
        send_legacy_response(fd, DMMR_PROTO_STATUS_OK, (uint16_t) value_len, value);
        free(value);
        return 0;
    }
    if (rc == -1) {
        send_legacy_response(fd, DMMR_PROTO_STATUS_NOT_FOUND, 0, NULL);
        return 1;
    }
    send_legacy_response(fd, DMMR_PROTO_STATUS_ERROR, 0, NULL);
    return -1;
}

static void broadcast_sync(const char *key, size_t key_len,
                           const void *value, size_t value_len,
                           uint64_t ts, uint64_t node_id) {
    struct dmmr_frame frame;
    memset(&frame, 0, sizeof(frame));
    frame.magic = htons(DMMR_MAGIC);
    frame.version = htons(DMMR_VERSION);
    frame.opcode = htons(OP_SYNC);
    frame.flags = htons(FLAG_NONE);
    frame.key_len = htonl((uint32_t) key_len);
    frame.value_len = htonl((uint32_t) value_len);
    frame.timestamp = htonll(ts);

    size_t total_payload = key_len + value_len;
    uint8_t *payload = malloc(total_payload);
    if (payload == NULL) {
        return;
    }
    memcpy(payload, key, key_len);
    if (value_len > 0 && value != NULL) {
        memcpy(payload + key_len, value, value_len);
    }

    pthread_mutex_lock(&peers_mutex);
    struct peer *peer;
    TAILQ_FOREACH(peer, &peers_head, entries) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            continue;
        }
        int opt = 1;
        setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(peer->port);
        if (inet_pton(AF_INET, peer->addr, &addr.sin_addr) <= 0) {
            close(sock);
            continue;
        }
        if (connect(sock, (struct sockaddr *) &addr, sizeof(addr)) == 0) {
            send_full(sock, &frame, sizeof(frame), 0);
            send_full(sock, payload, total_payload, 0);
        }
        close(sock);
    }
    pthread_mutex_unlock(&peers_mutex);
    free(payload);
}

static void handle_client(int fd) {
    struct dmmr_frame frame;
    uint8_t *payload = NULL;
    bool is_legacy = false;
    uint16_t legacy_opcode = 0;
    uint16_t legacy_key_len = 0;

    int rc = read_frame(fd, &frame, &payload, &is_legacy, &legacy_opcode, &legacy_key_len);
    if (rc == 0) {
        if (is_legacy) {
            process_legacy_request(fd, legacy_opcode, legacy_key_len, payload);
        } else {
            process_frame(fd, &frame, payload, my_node_id, false);
        }
    }
    if (payload != NULL) {
        free(payload);
    }
    close(fd);
}

static void enqueue_job(int fd) {
    if (fd < 0) {
        return;
    }

    pthread_mutex_lock(&queue_mutex);
    if (queue_size >= QUEUE_MAX) {
        pthread_mutex_unlock(&queue_mutex);
        close(fd);
        return;
    }

    struct job_entry *job = malloc(sizeof(*job));
    if (job == NULL) {
        pthread_mutex_unlock(&queue_mutex);
        close(fd);
        return;
    }

    job->fd = fd;
    TAILQ_INSERT_TAIL(&queue_head, job, entries);
    queue_size++;
    pthread_cond_signal(&queue_cond);
    pthread_mutex_unlock(&queue_mutex);
}

static void *worker_routine(void *arg) {
    while (running) {
        struct job_entry *job = NULL;
        pthread_mutex_lock(&queue_mutex);
        while (queue_size == 0 && running) {
            pthread_cond_wait(&queue_cond, &queue_mutex);
        }
        if (!running) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }
        job = TAILQ_FIRST(&queue_head);
        if (job != NULL) {
            TAILQ_REMOVE(&queue_head, job, entries);
            queue_size--;
        }
        pthread_mutex_unlock(&queue_mutex);

        if (job != NULL) {
            handle_client(job->fd);
            free(job);
        }
    }
    return NULL;
}

static void *cluster_listener(void *arg) {
    int port = *(int *) arg;
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("cluster_listener socket");
        return NULL;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        perror("cluster_listener bind");
        close(listen_fd);
        return NULL;
    }
    if (listen(listen_fd, 16) < 0) {
        perror("cluster_listener listen");
        close(listen_fd);
        return NULL;
    }

    while (running) {
        struct sockaddr_in peer_addr;
        socklen_t peer_len = sizeof(peer_addr);
        int peer_fd = accept(listen_fd, (struct sockaddr *) &peer_addr, &peer_len);
        if (peer_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("cluster_listener accept");
            break;
        }

        struct dmmr_frame frame;
        uint8_t *payload = NULL;
        bool is_legacy = false;
        uint16_t legacy_opcode = 0;
        uint16_t legacy_key_len = 0;
        int rc = read_frame(peer_fd, &frame, &payload, &is_legacy, &legacy_opcode, &legacy_key_len);
        if (rc == 0 && !is_legacy) {
            uint64_t source_node_id = (uint64_t) ntohl(peer_addr.sin_addr.s_addr) ^ ((uint64_t) ntohs(peer_addr.sin_port) << 32);
            struct dmmr_frame local_frame = frame;
            local_frame.flags = htons(FLAG_FROM_PEER);
            process_frame(peer_fd, &local_frame, payload, source_node_id, true);
        }
        if (payload != NULL) {
            free(payload);
        }
        close(peer_fd);
    }

    close(listen_fd);
    return NULL;
}

static void signal_handler(int sig) {
    running = 0;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Uso: %s [opções]\n"
            "  --unix              Ativa socket Unix\n"
            "  --tcp               Ativa socket TCP na porta %d\n"
            "  --both              Ativa ambos\n"
            "  --workers=N         Número de workers (padrão %d)\n"
            "  --cluster-port=P    Porta de cluster (padrão %d)\n"
            "  --peer=IP:PORT      Adiciona um peer\n"
            "  --node-id=N         ID do nó (64-bit)\n"
            "  --help              Esta mensagem\n",
            prog, PORT, DEFAULT_WORKERS, CLUSTER_PORT);
}

int main(int argc, char *argv[]) {
    bool use_unix = false;
    bool use_tcp = false;
    int listen_fds[2] = { -1, -1 };
    int listen_count = 0;
    int tcp_fd = -1;
    int unix_fd = -1;
    int cluster_port = CLUSTER_PORT;
    fd_set readfds;
    struct sockaddr_un addr;
    pthread_t cluster_thread;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--unix") == 0) {
            use_unix = true;
        } else if (strcmp(argv[i], "--tcp") == 0) {
            use_tcp = true;
        } else if (strcmp(argv[i], "--both") == 0) {
            use_unix = true;
            use_tcp = true;
        } else if (strncmp(argv[i], "--workers=", 10) == 0) {
            worker_count = atoi(argv[i] + 10);
            if (worker_count < 1) {
                worker_count = 1;
            }
        } else if (strncmp(argv[i], "--cluster-port=", 15) == 0) {
            cluster_port = atoi(argv[i] + 15);
        } else if (strncmp(argv[i], "--peer=", 7) == 0) {
            char *peer_spec = argv[i] + 7;
            char *colon = strchr(peer_spec, ':');
            if (colon != NULL) {
                struct peer *peer = malloc(sizeof(*peer));
                if (peer != NULL) {
                    size_t addr_len = (size_t) (colon - peer_spec);
                    if (addr_len >= sizeof(peer->addr)) {
                        addr_len = sizeof(peer->addr) - 1;
                    }
                    memcpy(peer->addr, peer_spec, addr_len);
                    peer->addr[addr_len] = '\0';
                    peer->port = atoi(colon + 1);

                    pthread_mutex_lock(&peers_mutex);
                    TAILQ_INSERT_TAIL(&peers_head, peer, entries);
                    pthread_mutex_unlock(&peers_mutex);
                }
            }
        } else if (strncmp(argv[i], "--node-id=", 10) == 0) {
            my_node_id = strtoull(argv[i] + 10, NULL, 0);
        } else if (strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        }
    }

    if (!use_unix && !use_tcp) {
        use_unix = true;
    }
    if (my_node_id == 0) {
        my_node_id = (uint64_t) time(NULL) ^ ((uint64_t) getpid() << 32);
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    if (init_db() != 0) {
        return 1;
    }

    TAILQ_INIT(&queue_head);
    TAILQ_INIT(&peers_head);

    worker_threads = malloc(sizeof(pthread_t) * worker_count);
    if (worker_threads == NULL) {
        fprintf(stderr, "Falha ao alocar workers\n");
        close_db();
        return 1;
    }

    for (int i = 0; i < worker_count; i++) {
        if (pthread_create(&worker_threads[i], NULL, worker_routine, NULL) != 0) {
            perror("pthread_create worker");
            running = 0;
            break;
        }
    }
    worker_count = worker_count > 0 ? worker_count : DEFAULT_WORKERS;

    if (pthread_create(&cluster_thread, NULL, cluster_listener, &cluster_port) != 0) {
        perror("pthread_create cluster");
        running = 0;
    }

    if (use_unix) {
        unix_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (unix_fd >= 0) {
            memset(&addr, 0, sizeof(addr));
            addr.sun_family = AF_UNIX;
            strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);
            unlink(SOCK_PATH);
            if (bind(unix_fd, (struct sockaddr *) &addr, sizeof(addr)) == 0 && listen(unix_fd, SOMAXCONN) == 0) {
                listen_fds[listen_count++] = unix_fd;
                printf("Unix socket: %s\n", SOCK_PATH);
            } else {
                perror("unix bind/listen");
                close(unix_fd);
                unix_fd = -1;
            }
        }
    }

    if (use_tcp) {
        tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (tcp_fd >= 0) {
            int opt = 1;
            setsockopt(tcp_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
            setsockopt(tcp_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

            struct sockaddr_in tcp_addr;
            memset(&tcp_addr, 0, sizeof(tcp_addr));
            tcp_addr.sin_family = AF_INET;
            tcp_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            tcp_addr.sin_port = htons(PORT);

            if (bind(tcp_fd, (struct sockaddr *) &tcp_addr, sizeof(tcp_addr)) == 0 && listen(tcp_fd, SOMAXCONN) == 0) {
                listen_fds[listen_count++] = tcp_fd;
                printf("TCP socket: port %d\n", PORT);
            } else {
                perror("tcp bind/listen");
                close(tcp_fd);
                tcp_fd = -1;
            }
        }
    }

    if (listen_count == 0) {
        fprintf(stderr, "Nenhum socket de escuta disponível.\n");
        running = 0;
    } else {
        printf("Workers: %d, Node ID: %llu\n", worker_count, (unsigned long long) my_node_id);
    }

    while (running) {
        int max_fd = -1;
        FD_ZERO(&readfds);
        for (int i = 0; i < listen_count; i++) {
            if (listen_fds[i] >= 0) {
                FD_SET(listen_fds[i], &readfds);
                if (listen_fds[i] > max_fd) {
                    max_fd = listen_fds[i];
                }
            }
        }
        if (max_fd < 0) {
            break;
        }

        int ready = select(max_fd + 1, &readfds, NULL, NULL, NULL);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("select");
            break;
        }

        for (int i = 0; i < listen_count; i++) {
            if (listen_fds[i] >= 0 && FD_ISSET(listen_fds[i], &readfds)) {
                struct sockaddr_storage client_addr;
                socklen_t addrlen = sizeof(client_addr);
                int client_fd = accept(listen_fds[i], (struct sockaddr *) &client_addr, &addrlen);
                if (client_fd < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        perror("accept");
                    }
                    continue;
                }
                enqueue_job(client_fd);
            }
        }
    }

    running = 0;
    pthread_mutex_lock(&queue_mutex);
    pthread_cond_broadcast(&queue_cond);
    pthread_mutex_unlock(&queue_mutex);

    for (int i = 0; i < worker_count; i++) {
        pthread_join(worker_threads[i], NULL);
    }
    free(worker_threads);

    pthread_join(cluster_thread, NULL);

    if (unix_fd >= 0) {
        close(unix_fd);
        unlink(SOCK_PATH);
    }
    if (tcp_fd >= 0) {
        close(tcp_fd);
    }
    close_db();
    return 0;
}