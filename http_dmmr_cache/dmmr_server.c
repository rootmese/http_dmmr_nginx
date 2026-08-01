#include "dmmr_server.h"
#include "dmmr_config.h"
#include "dmmr_protocol.h"
#include "dmmr_net.h"
#include "dmmr_db.h"
#include "dmmr_cluster.h"
#include "dmmr_pool.h"
#include "dmmr_heap_pool.h"
#include "dmmr_string.h"
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/stat.h>
#include <sys/queue.h>
#include <time.h>
#include <db.h>
#include <fcntl.h>

#define TTL_DEFAULT (3600ULL * 1000000ULL)

extern int cluster_listen_fd;

/* ============================================================
 * Estruturas de tamanho fixo (definidas localmente)
 * ============================================================ */
struct job_fd_entry {
    int fd;
    TAILQ_ENTRY(job_fd_entry) entries;
};

struct control_cmd_pooled {
    int       in_use;
    int       type;
    uint64_t  ts;
    uint64_t  node_id;
    uint64_t  expire_at;
    uint16_t  flags;
    char      key[MAX_KEY_LEN];
    size_t    key_len;
    uint8_t  *value;
    size_t    value_len;
    TAILQ_ENTRY(control_cmd_pooled) entries;
};

struct delete_entry {
    int    in_use;
    char   key[MAX_KEY_LEN];
    size_t key_len;
    TAILQ_ENTRY(delete_entry) entries;
};

TAILQ_HEAD(control_queue_pooled, control_cmd_pooled);
TAILQ_HEAD(delete_queue, delete_entry);
TAILQ_HEAD(job_fd_queue, job_fd_entry);

/* ============================================================
 * Pools de objetos de tamanho fixo (heap pool)
 * ============================================================ */
static dmmr_heap_pool_t job_pool;   // capacidade = QUEUE_MAX
static dmmr_heap_pool_t cmd_pool;   // capacidade = QUEUE_MAX * 2
static dmmr_heap_pool_t del_pool;   // capacidade = TTL_SCAN_CHUNK_SIZE * 4

/* ============================================================
 * Fila de controle (broadcast)
 * ============================================================ */
pthread_mutex_t control_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t control_cond = PTHREAD_COND_INITIALIZER;
struct control_queue_pooled control_head;
int control_queue_size = 0;

pthread_cond_t broadcast_cond = PTHREAD_COND_INITIALIZER;
pthread_t broadcast_workers[BROADCAST_WORKERS];
int broadcast_workers_running = 0;

/* ============================================================
 * Variáveis globais
 * ============================================================ */
DB *dbp = NULL;
volatile sig_atomic_t running = 1;
uint64_t my_node_id = 0;
static uint64_t cache_request_count = 0;
static uint64_t cache_hit_count = 0;
static uint64_t cache_miss_count = 0;
static uint64_t cache_set_count = 0;
static uint64_t cache_del_count = 0;

/* ============================================================
 * Fila de jobs
 * ============================================================ */
struct job_fd_queue queue_head;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;
int queue_size = 0;
int worker_count = DEFAULT_WORKERS;
pthread_t *worker_threads = NULL;

/* ============================================================
 * Garbage Collector
 * ============================================================ */
struct delete_queue gc_queue;
pthread_mutex_t gc_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t gc_cond = PTHREAD_COND_INITIALIZER;
static DBC *gc_cursor = NULL;
static pthread_mutex_t gc_cursor_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ============================================================
 * Funções auxiliares para os pools fixos
 * ============================================================ */
static struct job_fd_entry *alloc_job(void) {
    return (struct job_fd_entry *)dmmr_heap_pool_alloc(&job_pool);
}
static void free_job(struct job_fd_entry *j) {
    dmmr_heap_pool_free(&job_pool, j);
}

static struct control_cmd_pooled *alloc_cmd(void) {
    return (struct control_cmd_pooled *)dmmr_heap_pool_alloc(&cmd_pool);
}
static void free_cmd(struct control_cmd_pooled *c) {
    if (c) {
        free(c->value);
        c->value = NULL;
        c->value_len = 0;
        dmmr_heap_pool_free(&cmd_pool, c);
    }
}

static struct delete_entry *alloc_del(void) {
    return (struct delete_entry *)dmmr_heap_pool_alloc(&del_pool);
}
static void free_del(struct delete_entry *d) {
    dmmr_heap_pool_free(&del_pool, d);
}

/* ============================================================
 * Protótipos estáticos
 * ============================================================ */
static void enqueue_job(int fd);
static void *worker_routine(void *arg);
static void handle_client(int fd);
static void send_legacy_response(int fd, uint16_t status, uint16_t payload_len, const void *payload);
static int process_legacy_request(int fd, uint16_t opcode, uint16_t key_len, const uint8_t *payload);
static void *gc_worker_routine(void *arg);
static void enqueue_broadcast(const char *key, size_t key_len, const void *value, size_t value_len, uint64_t ts, uint64_t node_id, uint64_t expire_at, uint16_t flags);

/* ---------- Implementação ---------- */

void send_legacy_response(int fd, uint16_t status, uint16_t payload_len, const void *payload) {
    uint8_t resp[4 + 4096];
    uint16_t net_status = htons(status);
    uint16_t net_payload_len = htons(payload_len);
    memcpy(resp, &net_status, sizeof(net_status));
    memcpy(resp + sizeof(net_status), &net_payload_len, sizeof(net_payload_len));
    if (payload_len > 0 && payload != NULL)
        memcpy(resp + 4, payload, payload_len);
    send_full(fd, resp, 4 + payload_len, 0);
}

static void process_control_cmd(struct control_cmd_pooled *cmd) {
    switch (cmd->type) {
        case CMD_BROADCAST:
            broadcast_sync(cmd->key, cmd->key_len, cmd->value, cmd->value_len,
                           cmd->ts, cmd->node_id, cmd->expire_at, cmd->flags);
            break;
        case CMD_SHUTDOWN:
            break;
        default:
            break;
    }
}

static void scan_expired_entries(void) {
    DBT key, data;
    int count = 0;
    uint64_t now = now_micros();

    pthread_mutex_lock(&gc_cursor_mutex);
    if (gc_cursor == NULL) {
        int ret = dbp->cursor(dbp, NULL, &gc_cursor, 0);
        if (ret != 0) {
            pthread_mutex_unlock(&gc_cursor_mutex);
            return;
        }
    }

    memset(&key, 0, sizeof(key));
    memset(&data, 0, sizeof(data));
    data.flags = DB_DBT_MALLOC;

    while (count < TTL_SCAN_CHUNK_SIZE) {
        int ret = gc_cursor->get(gc_cursor, &key, &data, DB_NEXT);
        if (ret == DB_NOTFOUND) {
            memset(&key, 0, sizeof(key));
            memset(&data, 0, sizeof(data));
            data.flags = DB_DBT_MALLOC;
            ret = gc_cursor->get(gc_cursor, &key, &data, DB_FIRST);
            if (ret != 0) break;
        } else if (ret != 0) {
            gc_cursor->close(gc_cursor);
            gc_cursor = NULL;
            break;
        }

        count++;
        if (data.size >= sizeof(struct cache_entry)) {
            struct cache_entry *entry = (struct cache_entry *)data.data;
            if (entry->expire_at < now) {
                struct delete_entry *de = alloc_del();
                if (de) {
                    size_t klen = key.size;
                    if (klen > MAX_KEY_LEN) klen = MAX_KEY_LEN;
                    memcpy(de->key, key.data, klen);
                    de->key_len = klen;
                    pthread_mutex_lock(&gc_mutex);
                    TAILQ_INSERT_TAIL(&gc_queue, de, entries);
                    pthread_cond_signal(&gc_cond);
                    pthread_mutex_unlock(&gc_mutex);
                }
            }
        }
        if (data.data) free(data.data);
        memset(&key, 0, sizeof(key));
        memset(&data, 0, sizeof(data));
        data.flags = DB_DBT_MALLOC;
    }
    pthread_mutex_unlock(&gc_cursor_mutex);
}

static void *gc_worker_routine(void *arg) {
    (void)arg;
    while (running) {
        pthread_mutex_lock(&gc_mutex);
        while (TAILQ_EMPTY(&gc_queue) && running) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += GC_FLUSH_INTERVAL_MS * 1000000L;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec += ts.tv_nsec / 1000000000L;
                ts.tv_nsec %= 1000000000L;
            }
            pthread_cond_timedwait(&gc_cond, &gc_mutex, &ts);
        }
        if (!running && TAILQ_EMPTY(&gc_queue)) {
            pthread_mutex_unlock(&gc_mutex);
            break;
        }
        struct delete_entry *de;
        while ((de = TAILQ_FIRST(&gc_queue)) != NULL) {
            TAILQ_REMOVE(&gc_queue, de, entries);
            pthread_mutex_unlock(&gc_mutex);
            db_del_key(de->key, de->key_len);
            uint64_t ts_now = now_micros();
            enqueue_broadcast((const char *)de->key, de->key_len, NULL, 0, ts_now, my_node_id, 0, FLAG_TOMBSTONE);
            free_del(de);
            pthread_mutex_lock(&gc_mutex);
        }
        pthread_mutex_unlock(&gc_mutex);
    }
    return NULL;
}

static void *broadcast_worker_routine(void *arg) {
    (void)arg;
    while (running) {
        pthread_mutex_lock(&control_mutex);
        while (control_queue_size <= HIGH_WATERMARK && running)
            pthread_cond_wait(&broadcast_cond, &control_mutex);
        if (!running) {
            pthread_mutex_unlock(&control_mutex);
            break;
        }
        struct control_cmd_pooled *cmd = TAILQ_FIRST(&control_head);
        if (cmd) {
            TAILQ_REMOVE(&control_head, cmd, entries);
            control_queue_size--;
        }
        pthread_mutex_unlock(&control_mutex);
        if (cmd) {
            process_control_cmd(cmd);
            free_cmd(cmd);
        }
    }
    return NULL;
}

static void *control_thread_routine(void *arg) {
    (void)arg;
    time_t last_scan = 0;

    while (running) {
        pthread_mutex_lock(&control_mutex);
        while (TAILQ_EMPTY(&control_head) && running) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 5;
            pthread_cond_timedwait(&control_cond, &control_mutex, &ts);
            break;
        }
        if (!running) {
            pthread_mutex_unlock(&control_mutex);
            break;
        }
        struct control_cmd_pooled *cmd = TAILQ_FIRST(&control_head);
        if (cmd) {
            TAILQ_REMOVE(&control_head, cmd, entries);
            control_queue_size--;
            pthread_mutex_unlock(&control_mutex);
            process_control_cmd(cmd);
            free_cmd(cmd);
            pthread_mutex_lock(&control_mutex);
            if (control_queue_size > HIGH_WATERMARK)
                pthread_cond_broadcast(&broadcast_cond);
            pthread_mutex_unlock(&control_mutex);
        } else {
            pthread_mutex_unlock(&control_mutex);
        }
        time_t now = time(NULL);
        if (now - last_scan >= 5) {
            last_scan = now;
            scan_expired_entries();
        }
        pthread_mutex_lock(&control_mutex);
        int qsize = control_queue_size;
        pthread_mutex_unlock(&control_mutex);
        if (qsize > HIGH_WATERMARK)
            pthread_cond_broadcast(&broadcast_cond);
    }
    return NULL;
}

void enqueue_broadcast(const char *key, size_t key_len,
                       const void *value, size_t value_len,
                       uint64_t ts, uint64_t node_id,
                       uint64_t expire_at, uint16_t flags) {
    struct control_cmd_pooled *cmd = alloc_cmd();
    if (!cmd) return;
    cmd->type = CMD_BROADCAST;
    cmd->ts = ts;
    cmd->node_id = node_id;
    cmd->expire_at = expire_at;
    cmd->flags = flags;
    size_t klen = key_len;
    if (klen > MAX_KEY_LEN) klen = MAX_KEY_LEN;
    memcpy(cmd->key, key, klen);
    cmd->key_len = klen;
    if (value && value_len > 0) {
        size_t vlen = value_len;
        if (vlen > MAX_VALUE_LEN) vlen = MAX_VALUE_LEN;
        cmd->value = malloc(vlen);
        if (!cmd->value) { free_cmd(cmd); return; }
        memcpy(cmd->value, value, vlen);
        cmd->value_len = vlen;
    } else {
        cmd->value = NULL;
        cmd->value_len = 0;
    }
    pthread_mutex_lock(&control_mutex);
    TAILQ_INSERT_TAIL(&control_head, cmd, entries);
    control_queue_size++;
    pthread_cond_signal(&control_cond);
    pthread_mutex_unlock(&control_mutex);
}

int process_frame(int fd, struct dmmr_frame *frame, const uint8_t *payload,
                  uint64_t source_node_id, bool from_peer) {
    uint16_t opcode = frame->opcode;
    uint32_t key_len = frame->key_len;
    uint32_t value_len = frame->value_len;
    uint64_t ts = frame->timestamp;
    (void)frame->flags;

    if (key_len == 0 || key_len > MAX_KEY_LEN || value_len > MAX_VALUE_LEN)
        return -1;

    const char *key = (const char *) payload;
    const void *value = (value_len > 0) ? (payload + key_len) : NULL;
    uint16_t status = DMMR_PROTO_STATUS_ERROR;
    uint8_t *response_payload = NULL;
    uint32_t response_len = 0;

    switch (opcode) {
        case OP_GET: {
            DMMR_LOG_DEBUG("process_frame: OP_GET key='%.*s'", (int)key_len, key);
            cache_request_count++;
            uint64_t ts_found = 0, node_found = 0, expire_at = 0;
            void *memory = NULL;
            size_t value_len_out = 0;
            int rc = db_get_with_meta(key, key_len, &ts_found, &node_found, &memory, &value_len_out, &expire_at);
            if (rc == DMMR_DB_APPLIED) {
                cache_hit_count++;
                status = DMMR_PROTO_STATUS_OK;
                response_payload = (uint8_t *) memory;
                response_len = (uint32_t) value_len_out;
            } else if (rc == DMMR_DB_NOT_FOUND) {
                cache_miss_count++;
                status = DMMR_PROTO_STATUS_NOT_FOUND;
            } else {
                cache_miss_count++;
                status = DMMR_PROTO_STATUS_ERROR;
            }
            break;
        }
        case OP_SET:
        case OP_SYNC: {
            DMMR_LOG_DEBUG("process_frame: %s key='%.*s', value_len=%u",
                           (opcode == OP_SET ? "OP_SET" : "OP_SYNC"), (int)key_len, key, value_len);
            uint64_t ts_use = ts;
            uint64_t node_use = source_node_id;
            if (!from_peer) {
                ts_use = now_micros();
                node_use = my_node_id;
            }
            cache_request_count++;
            cache_set_count++;
            uint64_t expire_at = now_micros() + TTL_DEFAULT;
            int rc = db_set_with_meta(key, key_len, ts_use, node_use, value, value_len, expire_at);
            if (rc == 0) {
                status = DMMR_PROTO_STATUS_OK;
                if (!from_peer)
                    enqueue_broadcast(key, key_len, value, value_len, ts_use, node_use, expire_at, FLAG_NONE);
            }
            break;
        }
        case OP_DEL: {
            DMMR_LOG_DEBUG("process_frame: OP_DEL key='%.*s'", (int)key_len, key);
            cache_request_count++;
            cache_del_count++;
            int rc = db_del_key(key, key_len);
            if (rc == 0) {
                status = DMMR_PROTO_STATUS_OK;
            }
            if (!from_peer) {
                uint64_t ts_del = now_micros();
                enqueue_broadcast(key, key_len, NULL, 0, ts_del, my_node_id, 0, FLAG_TOMBSTONE);
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

int read_frame(int fd, struct dmmr_frame *frame, uint8_t **payload,
               struct payload_buf **payload_buf,
               bool *is_legacy, uint16_t *legacy_opcode, uint16_t *legacy_key_len) {
    uint8_t prefix[4];
    *payload = NULL;
    *payload_buf = NULL;
    if (recv_full(fd, prefix, sizeof(prefix), 0) != (ssize_t) sizeof(prefix))
        return -1;

    uint16_t magic = ntohs(*(uint16_t *) prefix);
    uint16_t version = ntohs(*(uint16_t *) (prefix + 2));

    if (magic == DMMR_MAGIC && version == DMMR_VERSION) {
        *is_legacy = false;
        memcpy(frame, prefix, sizeof(prefix));
        if (recv_full(fd, ((uint8_t *) frame) + 4, sizeof(*frame) - 4, 0) != (ssize_t)(sizeof(*frame) - 4))
            return -1;
        frame->magic = ntohs(frame->magic);
        frame->version = ntohs(frame->version);
        frame->opcode = ntohs(frame->opcode);
        frame->flags = ntohs(frame->flags);
        frame->key_len = ntohl(frame->key_len);
        frame->value_len = ntohl(frame->value_len);
        frame->timestamp = ntohll(frame->timestamp);

        if (frame->magic != DMMR_MAGIC || frame->version != DMMR_VERSION) return -2;
        if (frame->key_len > MAX_KEY_LEN || frame->value_len > MAX_VALUE_LEN) return -3;

        size_t total = frame->key_len + frame->value_len;
        if (total == 0) return 0;

        struct payload_buf *pbuf = get_payload_buf(total);
        if (!pbuf) return -4;
        *payload = pbuf->data;
        *payload_buf = pbuf;
        if (recv_full(fd, *payload, total, 0) != (ssize_t)total) {
            release_payload_buf(pbuf);
            *payload = NULL;
            *payload_buf = NULL;
            return -5;
        }
        return 0;
    }

    *is_legacy = true;
    *legacy_opcode = ntohs(*(uint16_t *) prefix);
    *legacy_key_len = ntohs(*(uint16_t *) (prefix + 2));

    if (*legacy_opcode != DMMR_PROTO_OP_GET || *legacy_key_len == 0)
        return 0;

    struct payload_buf *pbuf = get_payload_buf(*legacy_key_len);
    if (!pbuf) return -4;
    *payload = pbuf->data;
    *payload_buf = pbuf;
    if (recv_full(fd, *payload, *legacy_key_len, 0) != (ssize_t)*legacy_key_len) {
        release_payload_buf(pbuf);
        *payload = NULL;
        *payload_buf = NULL;
        return -5;
    }
    return 0;
}

int process_legacy_request(int fd, uint16_t opcode, uint16_t key_len, const uint8_t *payload) {
    if (opcode != DMMR_PROTO_OP_GET || key_len == 0 || payload == NULL) {
        send_legacy_response(fd, DMMR_PROTO_STATUS_ERROR, 0, NULL);
        return -1;
    }
    DMMR_LOG_DEBUG("process_legacy_request: OP_GET legacy key='%.*s'", (int)key_len, payload);
    uint64_t ts_found = 0, node_found = 0, expire_at = 0;
    void *value = NULL;
    size_t value_len = 0;
    int rc = db_get_with_meta((const char *) payload, key_len, &ts_found, &node_found, &value, &value_len, &expire_at);
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

void handle_client(int fd) {
    while (1) {
        struct dmmr_frame frame;
        uint8_t *payload = NULL;
        struct payload_buf *pbuf = NULL;
        bool is_legacy = false;
        uint16_t legacy_opcode = 0, legacy_key_len = 0;

        int rc = read_frame(fd, &frame, &payload, &pbuf, &is_legacy, &legacy_opcode, &legacy_key_len);
        if (rc < 0) break;

        if (is_legacy)
            process_legacy_request(fd, legacy_opcode, legacy_key_len, payload);
        else
            process_frame(fd, &frame, payload, my_node_id, false);
        release_payload_buf(pbuf);
    }
    close(fd);
}

void enqueue_job(int fd) {
    if (fd < 0) return;
    pthread_mutex_lock(&queue_mutex);
    if (queue_size >= QUEUE_MAX) {
        pthread_mutex_unlock(&queue_mutex);
        close(fd);
        return;
    }
    struct job_fd_entry *job = alloc_job();
    if (!job) {
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

void *worker_routine(void *arg) {
    (void)arg;
    while (running) {
        struct job_fd_entry *job = NULL;
        pthread_mutex_lock(&queue_mutex);
        while (queue_size == 0 && running)
            pthread_cond_wait(&queue_cond, &queue_mutex);
        if (!running) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }
        job = TAILQ_FIRST(&queue_head);
        if (job) {
            TAILQ_REMOVE(&queue_head, job, entries);
            queue_size--;
        }
        pthread_mutex_unlock(&queue_mutex);
        if (job) {
            handle_client(job->fd);
            free_job(job);
        }
    }
    return NULL;
}

static void signal_handler(int sig) { (void)sig; running = 0; }

static void usage(const char *prog) {
    fprintf(stderr,
            "Uso: %s [opções]\n"
            "  --unix                 Ativa socket Unix\n"
            "  --tcp                  Ativa socket TCP na porta %d\n"
            "  --both                 Ativa ambos\n"
            "  --workers=N            Número de workers (padrão %d)\n"
            "  --cluster-port=P       Porta de cluster (padrão %d)\n"
            "  --peer=IP:PORT         Adiciona um peer estático\n"
            "  --seeds=IP:PORT,...    Seeds para discovery automático\n"
            "  --advertise=IP         Endereço anunciado ao cluster\n"
            "  --cluster-name=NAME    Nome do cluster (isolamento)\n"
            "  --node-id=N            ID do nó (64-bit)\n"
            "  --daemon               Roda como daemon (foreground se omitido)\n"
            "  --help                 Esta mensagem\n"
            "\nVariáveis de ambiente:\n"
            "  DMMR_CLUSTER_SEEDS      Seeds para discovery (ex: 10.0.0.1:9081)\n"
            "  DMMR_ADVERTISE_ADDRESS  Endereço anunciado ao cluster\n"
            "  DMMR_CLUSTER_NAME       Nome do cluster para isolamento\n"
            "  DMMR_CACHE_PORT         Porta TCP do cache (padrão %d)\n"
            "  DMMR_CLUSTER_PORT       Porta do cluster (padrão %d)\n",
            prog, PORT, DEFAULT_WORKERS, CLUSTER_PORT, PORT, CLUSTER_PORT);
}

static inline void daemonize(void) {
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); exit(1); }
    if (pid > 0) exit(0);
    if (setsid() < 0) { perror("setsid"); exit(1); }
    pid = fork();
    if (pid < 0) { perror("fork2"); exit(1); }
    if (pid > 0) exit(0);
    int fd = open("/dev/null", O_RDWR);
    if (fd >= 0) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > 2) close(fd);
    }
}

int main(int argc, char *argv[]) {
    bool use_unix = false, use_tcp = false;
    int listen_fds[2] = { -1, -1 };
    int listen_count = 0;
    int tcp_fd = -1, unix_fd = -1;
    int cache_port = dmmr_env_int("DMMR_CACHE_PORT", PORT, 1, 65535);
    int cluster_port = dmmr_env_int("DMMR_CLUSTER_PORT", CLUSTER_PORT, 1, 65535);
    const char *socket_path = dmmr_env_string("DMMR_SOCKET_PATH", SOCK_PATH);
    const char *bind_address = dmmr_env_string("DMMR_BIND_ADDRESS", "127.0.0.1");
    mode_t socket_mode = (mode_t) dmmr_env_mode("DMMR_SOCKET_MODE", 0666);
    const char *seeds = dmmr_env_string("DMMR_CLUSTER_SEEDS", "");
    const char *advertise_address = dmmr_env_string("DMMR_ADVERTISE_ADDRESS", "");
    const char *cluster_name_str = dmmr_env_string("DMMR_CLUSTER_NAME", "");
    pthread_t cluster_thread, gc_thread, reaper_thread;
    int daemon_mode = 0;

    worker_count = dmmr_env_int("DMMR_WORKERS", DEFAULT_WORKERS, 1, 1024);

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--unix") == 0) use_unix = true;
        else if (strcmp(argv[i], "--tcp") == 0) use_tcp = true;
        else if (strcmp(argv[i], "--both") == 0) { use_unix = true; use_tcp = true; }
        else if (strncmp(argv[i], "--workers=", 10) == 0) worker_count = atoi(argv[i] + 10);
        else if (strncmp(argv[i], "--cluster-port=", 15) == 0) cluster_port = atoi(argv[i] + 15);
        else if (strncmp(argv[i], "--seeds=", 8) == 0) seeds = argv[i] + 8;
        else if (strncmp(argv[i], "--advertise=", 12) == 0) advertise_address = argv[i] + 12;
        else if (strncmp(argv[i], "--cluster-name=", 15) == 0) cluster_name_str = argv[i] + 15;
        else if (strncmp(argv[i], "--peer=", 7) == 0) {
            char *spec = argv[i] + 7;
            char *colon = strchr(spec, ':');
            if (colon) {
                char addr[64]; size_t len = colon - spec;
                if (len >= sizeof(addr)) len = sizeof(addr)-1;
                memcpy(addr, spec, len); addr[len] = '\0';
                add_peer(addr, atoi(colon+1));
            }
        }
        else if (strncmp(argv[i], "--node-id=", 10) == 0) my_node_id = strtoull(argv[i] + 10, NULL, 0);
        else if (strcmp(argv[i], "--daemon") == 0) daemon_mode = 1;
        else if (strcmp(argv[i], "--help") == 0) { usage(argv[0]); return 0; }
    }

    if (!use_unix && !use_tcp) use_unix = true;
    if (my_node_id == 0) my_node_id = (uint64_t) time(NULL) ^ ((uint64_t) getpid() << 32);
    if (daemon_mode) daemonize();

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    if (dmmr_heap_pool_init(&job_pool, sizeof(struct job_fd_entry), QUEUE_MAX) != 0 ||
        dmmr_heap_pool_init(&cmd_pool, sizeof(struct control_cmd_pooled), QUEUE_MAX * 2) != 0 ||
        dmmr_heap_pool_init(&del_pool, sizeof(struct delete_entry), TTL_SCAN_CHUNK_SIZE * 4) != 0) {
        fprintf(stderr, "Falha ao inicializar pools fixos\n");
        return 1;
    }

    if (init_pools() != 0) {
        fprintf(stderr, "Falha ao inicializar payload pool\n");
        return 1;
    }

    if (init_db() != 0) {
        destroy_pools();
        return 1;
    }

    TAILQ_INIT(&queue_head);
    TAILQ_INIT(&control_head);
    TAILQ_INIT(&gc_queue);
    init_peers();

    worker_threads = malloc(sizeof(pthread_t) * worker_count);
    for (int i = 0; i < worker_count; i++)
        pthread_create(&worker_threads[i], NULL, worker_routine, NULL);

    pthread_create(&cluster_thread, NULL, cluster_listener, &cluster_port);
    pthread_create(&reaper_thread, NULL, peer_reaper_thread, NULL);

    const char *adv = advertise_address[0] ? advertise_address : bind_address;
    cluster_configure(adv, cluster_port, seeds, 10, cluster_name_str);
    cluster_start_discovery();

    if (use_unix) {
        unix_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (unix_fd >= 0) {
            struct sockaddr_un addr = { .sun_family = AF_UNIX };
            strlcpy(addr.sun_path, socket_path, sizeof(addr.sun_path));
            unlink(socket_path);
            if (bind(unix_fd, (struct sockaddr*)&addr, sizeof(addr)) == 0 && listen(unix_fd, SOMAXCONN) == 0) {
                chmod(socket_path, socket_mode);
                listen_fds[listen_count++] = unix_fd;
            } else { close(unix_fd); unix_fd = -1; }
        }
    }

    if (use_tcp) {
        tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (tcp_fd >= 0) {
            int opt = 1;
            setsockopt(tcp_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
            struct sockaddr_in addr = { .sin_family = AF_INET };
            inet_pton(AF_INET, bind_address, &addr.sin_addr);
            addr.sin_port = htons((uint16_t)cache_port);
            if (bind(tcp_fd, (struct sockaddr*)&addr, sizeof(addr)) == 0 && listen(tcp_fd, SOMAXCONN) == 0) {
                listen_fds[listen_count++] = tcp_fd;
            } else { close(tcp_fd); tcp_fd = -1; }
        }
    }

    pthread_t control_thread;
    pthread_create(&control_thread, NULL, control_thread_routine, NULL);
    for (int i = 0; i < BROADCAST_WORKERS; i++)
        pthread_create(&broadcast_workers[i], NULL, broadcast_worker_routine, NULL);
    pthread_create(&gc_thread, NULL, gc_worker_routine, NULL);

    fd_set readfds;
    int max_fd = -1;
    for (int i = 0; i < listen_count; i++)
        if (listen_fds[i] > max_fd) max_fd = listen_fds[i];

    while (running) {
        FD_ZERO(&readfds);
        for (int i = 0; i < listen_count; i++)
            if (listen_fds[i] >= 0) FD_SET(listen_fds[i], &readfds);
        struct timeval tv = { .tv_sec = 1 };
        if (select(max_fd+1, &readfds, NULL, NULL, &tv) > 0) {
            for (int i = 0; i < listen_count; i++)
                if (FD_ISSET(listen_fds[i], &readfds)) {
                    int client = accept(listen_fds[i], NULL, NULL);
                    if (client >= 0) enqueue_job(client);
                }
        }
    }

    running = 0;
    pthread_cond_signal(&control_cond);
    pthread_join(control_thread, NULL);
    pthread_cond_broadcast(&broadcast_cond);
    for (int i = 0; i < BROADCAST_WORKERS; i++) pthread_join(broadcast_workers[i], NULL);
    pthread_cond_signal(&gc_cond);
    pthread_join(gc_thread, NULL);
    pthread_mutex_lock(&queue_mutex);
    pthread_cond_broadcast(&queue_cond);
    pthread_mutex_unlock(&queue_mutex);
    for (int i = 0; i < worker_count; i++) pthread_join(worker_threads[i], NULL);
    free(worker_threads);
    cluster_stop_discovery();
    cluster_close_listener();
    pthread_join(cluster_thread, NULL);
    pthread_join(reaper_thread, NULL);
    close_peer_connections();
    if (unix_fd >= 0) { close(unix_fd); unlink(socket_path); }
    if (tcp_fd >= 0) close(tcp_fd);
    close_db();
    destroy_pools();
    dmmr_heap_pool_destroy(&job_pool);
    dmmr_heap_pool_destroy(&cmd_pool);
    dmmr_heap_pool_destroy(&del_pool);
    return 0;
}