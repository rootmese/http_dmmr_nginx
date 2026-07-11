#include "dmmr_server.h"
#include "dmmr_config.h"
#include "dmmr_protocol.h"
#include "dmmr_net.h"
#include "dmmr_db.h"
#include "dmmr_cluster.h"
#include "dmmr_pool.h"
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
#include <sys/queue.h>
#include <time.h>
#include <db.h>
#include <fcntl.h>

#define TTL_DEFAULT (3600ULL * 1000000ULL)

/* ============================================================
 * Fila de controle (broadcast)  –  usa control_cmd_pooled do pool
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

void enqueue_broadcast(const char *key, size_t key_len,
                       const void *value, size_t value_len,
                       uint64_t ts, uint64_t node_id);

/* ============================================================
 * Fila de jobs  –  usa job_pool_entry do pool
 * ============================================================ */
struct job_queue_entry {
    struct job_pool_entry *pool_ref;   /* ponteiro para a entrada do pool */
    TAILQ_ENTRY(job_queue_entry) entries;
};
TAILQ_HEAD(job_queue_head, job_queue_entry);

/* Precisamos de uma fila TAILQ com ponteiros para pool entries.
 * Porém, para evitar malloc na fila, usamos índices.
 * Abordagem simplificada: a job_queue continua com TAILQ mas as
 * entradas vêm do pool de jobs e mantemos o fd inline. */

/* Fila de jobs simplificada: armazenamos fds diretamente */
struct job_fd_entry {
    int fd;
    TAILQ_ENTRY(job_fd_entry) entries;
};
TAILQ_HEAD(job_fd_queue, job_fd_entry);

/* Pool estático para job_fd_entry (evita malloc nos enqueue de fila) */
static struct job_fd_entry job_fd_pool_storage[QUEUE_MAX];
static int job_fd_pool_used[QUEUE_MAX];
static pthread_mutex_t job_fd_pool_lock = PTHREAD_MUTEX_INITIALIZER;

static struct job_fd_entry *get_job_fd(void) {
    pthread_mutex_lock(&job_fd_pool_lock);
    for (int i = 0; i < QUEUE_MAX; i++) {
        if (job_fd_pool_used[i] == 0) {
            job_fd_pool_used[i] = 1;
            job_fd_pool_storage[i].fd = -1;
            pthread_mutex_unlock(&job_fd_pool_lock);
            return &job_fd_pool_storage[i];
        }
    }
    pthread_mutex_unlock(&job_fd_pool_lock);
    return NULL;  /* fila cheia */
}

static void release_job_fd(struct job_fd_entry *p) {
    if (!p) return;
    pthread_mutex_lock(&job_fd_pool_lock);
    int idx = (int)(p - job_fd_pool_storage);
    if (idx >= 0 && idx < QUEUE_MAX) {
        job_fd_pool_used[idx] = 0;
        p->fd = -1;
    }
    pthread_mutex_unlock(&job_fd_pool_lock);
}

struct job_fd_queue queue_head;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;
int queue_size = 0;
int worker_count = DEFAULT_WORKERS;
pthread_t *worker_threads = NULL;

/* ============================================================
 * Garbage Collector – fila de chaves pendentes para exclusão
 * ============================================================ */
struct delete_queue gc_queue;
pthread_mutex_t gc_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t gc_cond = PTHREAD_COND_INITIALIZER;

/* Cursor persistente para TTL scan */
static DBC *gc_cursor = NULL;
static pthread_mutex_t gc_cursor_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ============================================================
 * Protótipos estáticos
 * ============================================================ */
static void enqueue_job(int fd);
static void *worker_routine(void *arg);
static void handle_client(int fd);
static void send_legacy_response(int fd, uint16_t status, uint16_t payload_len, const void *payload);
static int process_legacy_request(int fd, uint16_t opcode, uint16_t key_len, const uint8_t *payload);
static uint64_t now_micros(void);
static void *gc_worker_routine(void *arg);

/* ---------- Implementação das funções ---------- */

uint64_t now_micros(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ((uint64_t) ts.tv_sec * 1000000ULL) + (uint64_t) (ts.tv_nsec / 1000);
}

void send_legacy_response(int fd, uint16_t status, uint16_t payload_len, const void *payload) {
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

static void process_control_cmd(struct control_cmd_pooled *cmd) {
    switch (cmd->type) {
        case CMD_BROADCAST:
            broadcast_sync(cmd->key, cmd->key_len,
                           cmd->value, cmd->value_len,
                           cmd->ts, cmd->node_id);
            break;
        case CMD_SHUTDOWN:
            // já tratado pelo running
            break;
        default:
            break;
    }
}

/* ============================================================
 * (A) TTL Scan Incremental com Cursor Persistente
 * ============================================================
 * Em vez de varrer o banco inteiro, percorre TTL_SCAN_CHUNK_SIZE
 * chaves por ciclo. Registros expirados são enfileirados na
 * gc_queue para exclusão assíncrona pela thread GC.
 */
static void scan_expired_entries(void) {
    DBT key, data;
    int count = 0;
    uint64_t now = now_micros();

    pthread_mutex_lock(&gc_cursor_mutex);

    /* Criar cursor persistente na primeira chamada */
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
            /* Fim do banco: reposicionar no início (varredura circular) */
            memset(&key, 0, sizeof(key));
            memset(&data, 0, sizeof(data));
            data.flags = DB_DBT_MALLOC;
            ret = gc_cursor->get(gc_cursor, &key, &data, DB_FIRST);
            if (ret != 0) {
                /* Banco vazio ou erro */
                break;
            }
        } else if (ret != 0) {
            /* Erro inesperado: recriar cursor na próxima chamada */
            gc_cursor->close(gc_cursor);
            gc_cursor = NULL;
            break;
        }

        count++;

        if (data.size >= sizeof(struct cache_entry)) {
            struct cache_entry *entry = (struct cache_entry *)data.data;
            if (entry->expire_at < now) {
                /* Marcar para exclusão: enfileirar na gc_queue */
                struct delete_entry *de = get_delete_entry();
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

        if (data.data) {
            free(data.data);
        }
        memset(&key, 0, sizeof(key));
        memset(&data, 0, sizeof(data));
        data.flags = DB_DBT_MALLOC;
    }

    pthread_mutex_unlock(&gc_cursor_mutex);
}

/* ============================================================
 * Thread Garbage Collector – exclusão física assíncrona
 * ============================================================ */
static void *gc_worker_routine(void *arg) {
    (void)arg;

    while (running) {
        pthread_mutex_lock(&gc_mutex);

        /* Aguardar entradas ou timeout */
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

        /* Drenar a fila */
        struct delete_entry *de;
        while ((de = TAILQ_FIRST(&gc_queue)) != NULL) {
            TAILQ_REMOVE(&gc_queue, de, entries);
            pthread_mutex_unlock(&gc_mutex);

            /* Exclusão física do banco */
            db_del_key(de->key, de->key_len);

            /* Enfileirar broadcast de DEL */
            uint64_t ts_now = now_micros();
            enqueue_broadcast((const char *)de->key, de->key_len,
                              NULL, 0, ts_now, my_node_id);

            release_delete_entry(de);

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
        /* Só processa se a fila estiver acima do HIGH */
        while (control_queue_size <= HIGH_WATERMARK && running) {
            pthread_cond_wait(&broadcast_cond, &control_mutex);
        }
        if (!running) {
            pthread_mutex_unlock(&control_mutex);
            break;
        }
        /* Pega um comando */
        struct control_cmd_pooled *cmd = TAILQ_FIRST(&control_head);
        if (cmd) {
            TAILQ_REMOVE(&control_head, cmd, entries);
            control_queue_size--;
        }
        pthread_mutex_unlock(&control_mutex);

        if (cmd) {
            process_control_cmd(cmd);
            release_control_cmd(cmd);
        }
    }
    return NULL;
}

static void *control_thread_routine(void *arg) {
    (void)arg;
    time_t last_scan = 0;

    while (running) {
        /* =====================================================
         * 1. Aguarda comandos ou timeout para TTL scan
         * ===================================================== */
        pthread_mutex_lock(&control_mutex);

        /* Enquanto a fila estiver vazia e o sistema estiver rodando, espera */
        while (TAILQ_EMPTY(&control_head) && running) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 5;   // timeout de 5 segundos para escanear TTL
            pthread_cond_timedwait(&control_cond, &control_mutex, &ts);
            break;  /* sai para verificar TTL mesmo sem comando */
        }

        /* Se o sistema foi desligado, sai */
        if (!running) {
            pthread_mutex_unlock(&control_mutex);
            break;
        }

        /* =====================================================
         * 2. Processa comandos da fila
         * ===================================================== */
        struct control_cmd_pooled *cmd = TAILQ_FIRST(&control_head);
        if (cmd) {
            TAILQ_REMOVE(&control_head, cmd, entries);
            control_queue_size--;
            pthread_mutex_unlock(&control_mutex);

            /* Processa o comando (broadcast, etc.) */
            process_control_cmd(cmd);
            release_control_cmd(cmd);

            /* Após processar, verificamos se a fila ainda está grande
             * e acordamos os workers de broadcast se necessário */
            pthread_mutex_lock(&control_mutex);
            if (control_queue_size > HIGH_WATERMARK) {
                pthread_cond_broadcast(&broadcast_cond);
            }
            pthread_mutex_unlock(&control_mutex);
        } else {
            /* Fila vazia (timeout ocorreu) */
            pthread_mutex_unlock(&control_mutex);
        }

        /* =====================================================
         * 3. Escaneia entradas expiradas (TTL) a cada 5 segundos
         *    Agora com cursor persistente e scan incremental
         * ===================================================== */
        time_t now = time(NULL);
        if (now - last_scan >= 5) {
            last_scan = now;
            scan_expired_entries();
        }

        /* =====================================================
         * 4. Gerencia os workers de broadcast com histerese
         * ===================================================== */
        pthread_mutex_lock(&control_mutex);
        int qsize = control_queue_size;
        pthread_mutex_unlock(&control_mutex);

        if (qsize > HIGH_WATERMARK) {
            /* Se a fila está cheia, acorda os workers (se estiverem dormindo) */
            pthread_cond_broadcast(&broadcast_cond);
        }
        /* Nota: a condição de espera dos workers já verifica qsize,
         * então eles vão dormir sozinhos quando a fila estiver baixa. */
    }

    return NULL;
}

/* ============================================================
 * enqueue_broadcast – usa pool em vez de malloc
 * ============================================================ */
void enqueue_broadcast(const char *key, size_t key_len,
                       const void *value, size_t value_len,
                       uint64_t ts, uint64_t node_id) {
    struct control_cmd_pooled *cmd = get_control_cmd();
    if (!cmd) return;

    cmd->type = CMD_BROADCAST;
    cmd->ts = ts;
    cmd->node_id = node_id;

    /* Copiar chave inline */
    size_t klen = key_len;
    if (klen > MAX_KEY_LEN) klen = MAX_KEY_LEN;
    memcpy(cmd->key, key, klen);
    cmd->key_len = klen;

    /* Copiar valor inline */
    if (value && value_len > 0) {
        size_t vlen = value_len;
        if (vlen > MAX_VALUE_LEN) vlen = MAX_VALUE_LEN;
        memcpy(cmd->value_data, value, vlen);
        cmd->value = cmd->value_data;
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

/* ============================================================
 * process_frame – corrigido para nova assinatura de db_get_with_meta
 * ============================================================ */
int process_frame(int fd, struct dmmr_frame *frame, const uint8_t *payload,
                  uint64_t source_node_id, bool from_peer) {
    uint16_t opcode = frame->opcode;
    uint16_t flags = frame->flags;
    uint32_t key_len = frame->key_len;
    uint32_t value_len = frame->value_len;
    uint64_t ts = frame->timestamp;

    (void)flags;   /* suprime warning de variável não usada */

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
            DMMR_LOG_DEBUG("process_frame: OP_GET key='%.*s'", (int)key_len, key);
            uint64_t ts_found = 0, node_found = 0, expire_at = 0;
            void *memory = NULL;
            size_t value_len_out = 0;
            int rc = db_get_with_meta(key, key_len, &ts_found, &node_found, &memory, &value_len_out, &expire_at);
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
            DMMR_LOG_DEBUG("process_frame: %s key='%.*s', value_len=%u", (opcode == OP_SET ? "OP_SET" : "OP_SYNC"), (int)key_len, key, value_len);
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
            uint64_t expire_at = now_micros() + TTL_DEFAULT;
            int rc = db_set_with_meta(key, key_len, ts_use, node_use, value, value_len, expire_at);
            if (rc == 0) {
                status = DMMR_PROTO_STATUS_OK;
                if(!from_peer)
                    enqueue_broadcast(key, key_len, value, value_len, ts_use, node_use);
            }
            break;
        }
        case OP_DEL: {
            DMMR_LOG_DEBUG("process_frame: OP_DEL key='%.*s'", (int)key_len, key);
            int rc = db_del_key(key, key_len);
            if (rc == 0) {
                status = DMMR_PROTO_STATUS_OK;
            }
            if (!from_peer) {
                uint64_t ts_del = now_micros();
                enqueue_broadcast(key, key_len, NULL, 0, ts_del, my_node_id);
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

/* ============================================================
 * read_frame – usa pool de payload_buf em vez de malloc
 * ============================================================ */
int read_frame(int fd, struct dmmr_frame *frame, uint8_t **payload,
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

        /* Usa pool de payload_buf em vez de malloc */
        struct payload_buf *pbuf = get_payload_buf();
        if (pbuf == NULL) {
            return -4;
        }
        *payload = pbuf->data;
        pbuf->len = total_payload;

        if (recv_full(fd, *payload, total_payload, 0) != (ssize_t) total_payload) {
            release_payload_buf(pbuf);
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

    /* Usa pool de payload_buf em vez de malloc */
    struct payload_buf *pbuf = get_payload_buf();
    if (pbuf == NULL) {
        return -4;
    }
    *payload = pbuf->data;
    pbuf->len = *legacy_key_len;

    if (recv_full(fd, *payload, *legacy_key_len, 0) != (ssize_t) *legacy_key_len) {
        release_payload_buf(pbuf);
        *payload = NULL;
        return -5;
    }
    return 0;
}

/* ============================================================
 * Função auxiliar para liberar payload do pool
 * ============================================================
 * Como o payload é um ponteiro para dentro de um payload_buf,
 * precisamos recuperar o payload_buf a partir do ponteiro.
 */
static void release_payload_from_ptr(uint8_t *payload_ptr) {
    if (!payload_ptr) return;
    /* O payload_ptr aponta para o campo 'data' dentro de um payload_buf.
     * Calcula o offset para recuperar o ponteiro base da struct. */
    struct payload_buf *pbuf = (struct payload_buf *)
        ((uint8_t *)payload_ptr - offsetof(struct payload_buf, data));
    release_payload_buf(pbuf);
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
    struct dmmr_frame frame;
    uint8_t *payload = NULL;
    bool is_legacy = false;
    uint16_t legacy_opcode = 0, legacy_key_len = 0;

    int rc = read_frame(fd, &frame, &payload, &is_legacy, &legacy_opcode, &legacy_key_len);
    if (rc == 0) {
        if (is_legacy) {
            process_legacy_request(fd, legacy_opcode, legacy_key_len, payload);
        } else {
            process_frame(fd, &frame, payload, my_node_id, false);
        }
    }
    /* Libera payload de volta ao pool (em vez de free) */
    if (payload != NULL) release_payload_from_ptr(payload);
    close(fd);
}

/* ============================================================
 * enqueue_job / worker – usam pool estático de job_fd
 * ============================================================ */
void enqueue_job(int fd) {
    if (fd < 0) return;

    pthread_mutex_lock(&queue_mutex);
    if (queue_size >= QUEUE_MAX) {
        pthread_mutex_unlock(&queue_mutex);
        close(fd);
        return;
    }

    struct job_fd_entry *job = get_job_fd();
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

void *worker_routine(void *arg) {
    (void)arg;
    while (running) {
        struct job_fd_entry *job = NULL;
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
            release_job_fd(job);
        }
    }
    return NULL;
}

static void signal_handler(int sig) {
    (void)sig;
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
            "  --daemon            Roda como daemon (foreground se omitido)\n"
            "  --help              Esta mensagem\n",
            prog, PORT, DEFAULT_WORKERS, CLUSTER_PORT);
}

static inline void daemonize(void) {
    pid_t pid;

    /* Primeiro fork: sair do processo pai */
    pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(1);
    }
    if (pid > 0) {
        exit(0); /* pai sai */
    }

    /* Tornar-se líder de sessão */
    if (setsid() < 0) {
        perror("setsid");
        exit(1);
    }

    /* Segundo fork: garantir que não seja líder de sessão (opcional) */
    pid = fork();
    if (pid < 0) {
        perror("fork2");
        exit(1);
    }
    if (pid > 0) {
        exit(0);
    }

    /* Mudar para diretório raiz (opcional - comente se quiser manter o atual) */
    // chdir("/");

    /* Redirecionar stdin, stdout, stderr para /dev/null */
    int fd = open("/dev/null", O_RDWR);
    if (fd < 0) {
        perror("open /dev/null");
        /* Continua mesmo sem redirecionamento */
    } else {
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
    int cluster_port = CLUSTER_PORT;
    pthread_t cluster_thread;
    pthread_t gc_thread;
    int daemon_mode = 0;  /* 0 = foreground, 1 = daemon */

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--unix") == 0) use_unix = true;
        else if (strcmp(argv[i], "--tcp") == 0) use_tcp = true;
        else if (strcmp(argv[i], "--both") == 0) { use_unix = true; use_tcp = true; }
        else if (strncmp(argv[i], "--workers=", 10) == 0) {
            worker_count = atoi(argv[i] + 10);
            if (worker_count < 1) worker_count = 1;
        } else if (strncmp(argv[i], "--cluster-port=", 15) == 0) {
            cluster_port = atoi(argv[i] + 15);
        } else if (strncmp(argv[i], "--peer=", 7) == 0) {
            char *spec = argv[i] + 7;
            char *colon = strchr(spec, ':');
            if (colon) {
                char addr[64];
                size_t len = colon - spec;
                if (len >= sizeof(addr)) len = sizeof(addr)-1;
                memcpy(addr, spec, len);
                addr[len] = '\0';
                int port = atoi(colon+1);
                add_peer(addr, port);
            }
        } else if (strncmp(argv[i], "--node-id=", 10) == 0) {
            my_node_id = strtoull(argv[i] + 10, NULL, 0);
        } else if (strcmp(argv[i], "--daemon") == 0) {
            daemon_mode = 1;
        } else if (strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        }
    }

    if (!use_unix && !use_tcp) use_unix = true;
    if (my_node_id == 0) {
        my_node_id = (uint64_t) time(NULL) ^ ((uint64_t) getpid() << 32);
    }

    if (daemon_mode) {
        daemonize();
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    /* Inicializar pools de objetos (antes de qualquer uso) */
    if (init_pools() != 0) {
        fprintf(stderr, "Falha ao inicializar pools\n");
        return 1;
    }

    if (init_db() != 0) {
        destroy_pools();
        return 1;
    }

    TAILQ_INIT(&queue_head);
    TAILQ_INIT(&control_head);
    TAILQ_INIT(&gc_queue);
    memset(job_fd_pool_used, 0, sizeof(job_fd_pool_used));

    init_peers();

    worker_threads = malloc(sizeof(pthread_t) * worker_count);
    if (!worker_threads) {
        fprintf(stderr, "Falha ao alocar workers\n");
        close_db();
        destroy_pools();
        return 1;
    }

    for (int i = 0; i < worker_count; i++) {
        if (pthread_create(&worker_threads[i], NULL, worker_routine, NULL) != 0) {
            perror("pthread_create worker");
            running = 0;
            break;
        }
    }

    if (pthread_create(&cluster_thread, NULL, cluster_listener, &cluster_port) != 0) {
        perror("pthread_create cluster");
        running = 0;
    }

    /* Cria sockets de escuta */
    if (use_unix) {
        unix_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (unix_fd >= 0) {
            struct sockaddr_un addr;
            memset(&addr, 0, sizeof(addr));
            addr.sun_family = AF_UNIX;
            strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);
            unlink(SOCK_PATH);
            if (bind(unix_fd, (struct sockaddr *)&addr, sizeof(addr)) == 0 &&
                listen(unix_fd, SOMAXCONN) == 0) {
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

            struct sockaddr_in addr;
            memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            addr.sin_port = htons(PORT);
            if (bind(tcp_fd, (struct sockaddr *)&addr, sizeof(addr)) == 0 &&
                listen(tcp_fd, SOMAXCONN) == 0) {
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

    /* Threads de controle, broadcast e GC */
    pthread_t control_thread;
    pthread_create(&control_thread, NULL, control_thread_routine, NULL);
    for (int i = 0; i < BROADCAST_WORKERS; i++) {
        pthread_create(&broadcast_workers[i], NULL, broadcast_worker_routine, NULL);
    }
    /* Thread do Garbage Collector */
    pthread_create(&gc_thread, NULL, gc_worker_routine, NULL);

/* ============================================================
 * (C) Loop principal de accept – usando select() (POSIX)
 * ============================================================ */
fd_set readfds;
int max_fd = -1;

/* Encontra o maior descritor para o select */
for (int i = 0; i < listen_count; i++) {
    if (listen_fds[i] > max_fd) {
        max_fd = listen_fds[i];
    }
}

while (running) {
    FD_ZERO(&readfds);
    for (int i = 0; i < listen_count; i++) {
        if (listen_fds[i] >= 0) {
            FD_SET(listen_fds[i], &readfds);
        }
    }

    /* Timeout de 1 segundo para verificar running periodicamente */
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;

    int ready = select(max_fd + 1, &readfds, NULL, NULL, &tv);
    if (ready < 0) {
        if (errno == EINTR) continue;
        perror("select");
        break;
    }
    if (ready == 0) {
        /* Timeout: apenas reavalia running */
        continue;
    }

    for (int i = 0; i < listen_count; i++) {
        if (listen_fds[i] >= 0 && FD_ISSET(listen_fds[i], &readfds)) {
            struct sockaddr_storage client_addr;
            socklen_t addrlen = sizeof(client_addr);
            int client_fd = accept(listen_fds[i], (struct sockaddr *)&client_addr, &addrlen);
            if (client_fd < 0) {
                if (errno != EAGAIN && errno != EWOULDBLOCK)
                    perror("accept");
                continue;
            }
            enqueue_job(client_fd);
        }
    }
}

    /* ============================================================
     * Shutdown
     * ============================================================ */
    running = 0;

    /* Acorda a thread de controle */
    pthread_cond_signal(&control_cond);
    pthread_join(control_thread, NULL);

    /* Acorda os broadcast workers */
    pthread_cond_broadcast(&broadcast_cond);
    for (int i = 0; i < BROADCAST_WORKERS; i++) {
        pthread_join(broadcast_workers[i], NULL);
    }

    /* Acorda o GC */
    pthread_cond_signal(&gc_cond);
    pthread_join(gc_thread, NULL);

    /* Fechar cursor persistente do GC */
    pthread_mutex_lock(&gc_cursor_mutex);
    if (gc_cursor != NULL) {
        gc_cursor->close(gc_cursor);
        gc_cursor = NULL;
    }
    pthread_mutex_unlock(&gc_cursor_mutex);

    /* Acorda os workers de job */
    pthread_mutex_lock(&queue_mutex);
    pthread_cond_broadcast(&queue_cond);
    pthread_mutex_unlock(&queue_mutex);

    for (int i = 0; i < worker_count; i++) {
        pthread_join(worker_threads[i], NULL);
    }
    free(worker_threads);

    pthread_join(cluster_thread, NULL);

    /* Fechar conexões persistentes de cluster */
    close_peer_connections();

    if (unix_fd >= 0) {
        close(unix_fd);
        unlink(SOCK_PATH);
    }
    if (tcp_fd >= 0) close(tcp_fd);

    close_db();
    destroy_pools();
    return 0;
}