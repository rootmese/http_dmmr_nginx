#include "dmmr_server.h"
#include "dmmr_config.h"
#include "dmmr_protocol.h"
#include "dmmr_net.h"
#include "dmmr_db.h"
#include "dmmr_cluster.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/queue.h>
#include <time.h>

/* Variáveis globais */
DB *dbp = NULL;
volatile sig_atomic_t running = 1;
uint64_t my_node_id = 0;

/* Fila de jobs */
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

/* Protótipos estáticos */
static void enqueue_job(int fd);
static void *worker_routine(void *arg);
static void handle_client(int fd);
static void send_legacy_response(int fd, uint16_t status, uint16_t payload_len, const void *payload);
static int process_legacy_request(int fd, uint16_t opcode, uint16_t key_len, const uint8_t *payload);
static uint64_t now_micros(void);

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

int process_frame(int fd, struct dmmr_frame *frame, const uint8_t *payload,
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
            uint64_t ts_found = 0, node_found = 0;
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

int process_legacy_request(int fd, uint16_t opcode, uint16_t key_len, const uint8_t *payload) {
    if (opcode != DMMR_PROTO_OP_GET || key_len == 0 || payload == NULL) {
        send_legacy_response(fd, DMMR_PROTO_STATUS_ERROR, 0, NULL);
        return -1;
    }

    uint64_t ts_found = 0, node_found = 0;
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
    if (payload != NULL) free(payload);
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

void *worker_routine(void *arg) {
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
    bool use_unix = false, use_tcp = false;
    int listen_fds[2] = { -1, -1 };
    int listen_count = 0;
    int tcp_fd = -1, unix_fd = -1;
    int cluster_port = CLUSTER_PORT;
    pthread_t cluster_thread;

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
        } else if (strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        }
    }

    if (!use_unix && !use_tcp) use_unix = true;
    if (my_node_id == 0) {
        my_node_id = (uint64_t) time(NULL) ^ ((uint64_t) getpid() << 32);
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    if (init_db() != 0) return 1;

    TAILQ_INIT(&queue_head);
    init_peers();

    worker_threads = malloc(sizeof(pthread_t) * worker_count);
    if (!worker_threads) {
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

    /* Loop principal de accept */
    fd_set readfds;
    while (running) {
        int max_fd = -1;
        FD_ZERO(&readfds);
        for (int i = 0; i < listen_count; i++) {
            if (listen_fds[i] >= 0) {
                FD_SET(listen_fds[i], &readfds);
                if (listen_fds[i] > max_fd) max_fd = listen_fds[i];
            }
        }
        if (max_fd < 0) break;

        int ready = select(max_fd + 1, &readfds, NULL, NULL, NULL);
        if (ready < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
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

    /* Shutdown */
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
    if (tcp_fd >= 0) close(tcp_fd);

    close_db();
    return 0;
}