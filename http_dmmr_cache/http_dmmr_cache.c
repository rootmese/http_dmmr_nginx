#include <microhttpd.h>
#include <db.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/select.h>
#include <unistd.h>
#include <signal.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <sys/queue.h>

#define DMMR_PROTO_OP_GET 1
#define DMMR_PROTO_STATUS_OK 0
#define DMMR_PROTO_STATUS_NOT_FOUND 1
#define DMMR_PROTO_STATUS_ERROR 2

#define PORT 9080
#define DB_PATH "./apikeys.db"
#define SOCK_PATH "/tmp/dmmr_cache.sock"
#define DEFAULT_WORKERS 4
#define QUEUE_MAX 128

DB *dbp = NULL;
volatile sig_atomic_t running = 1;

/* Estrutura para fila de jobs (file descriptors) */
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

/* Inicializa/abre o Berkeley DB */
int init_db() {
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

void close_db() {
    if (dbp) {
        dbp->close(dbp, 0);
    }
}

static int
handle_binary_request(int client_fd)
{
    unsigned char req_buf[1024];
    unsigned char resp_buf[4096];
    uint16_t opcode;
    uint16_t key_len;
    uint16_t status;
    uint16_t payload_len;
    char *key;
    DBT key_dbt, data_dbt;
    ssize_t n;
    int rc;

    n = recv(client_fd, req_buf, sizeof(req_buf), 0);
    if (n < 4) {
        return -1;
    }

    opcode = ntohs(*(uint16_t *) req_buf);
    key_len = ntohs(*(uint16_t *) (req_buf + sizeof(uint16_t)));

    if (opcode != DMMR_PROTO_OP_GET || key_len == 0 || (size_t) key_len > sizeof(req_buf) - 4) {
        status = DMMR_PROTO_STATUS_ERROR;
        payload_len = 0;
        goto respond;
    }

    key = (char *) (req_buf + 4);
    key[key_len] = '\0';

    memset(&key_dbt, 0, sizeof(key_dbt));
    memset(&data_dbt, 0, sizeof(data_dbt));
    key_dbt.data = key;
    key_dbt.size = key_len;

    rc = dbp->get(dbp, NULL, &key_dbt, &data_dbt, 0);
    if (rc == 0) {
        status = DMMR_PROTO_STATUS_OK;
        payload_len = (uint16_t) data_dbt.size;
        if (payload_len > sizeof(resp_buf) - 4) {
            payload_len = (uint16_t) (sizeof(resp_buf) - 4);
        }
        memcpy(resp_buf + 4, data_dbt.data, payload_len);
    } else if (rc == DB_NOTFOUND) {
        status = DMMR_PROTO_STATUS_NOT_FOUND;
        payload_len = 0;
    } else {
        status = DMMR_PROTO_STATUS_ERROR;
        payload_len = 0;
    }

respond:
    status = htons(status);
    payload_len = htons(payload_len);
    memcpy(resp_buf, &status, sizeof(status));
    memcpy(resp_buf + sizeof(status), &payload_len, sizeof(payload_len));

    send(client_fd, resp_buf, 4 + payload_len, 0);
    return 0;
}

/* Insere um job na fila (thread-safe) */
static void
enqueue_job(int fd)
{
    struct job_entry *job = malloc(sizeof(*job));
    if (!job) {
        close(fd);
        return;
    }
    job->fd = fd;

    pthread_mutex_lock(&queue_mutex);
    TAILQ_INSERT_TAIL(&queue_head, job, entries);
    queue_size++;
    pthread_cond_signal(&queue_cond);
    pthread_mutex_unlock(&queue_mutex);
}

/* Worker thread: retira jobs da fila e processa */
static void *
worker_routine(void *arg)
{
    struct job_entry *job;

    while (running) {
        pthread_mutex_lock(&queue_mutex);
        while (queue_size == 0 && running) {
            pthread_cond_wait(&queue_cond, &queue_mutex);
        }
        if (!running) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }
        job = TAILQ_FIRST(&queue_head);
        TAILQ_REMOVE(&queue_head, job, entries);
        queue_size--;
        pthread_mutex_unlock(&queue_mutex);

        handle_binary_request(job->fd);
        close(job->fd);
        free(job);
    }

    /* Processa eventuais jobs restantes antes de sair (se parou por sinal) */
    pthread_mutex_lock(&queue_mutex);
    while ((job = TAILQ_FIRST(&queue_head)) != NULL) {
        TAILQ_REMOVE(&queue_head, job, entries);
        queue_size--;
        pthread_mutex_unlock(&queue_mutex);
        handle_binary_request(job->fd);
        close(job->fd);
        free(job);
        pthread_mutex_lock(&queue_mutex);
    }
    pthread_mutex_unlock(&queue_mutex);
    return NULL;
}

static int
wait_for_connection(int *listen_fds, int listen_count, fd_set *readfds)
{
    int max_fd = -1;
    int i;

    FD_ZERO(readfds);

    for (i = 0; i < listen_count; i++) {
        if (listen_fds[i] >= 0) {
            FD_SET(listen_fds[i], readfds);
            if (listen_fds[i] > max_fd) {
                max_fd = listen_fds[i];
            }
        }
    }

    if (max_fd < 0) {
        return -1;
    }

    return select(max_fd + 1, readfds, NULL, NULL, NULL);
}

static void
signal_handler(int sig)
{
    running = 0;
}

int main(int argc, char *argv[]) {
    struct MHD_Daemon *daemon_unix = NULL;
    struct MHD_Daemon *daemon_tcp = NULL;
    int use_unix = 0, use_tcp = 0;
    int fd = -1;
    int tcp_fd = -1;
    int listen_fds[2] = { -1, -1 };
    int listen_count = 0;
    fd_set readfds;
    struct sockaddr_un addr;
    int i;

    /* Processa argumentos */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--unix") == 0) {
            use_unix = 1;
        } else if (strcmp(argv[i], "--tcp") == 0) {
            use_tcp = 1;
        } else if (strcmp(argv[i], "--both") == 0) {
            use_unix = 1;
            use_tcp = 1;
        } else if (strncmp(argv[i], "--workers=", 10) == 0) {
            worker_count = atoi(argv[i] + 10);
            if (worker_count < 1) worker_count = 1;
        } else if (strcmp(argv[i], "--help") == 0) {
            printf("Uso: %s [--unix] [--tcp] [--both] [--workers=N]\n", argv[0]);
            printf("Padrão: apenas UNIX socket, %d workers\n", DEFAULT_WORKERS);
            return 0;
        }
    }

    /* Padrão: Unix se nada for escolhido */
    if (!use_unix && !use_tcp) {
        use_unix = 1;
    }

    /* Captura sinais */
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    if (init_db() != 0) {
        return 1;
    }

    /* Inicializa fila de jobs */
    TAILQ_INIT(&queue_head);

    /* Cria threads trabalhadoras */
    worker_threads = malloc(sizeof(pthread_t) * worker_count);
    for (i = 0; i < worker_count; i++) {
        if (pthread_create(&worker_threads[i], NULL, worker_routine, NULL) != 0) {
            perror("pthread_create");
            running = 0;
            break;
        }
    }
    worker_count = i; /* número real de threads iniciadas */

    /* Inicia Unix socket */
    if (use_unix) {
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            perror("socket");
            goto shutdown;
        }

        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);
        unlink(SOCK_PATH);

        if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            perror("bind");
            close(fd);
            goto shutdown;
        }

        if (listen(fd, SOMAXCONN) < 0) {
            perror("listen");
            close(fd);
            unlink(SOCK_PATH);
            goto shutdown;
        }

        listen_fds[listen_count++] = fd;
        printf("Escutando em UNIX socket: %s\n", SOCK_PATH);
    }

    /* Inicia TCP */
    if (use_tcp) {
        tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in tcp_addr;
        memset(&tcp_addr, 0, sizeof(tcp_addr));
        tcp_addr.sin_family = AF_INET;
        tcp_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        tcp_addr.sin_port = htons(PORT);

        if (bind(tcp_fd, (struct sockaddr *)&tcp_addr, sizeof(tcp_addr)) < 0) {
            perror("bind tcp");
            goto shutdown;
        }

        if (listen(tcp_fd, SOMAXCONN) < 0) {
            perror("listen tcp");
            close(tcp_fd);
            goto shutdown;
        }

        printf("Escutando em TCP: porta %d\n", PORT);
        listen_fds[listen_count++] = tcp_fd;
    }

    printf("Usando %d workers\n", worker_count);

    /* Loop principal: aceita conexões e enfileira */
    while (running) {
        int ready = wait_for_connection(listen_fds, listen_count, &readfds);
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

    /* Encerramento: sinaliza workers para parar */
    running = 0;
    pthread_mutex_lock(&queue_mutex);
    pthread_cond_broadcast(&queue_cond);
    pthread_mutex_unlock(&queue_mutex);

    for (i = 0; i < worker_count; i++) {
        pthread_join(worker_threads[i], NULL);
    }
    free(worker_threads);

shutdown:
    if (fd >= 0) {
        close(fd);
        unlink(SOCK_PATH);
    }
    if (tcp_fd >= 0) {
        close(tcp_fd);
    }
    close_db();
    return 0;
}