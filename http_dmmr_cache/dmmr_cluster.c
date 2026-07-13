#include "dmmr_cluster.h"
#include "dmmr_config.h"
#include "dmmr_protocol.h"
#include "dmmr_net.h"
#include "dmmr_pool.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/queue.h>

extern uint64_t my_node_id;
extern volatile sig_atomic_t running;
extern int process_frame(int fd, struct dmmr_frame *frame, const uint8_t *payload,
                         uint64_t source_node_id, bool from_peer);
extern int read_frame(int fd, struct dmmr_frame *frame, uint8_t **payload,
                      bool *is_legacy, uint16_t *legacy_opcode, uint16_t *legacy_key_len);

/* ============================================================
 * (D) Peers com conexões persistentes
 * ============================================================ */
struct peer {
    char addr[64];
    int port;
    int sock;    /* conexão persistente (-1 = desconectado) */
    TAILQ_ENTRY(peer) entries;
};
TAILQ_HEAD(peer_list, peer) peers_head;
pthread_mutex_t peers_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Descritor do socket de cluster (GLOBAL, não static) */
int cluster_listen_fd = -1;

void init_peers(void) {
    TAILQ_INIT(&peers_head);
}

void add_peer(const char *addr, int port) {
    struct peer *p = malloc(sizeof(*p));
    if (!p) return;
    strncpy(p->addr, addr, sizeof(p->addr)-1);
    p->addr[sizeof(p->addr)-1] = '\0';
    p->port = port;
    p->sock = -1;  /* será conectado no primeiro uso */
    pthread_mutex_lock(&peers_mutex);
    TAILQ_INSERT_TAIL(&peers_head, p, entries);
    pthread_mutex_unlock(&peers_mutex);
}

/* ============================================================
 * Conexão persistente: conecta ou reutiliza socket existente
 * ============================================================ */
static int peer_connect(struct peer *p) {
    /* Se já conectado, verifica se a conexão ainda é válida */
    if (p->sock >= 0) {
        /* Teste rápido: tenta peek sem bloquear */
        char test;
        ssize_t rc = recv(p->sock, &test, 1, MSG_PEEK | MSG_DONTWAIT);
        if (rc == 0) {
            /* Peer fechou a conexão */
            close(p->sock);
            p->sock = -1;
        } else if (rc < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            /* Erro na conexão */
            close(p->sock);
            p->sock = -1;
        }
        /* Se rc == -1 com EAGAIN/EWOULDBLOCK, a conexão está ok (sem dados) */
    }

    if (p->sock >= 0) {
        return p->sock;  /* conexão reutilizada */
    }

    /* Criar nova conexão */
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    int opt = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    /* Manter a conexão viva */
    setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(p->port);
    if (inet_pton(AF_INET, p->addr, &addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(sock);
        return -1;
    }

    p->sock = sock;
    return sock;
}

void broadcast_sync(const char *key, size_t key_len,
                    const void *value, size_t value_len,
                    uint64_t ts, uint64_t node_id) {
    (void)node_id;  /* suprime warning de parâmetro não usado */
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
    if (!payload) return;
    memcpy(payload, key, key_len);
    if (value_len > 0 && value) {
        memcpy(payload + key_len, value, value_len);
    }

    pthread_mutex_lock(&peers_mutex);
    struct peer *p;
    TAILQ_FOREACH(p, &peers_head, entries) {
        int sock = peer_connect(p);
        if (sock < 0) continue;

        /* Tenta enviar; se falhar, fecha e tenta reconectar uma vez */
        ssize_t r1 = send_full(sock, &frame, sizeof(frame), 0);
        if (r1 != (ssize_t)sizeof(frame)) {
            close(p->sock);
            p->sock = -1;
            sock = peer_connect(p);
            if (sock < 0) continue;
            if (send_full(sock, &frame, sizeof(frame), 0) != (ssize_t)sizeof(frame)) {
                close(p->sock);
                p->sock = -1;
                continue;
            }
        }
        if (send_full(sock, payload, total_payload, 0) != (ssize_t)total_payload) {
            close(p->sock);
            p->sock = -1;
        }
        /* NÃO fecha o socket: conexão persistente */
    }
    pthread_mutex_unlock(&peers_mutex);
    free(payload);
}

/* ============================================================
 * Fechar o socket de cluster externamente (para liberar a porta)
 * ============================================================ */
void cluster_close_listener(void) {
    if (cluster_listen_fd >= 0) {
        close(cluster_listen_fd);
        cluster_listen_fd = -1;
    }
}

/* ============================================================
 * Fechar todas as conexões persistentes (chamado no shutdown)
 * ============================================================ */
void close_peer_connections(void) {
    pthread_mutex_lock(&peers_mutex);
    struct peer *p;
    TAILQ_FOREACH(p, &peers_head, entries) {
        if (p->sock >= 0) {
            close(p->sock);
            p->sock = -1;
        }
    }
    pthread_mutex_unlock(&peers_mutex);
}

/* ============================================================
 * Thread listener para conexões de cluster (peers)
 * ============================================================ */
void *cluster_listener(void *arg) {
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

    /* Tentativas com retry para bind */
    int retries = 5;
    while (retries > 0) {
        if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
            break;
        }
        if (errno == EADDRINUSE) {
            perror("cluster_listener bind (retrying)");
            sleep(1);
            retries--;
        } else {
            perror("cluster_listener bind");
            close(listen_fd);
            return NULL;
        }
    }
    if (retries == 0) {
        fprintf(stderr, "cluster_listener bind failed after retries\n");
        close(listen_fd);
        return NULL;
    }

    if (listen(listen_fd, 16) < 0) {
        perror("cluster_listener listen");
        close(listen_fd);
        return NULL;
    }

    cluster_listen_fd = listen_fd;  /* armazena para fechamento externo */

    /* Loop principal de accept */
    while (running) {
        struct sockaddr_in peer_addr;
        socklen_t peer_len = sizeof(peer_addr);
        int peer_fd = accept(listen_fd, (struct sockaddr *)&peer_addr, &peer_len);
        if (peer_fd < 0) {
            if (errno == EINTR) continue;
            perror("cluster_listener accept");
            break;
        }

        struct dmmr_frame frame;
        uint8_t *payload = NULL;
        bool is_legacy = false;
        uint16_t legacy_opcode = 0, legacy_key_len = 0;
        int rc = read_frame(peer_fd, &frame, &payload, &is_legacy, &legacy_opcode, &legacy_key_len);
        if (rc == 0 && !is_legacy) {
            uint64_t source_node_id = (uint64_t) ntohl(peer_addr.sin_addr.s_addr) ^
                                      ((uint64_t) ntohs(peer_addr.sin_port) << 32);
            struct dmmr_frame local_frame = frame;
            local_frame.flags = htons(FLAG_FROM_PEER);
            process_frame(peer_fd, &local_frame, payload, source_node_id, true);
        }
        if (payload) {
            /* Payload vem do pool – liberar via container_of */
            struct payload_buf *pbuf = (struct payload_buf *)
                ((uint8_t *)payload - __builtin_offsetof(struct payload_buf, data));
            release_payload_buf(pbuf);
        }
        close(peer_fd);
    }

    close(listen_fd);
    cluster_listen_fd = -1;
    return NULL;
}