#include "dmmr_cluster.h"
#include "dmmr_config.h"
#include "dmmr_protocol.h"
#include "dmmr_net.h"
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
#include <sys/queue.h>

extern uint64_t my_node_id;
extern volatile sig_atomic_t running;
extern int process_frame(int fd, struct dmmr_frame *frame, const uint8_t *payload,
                         uint64_t source_node_id, bool from_peer);
extern int read_frame(int fd, struct dmmr_frame *frame, uint8_t **payload,
                      bool *is_legacy, uint16_t *legacy_opcode, uint16_t *legacy_key_len);

struct peer {
    char addr[64];
    int port;
    TAILQ_ENTRY(peer) entries;
};
TAILQ_HEAD(peer_list, peer) peers_head;
pthread_mutex_t peers_mutex = PTHREAD_MUTEX_INITIALIZER;

void init_peers(void) {
    TAILQ_INIT(&peers_head);
}

void add_peer(const char *addr, int port) {
    struct peer *p = malloc(sizeof(*p));
    if (!p) return;
    strncpy(p->addr, addr, sizeof(p->addr)-1);
    p->addr[sizeof(p->addr)-1] = '\0';
    p->port = port;
    pthread_mutex_lock(&peers_mutex);
    TAILQ_INSERT_TAIL(&peers_head, p, entries);
    pthread_mutex_unlock(&peers_mutex);
}

void broadcast_sync(const char *key, size_t key_len,
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
    if (!payload) return;
    memcpy(payload, key, key_len);
    if (value_len > 0 && value) {
        memcpy(payload + key_len, value, value_len);
    }

    pthread_mutex_lock(&peers_mutex);
    struct peer *p;
    TAILQ_FOREACH(p, &peers_head, entries) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) continue;
        int opt = 1;
        setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(p->port);
        if (inet_pton(AF_INET, p->addr, &addr.sin_addr) <= 0) {
            close(sock);
            continue;
        }
        if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
            send_full(sock, &frame, sizeof(frame), 0);
            send_full(sock, payload, total_payload, 0);
        }
        close(sock);
    }
    pthread_mutex_unlock(&peers_mutex);
    free(payload);
}

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

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
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
            uint64_t source_node_id = (uint64_t) ntohl(peer_addr.sin_addr.s_addr) ^ ((uint64_t) ntohs(peer_addr.sin_port) << 32);
            struct dmmr_frame local_frame = frame;
            local_frame.flags = htons(FLAG_FROM_PEER);
            process_frame(peer_fd, &local_frame, payload, source_node_id, true);
        }
        if (payload) free(payload);
        close(peer_fd);
    }

    close(listen_fd);
    return NULL;
}