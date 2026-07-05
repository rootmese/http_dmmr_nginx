#ifndef DMMR_CLUSTER_H
#define DMMR_CLUSTER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

void init_peers(void);
void add_peer(const char *addr, int port);
void broadcast_sync(const char *key, size_t key_len,
                    const void *value, size_t value_len,
                    uint64_t ts, uint64_t node_id);
void *cluster_listener(void *arg);

enum control_cmd_type {
    CMD_BROADCAST,
    CMD_SHUTDOWN,
    /* futuros: CMD_TTL_SCAN, CMD_STATS, etc. */
};

struct control_cmd {
    enum control_cmd_type type;
    uint64_t ts;          // timestamp do evento (para broadcast)
    uint64_t node_id;     // nó origem
    char key[MAX_KEY_LEN];
    size_t key_len;
    uint8_t *value;       // pode ser NULL (para DEL)
    size_t value_len;
    TAILQ_ENTRY(control_cmd) entries;
};
TAILQ_HEAD(control_queue, control_cmd);

#endif /* DMMR_CLUSTER_H */