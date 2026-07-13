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
void cluster_close_listener(void);
void close_peer_connections(void);

/* control_cmd e control_queue estão definidos em dmmr_protocol.h */

#endif /* DMMR_CLUSTER_H */