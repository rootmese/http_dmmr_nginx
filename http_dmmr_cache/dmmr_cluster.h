#ifndef DMMR_CLUSTER_H
#define DMMR_CLUSTER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#define DMMR_CLUSTER_NAME_MAX 63
#define DMMR_CLUSTER_SECRET_MAX 128
#define AUTH_TTL_MICROS (10ULL * 60ULL * 1000000ULL)   // 10 minutos

void init_peers(void);
void add_peer(const char *addr, int port);
void add_peer_with_node(const char *addr, int port, uint64_t node_id);
void cluster_configure(const char *advertise_address, int cluster_port,
                       const char *seeds, int discovery_interval_seconds,
                       const char *cluster_name, const char *cluster_secret);
int cluster_start_discovery(void);
void cluster_stop_discovery(void);
void broadcast_sync(const char *key, size_t key_len,
                    const void *value, size_t value_len,
                    uint64_t ts, uint64_t node_id, uint64_t expire_at,
                    uint16_t flags);
void *cluster_listener(void *arg);
void cluster_close_listener(void);
void close_peer_connections(void);
void *peer_reaper_thread(void *arg);

static inline uint64_t now_micros(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ((uint64_t) ts.tv_sec * 1000000ULL) + (uint64_t) (ts.tv_nsec / 1000);
}


/* control_cmd e control_queue estão definidos em dmmr_protocol.h */

#endif /* DMMR_CLUSTER_H */
