#ifndef DMMR_PROTOCOL_H
#define DMMR_PROTOCOL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/queue.h>
#include <arpa/inet.h>
#include "dmmr_config.h"

/* Opcodes e flags */
enum dmmr_opcode {
    OP_GET = 1,
    OP_SET = 2,
    OP_DEL = 3,
    OP_SYNC = 4,
    OP_CLUSTER_HELLO = 5,
    OP_CLUSTER_PEER_LIST = 6,
    OP_CLUSTER_SYNC_REQUEST = 7,
    OP_CLUSTER_SYNC_BEGIN = 8,
    OP_CLUSTER_SYNC_END = 9,
    OP_PING = 10,
    OP_STATUS = 11,
    OP_STATS = 12,
    OP_AUTH_REQUEST  = 13,
    OP_AUTH_RESPONSE = 14,
    OP_AUTH_OK       = 15,
};

enum dmmr_flags {
    FLAG_NONE = 0,
    FLAG_FROM_PEER = 1 << 0,
    FLAG_TOMBSTONE = 1 << 1,
};

/* Estrutura do frame (em ordem de rede) */
struct dmmr_frame {
    uint16_t magic;
    uint16_t version;
    uint16_t opcode;
    uint16_t flags;
    uint32_t key_len;
    uint32_t value_len;
    uint64_t timestamp;
};

/*
 * Cluster traffic uses a separate v3 frame so the public v1 protocol remains
 * wire-compatible. Replication must preserve the full LWW tuple, TTL and cluster isolation.
 */
#define DMMR_CLUSTER_VERSION 3
struct dmmr_cluster_frame {
    uint16_t magic;
    uint16_t version;
    uint16_t opcode;
    uint16_t flags;
    uint32_t key_len;
    uint32_t value_len;
    uint64_t timestamp;
    uint64_t node_id;
    uint64_t expire_at;
    uint32_t cluster_id;
};

static inline uint32_t dmmr_fnv1a_32(const char *str, size_t len) {
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint8_t)str[i];
        hash *= 16777619u;
    }
    return hash;
}

/* Metadados armazenados com o valor no DB */
struct cache_entry {
    uint64_t timestamp;   // usado para resolução de conflitos de sincronia
    uint64_t node_id;     // origem do dado
    uint32_t value_len;
    uint64_t expire_at;   // timestamp em microssegundos quando o registro expira
};

/* The high bit is unused because MAX_VALUE_LEN is far below 2^31. */
#define DMMR_RECORD_TOMBSTONE UINT32_C(0x80000000)
#define DMMR_RECORD_VALUE_LEN_MASK UINT32_C(0x7fffffff)

static inline bool dmmr_record_is_tombstone(const struct cache_entry *entry) {
    return (entry->value_len & DMMR_RECORD_TOMBSTONE) != 0;
}

static inline uint32_t dmmr_record_value_len(const struct cache_entry *entry) {
    return entry->value_len & DMMR_RECORD_VALUE_LEN_MASK;
}

/* Funções de conversão 64-bit */
static inline uint64_t htonll(uint64_t value) {
    union { uint64_t u64; uint32_t u32[2]; } v;
    v.u64 = value;
    return ((uint64_t) htonl(v.u32[0]) << 32) | htonl(v.u32[1]);
}

static inline uint64_t ntohll(uint64_t value) {
    return htonll(value);
}

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

#endif /* DMMR_PROTOCOL_H */
