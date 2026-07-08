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
};

enum dmmr_flags {
    FLAG_NONE = 0,
    FLAG_FROM_PEER = 1 << 0,
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

/* Metadados armazenados com o valor no DB */
struct cache_entry {
    uint64_t timestamp;   // usado para resolução de conflitos de sincronia
    uint64_t node_id;     // origem do dado
    uint32_t value_len;
    uint64_t expire_at;   // timestamp em microssegundos quando o registro expira
};

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