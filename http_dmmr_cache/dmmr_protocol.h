#ifndef DMMR_PROTOCOL_H
#define DMMR_PROTOCOL_H

#include <stdint.h>
#include <stdbool.h>

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
    uint64_t timestamp;
    uint64_t node_id;
    uint32_t value_len;
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

#endif /* DMMR_PROTOCOL_H */