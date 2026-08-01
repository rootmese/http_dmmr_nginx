#ifndef DMMR_SERVER_H
#define DMMR_SERVER_H

#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include "dmmr_protocol.h"   // <-- necessário para struct dmmr_frame
#include "dmmr_pool.h"       // opcional (já que usa struct payload_buf*)

struct payload_buf;          // mantido para clareza

static inline uint64_t now_micros(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ((uint64_t) ts.tv_sec * 1000000ULL) + (uint64_t) (ts.tv_nsec / 1000);
}

int process_frame(int fd, struct dmmr_frame *frame, const uint8_t *payload,
                  uint64_t source_node_id, bool from_peer);
int read_frame(int fd, struct dmmr_frame *frame, uint8_t **payload,
               struct payload_buf **payload_buf,
               bool *is_legacy, uint16_t *legacy_opcode, uint16_t *legacy_key_len);

#endif