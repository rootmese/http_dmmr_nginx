#ifndef DMMR_SERVER_H
#define DMMR_SERVER_H

#include <stdbool.h>
#include <stdint.h>
#include "dmmr_protocol.h"   // <-- necessário para struct dmmr_frame
#include "dmmr_pool.h"       // opcional (já que usa struct payload_buf*)
#include "dmmr_cluster.h"

struct payload_buf;          // mantido para clareza

int process_frame(int fd, struct dmmr_frame *frame, const uint8_t *payload,
                  uint64_t source_node_id, bool from_peer);
int read_frame(int fd, struct dmmr_frame *frame, uint8_t **payload,
               struct payload_buf **payload_buf,
               bool *is_legacy, uint16_t *legacy_opcode, uint16_t *legacy_key_len);

#endif