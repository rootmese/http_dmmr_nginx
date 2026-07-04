#ifndef DMMR_SERVER_H
#define DMMR_SERVER_H

#include "dmmr_protocol.h"
#include <stdbool.h>
#include <stdint.h>

int process_frame(int fd, struct dmmr_frame *frame, const uint8_t *payload,
                  uint64_t source_node_id, bool from_peer);
int read_frame(int fd, struct dmmr_frame *frame, uint8_t **payload,
               bool *is_legacy, uint16_t *legacy_opcode, uint16_t *legacy_key_len);

#endif /* DMMR_SERVER_H */