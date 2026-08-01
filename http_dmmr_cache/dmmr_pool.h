#ifndef DMMR_POOL_H
#define DMMR_POOL_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/queue.h>
#include "dmmr_config.h"

struct payload_buf {
    int      in_use;
    uint8_t *data;
    size_t   len;
    size_t   capacity;
    TAILQ_ENTRY(payload_buf) free_entries;
};

struct payload_buf *get_payload_buf(size_t required);
void release_payload_buf(struct payload_buf *p);

int init_pools(void);

void destroy_pools(void) ;

#endif