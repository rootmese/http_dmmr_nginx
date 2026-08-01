#include "dmmr_pool.h"
#include <stdlib.h>
#include <string.h>

#define PAYLOAD_RETAIN_LIMIT (64U * 1024U)

struct payload_chunk {
    struct payload_buf entries[POOL_INITIAL_SIZE];
    struct payload_chunk *next;
};

TAILQ_HEAD(payload_free_list, payload_buf);
static struct payload_chunk *payload_chunks = NULL;
static struct payload_free_list payload_free = TAILQ_HEAD_INITIALIZER(payload_free);
static pthread_mutex_t payload_mutex = PTHREAD_MUTEX_INITIALIZER;

static int add_payload_chunk(void) {
    struct payload_chunk *chunk = calloc(1, sizeof(*chunk));
    if (!chunk) return -1;
    chunk->next = payload_chunks;
    payload_chunks = chunk;
    for (unsigned i = 0; i < POOL_INITIAL_SIZE; ++i)
        TAILQ_INSERT_TAIL(&payload_free, &chunk->entries[i], free_entries);
    return 0;
}

struct payload_buf *get_payload_buf(size_t required) {
    if (!required || required > MAX_KEY_LEN + MAX_VALUE_LEN) return NULL;
    pthread_mutex_lock(&payload_mutex);
    if (TAILQ_EMPTY(&payload_free) && add_payload_chunk() != 0) {
        pthread_mutex_unlock(&payload_mutex);
        return NULL;
    }
    struct payload_buf *ret = TAILQ_FIRST(&payload_free);
    TAILQ_REMOVE(&payload_free, ret, free_entries);
    if (ret->capacity < required) {
        uint8_t *data = realloc(ret->data, required);
        if (!data) {
            TAILQ_INSERT_TAIL(&payload_free, ret, free_entries);
            pthread_mutex_unlock(&payload_mutex);
            return NULL;
        }
        ret->data = data;
        ret->capacity = required;
    }
    ret->in_use = 1;
    ret->len = required;
    pthread_mutex_unlock(&payload_mutex);
    return ret;
}

void release_payload_buf(struct payload_buf *p) {
    if (!p) return;
    pthread_mutex_lock(&payload_mutex);
    if (p->data && p->len > 0) memset(p->data, 0, p->len);
    if (p->capacity > PAYLOAD_RETAIN_LIMIT) {
        free(p->data);
        p->data = NULL;
        p->capacity = 0;
    }
    p->len = 0;
    p->in_use = 0;
    TAILQ_INSERT_TAIL(&payload_free, p, free_entries);
    pthread_mutex_unlock(&payload_mutex);
}

static void free_payload_chunks(void) {
    while (payload_chunks) {
        struct payload_chunk *chunk = payload_chunks;
        payload_chunks = chunk->next;
        for (unsigned i = 0; i < POOL_INITIAL_SIZE; ++i)
            free(chunk->entries[i].data);
        free(chunk);
    }
}

int init_pools(void) {
    if (add_payload_chunk() != 0) return -1;
    return 0;
}

void destroy_pools(void) {
    free_payload_chunks();
    pthread_mutex_destroy(&payload_mutex);
}