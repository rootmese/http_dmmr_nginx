#include "dmmr_heap_pool.h"
#include <stdlib.h>
#include <string.h>

int dmmr_heap_pool_init(dmmr_heap_pool_t *pool, size_t obj_size, unsigned capacity) {
    if (pool == NULL || obj_size == 0 || capacity == 0)
        return -1;

    /* Cada slot: [1 byte status][obj_size bytes do objeto] */
    pool->buffer = (uint8_t *)calloc(capacity, 1 + obj_size);
    if (pool->buffer == NULL)
        return -1;

    pool->obj_size   = obj_size;
    pool->capacity   = capacity;
    pool->next_free  = 0;
    pthread_mutex_init(&pool->mutex, NULL);
    return 0;
}

void dmmr_heap_pool_destroy(dmmr_heap_pool_t *pool) {
    if (pool == NULL || pool->buffer == NULL)
        return;
    free(pool->buffer);
    pool->buffer = NULL;
    pthread_mutex_destroy(&pool->mutex);
}

void *dmmr_heap_pool_alloc(dmmr_heap_pool_t *pool) {
    if (pool == NULL)
        return NULL;

    const size_t slot_size = 1 + pool->obj_size;  /* status + dados */

    pthread_mutex_lock(&pool->mutex);

    unsigned start = pool->next_free;
    unsigned idx   = start;
    uint8_t *found = NULL;

    do {
        uint8_t *slot = pool->buffer + idx * slot_size;
        if (*slot == 0) {          /* slot livre */
            *slot = 1;             /* marca ocupado */
            pool->next_free = (idx + 1) % pool->capacity;
            found = slot + 1;      /* ponteiro para o início do objeto */
            break;
        }
        idx = (idx + 1) % pool->capacity;
    } while (idx != start);

    pthread_mutex_unlock(&pool->mutex);

    if (found) {
        memset(found, 0, pool->obj_size);  /* zera o objeto */
    }
    return found;
}

void dmmr_heap_pool_free(dmmr_heap_pool_t *pool, void *ptr) {
    if (pool == NULL || ptr == NULL || pool->buffer == NULL)
        return;

    const size_t slot_size = 1 + pool->obj_size;
    uint8_t *byte_ptr = (uint8_t *)ptr;

    /* Verifica se o ponteiro está dentro do buffer e alinhado */
    if (byte_ptr < pool->buffer + 1 ||
        byte_ptr >= pool->buffer + pool->capacity * slot_size)
        return;

    /* Calcula o início do slot (1 byte antes do objeto) */
    ptrdiff_t offset = byte_ptr - pool->buffer;
    if ((offset - 1) % slot_size != 0)   /* não está no início de um slot */
        return;

    uint8_t *slot = byte_ptr - 1;
    pthread_mutex_lock(&pool->mutex);
    if (*slot == 1) {
        *slot = 0;               /* marca livre */
    }
    pthread_mutex_unlock(&pool->mutex);
}

unsigned dmmr_heap_pool_used(dmmr_heap_pool_t *pool) {
    if (pool == NULL)
        return 0;

    unsigned count = 0;
    const size_t slot_size = 1 + pool->obj_size;

    pthread_mutex_lock(&pool->mutex);
    for (unsigned i = 0; i < pool->capacity; ++i) {
        if (*(pool->buffer + i * slot_size) != 0)
            ++count;
    }
    pthread_mutex_unlock(&pool->mutex);
    return count;
}