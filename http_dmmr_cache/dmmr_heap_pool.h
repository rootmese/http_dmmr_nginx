#ifndef DMMR_HEAP_POOL_H
#define DMMR_HEAP_POOL_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

typedef struct {
    size_t           obj_size;      /* tamanho de cada objeto (já alinhado) */
    unsigned         capacity;      /* número máximo de objetos */
    uint8_t         *buffer;        /* ponteiro para o bloco contíguo de objetos */
    unsigned         free_head;     /* índice do primeiro slot livre; (unsigned)-1 se vazio */
    pthread_mutex_t  mutex;
} dmmr_heap_pool_t;

int dmmr_heap_pool_init(dmmr_heap_pool_t *pool, size_t obj_size, unsigned capacity);
void dmmr_heap_pool_destroy(dmmr_heap_pool_t *pool);
void *dmmr_heap_pool_alloc(dmmr_heap_pool_t *pool);
void dmmr_heap_pool_free(dmmr_heap_pool_t *pool, void *ptr);
unsigned dmmr_heap_pool_used(dmmr_heap_pool_t *pool);

#endif