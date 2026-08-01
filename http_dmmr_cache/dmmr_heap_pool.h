#ifndef DMMR_HEAP_POOL_H
#define DMMR_HEAP_POOL_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

typedef struct {
    size_t           obj_size;    /* tamanho de cada objeto */
    unsigned         capacity;    /* número máximo de objetos */
    uint8_t         *buffer;      /* bloco contíguo: (1 byte status + obj_size) por slot */
    unsigned         next_free;   /* índice de início da busca por slot livre */
    pthread_mutex_t  mutex;
} dmmr_heap_pool_t;

/**
 * Inicializa o pool com capacidade fixa.
 * Retorna 0 em sucesso, -1 em erro (memória insuficiente).
 */
int dmmr_heap_pool_init(dmmr_heap_pool_t *pool, size_t obj_size, unsigned capacity);

/**
 * Libera toda a memória associada ao pool.
 */
void dmmr_heap_pool_destroy(dmmr_heap_pool_t *pool);

/**
 * Aloca um objeto do pool.
 * Retorna ponteiro para o objeto (já zerado) ou NULL se o pool estiver cheio.
 */
void *dmmr_heap_pool_alloc(dmmr_heap_pool_t *pool);

/**
 * Devolve um objeto ao pool.
 * 'ptr' deve ter sido obtido de dmmr_heap_pool_alloc().
 */
void dmmr_heap_pool_free(dmmr_heap_pool_t *pool, void *ptr);

/**
 * Retorna o número de objetos atualmente em uso.
 */
unsigned dmmr_heap_pool_used(dmmr_heap_pool_t *pool);

#endif /* DMMR_HEAP_POOL_H */