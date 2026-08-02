#include "dmmr_heap_pool.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Inicializa o pool: aloca buffer e encadeia todos os slots livres.
   Cada slot, quando livre, armazena o índice do próximo slot livre
   (um unsigned int). Como o objeto não é usado enquanto livre,
   podemos sobrescrever seus primeiros bytes com esse índice. */
int dmmr_heap_pool_init(dmmr_heap_pool_t *pool, size_t obj_size, unsigned capacity) {
    if (!pool || obj_size == 0 || capacity == 0)
        return -1;

    /* Para garantir que caiba um índice no slot livre, obj_size deve ser >= sizeof(unsigned) */
    if (obj_size < sizeof(unsigned))
        return -1;

    pool->buffer = (uint8_t *)calloc(capacity, obj_size);
    if (!pool->buffer)
        return -1;

    pool->obj_size = obj_size;
    pool->capacity = capacity;
    pool->free_head = 0;

    /* Encadeia todos os slots: slot i aponta para i+1, o último para (unsigned)-1 */
    for (unsigned i = 0; i < capacity - 1; i++) {
        unsigned *next = (unsigned *)(pool->buffer + i * obj_size);
        *next = i + 1;
    }
    unsigned *last = (unsigned *)(pool->buffer + (capacity - 1) * obj_size);
    *last = (unsigned)-1;

    pthread_mutex_init(&pool->mutex, NULL);
    return 0;
}

void dmmr_heap_pool_destroy(dmmr_heap_pool_t *pool) {
    if (!pool || !pool->buffer)
        return;
    free(pool->buffer);
    pool->buffer = NULL;
    pthread_mutex_destroy(&pool->mutex);
}

/* Aloca um objeto: remove o primeiro da free list e retorna ponteiro.
   Se a free list estiver vazia, retorna NULL. */
void *dmmr_heap_pool_alloc(dmmr_heap_pool_t *pool) {
    if (!pool)
        return NULL;

    pthread_mutex_lock(&pool->mutex);
    if (pool->free_head == (unsigned)-1) {
        pthread_mutex_unlock(&pool->mutex);
        return NULL;
    }

    unsigned idx = pool->free_head;
    uint8_t *slot = pool->buffer + idx * pool->obj_size;
    /* O próximo livre agora é o valor armazenado no slot (que era o próximo índice) */
    pool->free_head = *(unsigned *)slot;

    pthread_mutex_unlock(&pool->mutex);

    /* Zera o objeto (opcional, mas mantido para consistência) */
    memset(slot, 0, pool->obj_size);
    return (void *)slot;
}

/* Libera um objeto: insere o slot no início da free list. */
void dmmr_heap_pool_free(dmmr_heap_pool_t *pool, void *ptr) {
    if (!pool || !ptr || !pool->buffer)
        return;

    /* Verifica se o ponteiro está dentro do buffer e alinhado */
    uint8_t *byte_ptr = (uint8_t *)ptr;
    if (byte_ptr < pool->buffer ||
        byte_ptr >= pool->buffer + pool->capacity * pool->obj_size)
        return;

    ptrdiff_t offset = byte_ptr - pool->buffer;
    if (offset % pool->obj_size != 0)
        return;   /* não está no início de um slot */

    unsigned idx = (unsigned)(offset / pool->obj_size);

    pthread_mutex_lock(&pool->mutex);
    /* O slot agora guarda o antigo cabeçalho da free list */
    *(unsigned *)ptr = pool->free_head;
    pool->free_head = idx;
    pthread_mutex_unlock(&pool->mutex);
}

/* Retorna o número de objetos atualmente em uso (percorrendo todos os slots) */
unsigned dmmr_heap_pool_used(dmmr_heap_pool_t *pool) {
    if (!pool)
        return 0;

    /* Não temos uma contagem direta; podemos percorrer o buffer
       e verificar se cada slot está na free list. Mas para manter
       O(n) em used() (que raramente é chamado), podemos usar um
       array de booleanos ou simplesmente contar percorrendo a free list.
       Abaixo, uma implementação simples que percorre todos os slots
       e verifica se estão livres (comparando com a free list).
       Para simplificar, vamos manter a abordagem anterior com byte de status?
       Mas podemos usar uma contagem separada.
    */

    /* Implementação rápida: percorrer a free list e subtrair da capacidade */
    unsigned free_count = 0;
    unsigned idx = pool->free_head;
    while (idx != (unsigned)-1) {
        free_count++;
        idx = *(unsigned *)(pool->buffer + idx * pool->obj_size);
    }
    return pool->capacity - free_count;
}