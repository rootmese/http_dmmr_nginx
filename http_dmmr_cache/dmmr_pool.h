#ifndef DMMR_POOL_H
#define DMMR_POOL_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include "dmmr_config.h"

/* ============================================================
 * Pool de buffers de payload (substituir malloc em read_frame)
 * ============================================================ */
struct payload_buf {
    int in_use;                              /* flag de uso (0 = livre) */
    uint8_t data[MAX_KEY_LEN + MAX_VALUE_LEN]; /* buffer de dados */
    size_t len;                              /* tamanho efetivo dos dados */
};

struct payload_buf *get_payload_buf(void);
void release_payload_buf(struct payload_buf *p);

/* ============================================================
 * Pool de job_entry (substituir malloc em enqueue_job)
 * ============================================================ */
struct job_pool_entry {
    int in_use;    /* flag de uso (0 = livre) */
    int fd;        /* descritor do cliente */
};

struct job_pool_entry *get_job_entry(void);
void release_job_entry(struct job_pool_entry *p);

/* ============================================================
 * Pool de control_cmd (substituir malloc em enqueue_broadcast)
 * ============================================================ */
#include <sys/queue.h>

struct control_cmd_pooled {
    int in_use;                            /* flag de uso (0 = livre) */
    int type;
    uint64_t ts;
    uint64_t node_id;
    char key[MAX_KEY_LEN];
    size_t key_len;
    uint8_t value_data[MAX_VALUE_LEN];     /* valor inline (sem ponteiro externo) */
    size_t value_len;
    uint8_t *value;                        /* aponta para value_data ou NULL */
    TAILQ_ENTRY(control_cmd_pooled) entries;
};
TAILQ_HEAD(control_queue_pooled, control_cmd_pooled);

struct control_cmd_pooled *get_control_cmd(void);
void release_control_cmd(struct control_cmd_pooled *p);

/* ============================================================
 * Pool de delete_entry (fila do Garbage Collector)
 * ============================================================ */
struct delete_entry {
    int in_use;                /* flag de uso (0 = livre) */
    char key[MAX_KEY_LEN];
    size_t key_len;
    TAILQ_ENTRY(delete_entry) entries;
};
TAILQ_HEAD(delete_queue, delete_entry);

struct delete_entry *get_delete_entry(void);
void release_delete_entry(struct delete_entry *p);

/* ============================================================
 * Inicialização e finalização de todos os pools
 * ============================================================ */
int init_pools(void);
void destroy_pools(void);

#endif /* DMMR_POOL_H */
