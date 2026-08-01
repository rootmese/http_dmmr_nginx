#ifndef DMMR_POOL_H
#define DMMR_POOL_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/queue.h>
#include "dmmr_config.h"

/* ============================================================
 * Pool de buffers de payload (substituir malloc em read_frame)
 * ============================================================ */
struct payload_buf {
    int in_use;                              /* flag de uso (0 = livre) */
    uint8_t *data;                           /* alocado conforme o frame */
    size_t len;                              /* tamanho efetivo dos dados */
    size_t capacity;
    TAILQ_ENTRY(payload_buf) free_entries;
};

struct payload_buf *get_payload_buf(size_t required);
void release_payload_buf(struct payload_buf *p);

/* ============================================================
 * Pool de job_entry (substituir malloc em enqueue_job)
 * ============================================================ */
struct job_pool_entry {
    int in_use;    /* flag de uso (0 = livre) */
    int fd;        /* descritor do cliente */
    TAILQ_ENTRY(job_pool_entry) free_entries;
};

struct job_pool_entry *get_job_entry(void);
void release_job_entry(struct job_pool_entry *p);

/* ============================================================
 * Pool de control_cmd (substituir malloc em enqueue_broadcast)
 * ============================================================ */
struct control_cmd_pooled {
    int in_use;                            /* flag de uso (0 = livre) */
    int type;
    uint64_t ts;
    uint64_t node_id;
    uint64_t expire_at;
    uint16_t flags;
    char key[MAX_KEY_LEN];
    size_t key_len;
    size_t value_len;
    uint8_t *value;                        /* valor alocado conforme necessário */
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
