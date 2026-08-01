#include "dmmr_pool.h"
#include <stdlib.h>
#include <string.h>

/* Chunks keep allocated entries at stable addresses; TAILQs track free slots. */

/* Keep common small requests hot without retaining arbitrarily large frames. */
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
    unsigned i;
    if (chunk == NULL) return -1;
    chunk->next = payload_chunks;
    payload_chunks = chunk;
    for (i = 0; i < POOL_INITIAL_SIZE; ++i)
        TAILQ_INSERT_TAIL(&payload_free, &chunk->entries[i], free_entries);
    return 0;
}

struct payload_buf *get_payload_buf(size_t required) {
    struct payload_buf *ret;
    uint8_t *data;

    if (required == 0 || required > MAX_KEY_LEN + MAX_VALUE_LEN) return NULL;

    pthread_mutex_lock(&payload_mutex);
    if (TAILQ_EMPTY(&payload_free) && add_payload_chunk() != 0) {
        pthread_mutex_unlock(&payload_mutex);
        return NULL;
    }
    ret = TAILQ_FIRST(&payload_free);
    TAILQ_REMOVE(&payload_free, ret, free_entries);

    if (ret->capacity < required) {
        data = realloc(ret->data, required);
        if (data == NULL) {
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
    if (p) {
        pthread_mutex_lock(&payload_mutex);
        if (p->data != NULL && p->len > 0) {
            memset(p->data, 0, p->len);
        }
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
}

struct job_chunk {
    struct job_pool_entry entries[POOL_INITIAL_SIZE];
    struct job_chunk *next;
};
TAILQ_HEAD(job_free_list, job_pool_entry);
static struct job_chunk *job_chunks = NULL;
static struct job_free_list job_free = TAILQ_HEAD_INITIALIZER(job_free);
static pthread_mutex_t job_mutex = PTHREAD_MUTEX_INITIALIZER;

static int add_job_chunk(void) {
    struct job_chunk *chunk = calloc(1, sizeof(*chunk));
    unsigned i;
    if (chunk == NULL) return -1;
    chunk->next = job_chunks;
    job_chunks = chunk;
    for (i = 0; i < POOL_INITIAL_SIZE; ++i)
        TAILQ_INSERT_TAIL(&job_free, &chunk->entries[i], free_entries);
    return 0;
}

struct job_pool_entry *get_job_entry(void) {
    struct job_pool_entry *ret;
    pthread_mutex_lock(&job_mutex);
    if (TAILQ_EMPTY(&job_free) && add_job_chunk() != 0) {
        pthread_mutex_unlock(&job_mutex);
        return NULL;
    }
    ret = TAILQ_FIRST(&job_free);
    TAILQ_REMOVE(&job_free, ret, free_entries);
    ret->in_use = 1;
    ret->fd = -1;
    pthread_mutex_unlock(&job_mutex);
    return ret;
}

void release_job_entry(struct job_pool_entry *p) {
    if (p) {
        pthread_mutex_lock(&job_mutex);
        memset(p, 0, sizeof(*p));
        TAILQ_INSERT_TAIL(&job_free, p, free_entries);
        pthread_mutex_unlock(&job_mutex);
    }
}

struct cmd_chunk {
    struct control_cmd_pooled entries[POOL_INITIAL_SIZE];
    struct cmd_chunk *next;
};
TAILQ_HEAD(cmd_free_list, control_cmd_pooled);
static struct cmd_chunk *cmd_chunks = NULL;
static struct cmd_free_list cmd_free = TAILQ_HEAD_INITIALIZER(cmd_free);
static pthread_mutex_t cmd_pool_mutex = PTHREAD_MUTEX_INITIALIZER;

static int add_cmd_chunk(void) {
    struct cmd_chunk *chunk = calloc(1, sizeof(*chunk));
    unsigned i;
    if (chunk == NULL) return -1;
    chunk->next = cmd_chunks;
    cmd_chunks = chunk;
    for (i = 0; i < POOL_INITIAL_SIZE; ++i)
        TAILQ_INSERT_TAIL(&cmd_free, &chunk->entries[i], entries);
    return 0;
}

struct control_cmd_pooled *get_control_cmd(void) {
    struct control_cmd_pooled *ret;
    pthread_mutex_lock(&cmd_pool_mutex);
    if (TAILQ_EMPTY(&cmd_free) && add_cmd_chunk() != 0) {
        pthread_mutex_unlock(&cmd_pool_mutex);
        return NULL;
    }
    ret = TAILQ_FIRST(&cmd_free);
    TAILQ_REMOVE(&cmd_free, ret, entries);
    memset(ret, 0, sizeof(*ret));
    ret->in_use = 1;
    pthread_mutex_unlock(&cmd_pool_mutex);
    return ret;
}

void release_control_cmd(struct control_cmd_pooled *p) {
    if (p) {
        pthread_mutex_lock(&cmd_pool_mutex);
        if (p->value != NULL) {
            memset(p->value, 0, p->value_len);
            free(p->value);
        }
        memset(p, 0, sizeof(*p));
        TAILQ_INSERT_TAIL(&cmd_free, p, entries);
        pthread_mutex_unlock(&cmd_pool_mutex);
    }
}

struct delete_chunk {
    struct delete_entry entries[POOL_INITIAL_SIZE];
    struct delete_chunk *next;
};
TAILQ_HEAD(delete_free_list, delete_entry);
static struct delete_chunk *del_chunks = NULL;
static struct delete_free_list del_free = TAILQ_HEAD_INITIALIZER(del_free);
static pthread_mutex_t del_pool_mutex = PTHREAD_MUTEX_INITIALIZER;

static int add_delete_chunk(void) {
    struct delete_chunk *chunk = calloc(1, sizeof(*chunk));
    unsigned i;
    if (chunk == NULL) return -1;
    chunk->next = del_chunks;
    del_chunks = chunk;
    for (i = 0; i < POOL_INITIAL_SIZE; ++i)
        TAILQ_INSERT_TAIL(&del_free, &chunk->entries[i], entries);
    return 0;
}

struct delete_entry *get_delete_entry(void) {
    struct delete_entry *ret;
    pthread_mutex_lock(&del_pool_mutex);
    if (TAILQ_EMPTY(&del_free) && add_delete_chunk() != 0) {
        pthread_mutex_unlock(&del_pool_mutex);
        return NULL;
    }
    ret = TAILQ_FIRST(&del_free);
    TAILQ_REMOVE(&del_free, ret, entries);
    ret->in_use = 1;
    pthread_mutex_unlock(&del_pool_mutex);
    return ret;
}

void release_delete_entry(struct delete_entry *p) {
    if (p) {
        pthread_mutex_lock(&del_pool_mutex);
        memset(p, 0, sizeof(*p));
        TAILQ_INSERT_TAIL(&del_free, p, entries);
        pthread_mutex_unlock(&del_pool_mutex);
    }
}

static void free_payload_chunks(void) {
    while (payload_chunks != NULL) {
        struct payload_chunk *chunk = payload_chunks;
        unsigned i;
        payload_chunks = chunk->next;
        for (i = 0; i < POOL_INITIAL_SIZE; ++i)
            free(chunk->entries[i].data);
        free(chunk);
    }
}

static void free_job_chunks(void) {
    while (job_chunks != NULL) {
        struct job_chunk *chunk = job_chunks;
        job_chunks = chunk->next;
        free(chunk);
    }
}

static void free_cmd_chunks(void) {
    while (cmd_chunks != NULL) {
        struct cmd_chunk *chunk = cmd_chunks;
        unsigned i;
        cmd_chunks = chunk->next;
        for (i = 0; i < POOL_INITIAL_SIZE; ++i)
            free(chunk->entries[i].value);
        free(chunk);
    }
}

static void free_delete_chunks(void) {
    while (del_chunks != NULL) {
        struct delete_chunk *chunk = del_chunks;
        del_chunks = chunk->next;
        free(chunk);
    }
}

int init_pools(void) {
    if (add_payload_chunk() != 0) return -1;
    if (add_job_chunk() != 0) goto fail_payload;
    if (add_cmd_chunk() != 0) goto fail_job;
    if (add_delete_chunk() != 0) goto fail_cmd;
    return 0;

fail_cmd:
    free_cmd_chunks();
fail_job:
    free_job_chunks();
fail_payload:
    free_payload_chunks();
    return -1;
}

void destroy_pools(void) {
    free_payload_chunks();
    free_job_chunks();
    free_cmd_chunks();
    free_delete_chunks();
    pthread_mutex_destroy(&payload_mutex);
    pthread_mutex_destroy(&job_mutex);
    pthread_mutex_destroy(&cmd_pool_mutex);
    pthread_mutex_destroy(&del_pool_mutex);
}
