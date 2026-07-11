#include "dmmr_pool.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ============================================================
 * Pool de payload_buf
 * ============================================================ */
static struct payload_buf *payload_pool = NULL;
static unsigned payload_pool_size = 0;
static unsigned payload_pool_count = 0;
static pthread_mutex_t payload_mutex = PTHREAD_MUTEX_INITIALIZER;

struct payload_buf *get_payload_buf(void) {
    pthread_mutex_lock(&payload_mutex);
    register struct payload_buf *p = payload_pool;
    register struct payload_buf *p1 = payload_pool + payload_pool_size;
    for (; p < p1; ++p) {
        if (!(p->in_use)) {
            p->in_use = 1;
            p->len = 0;
            pthread_mutex_unlock(&payload_mutex);
            return p;
        }
    }

    if (payload_pool_count >= payload_pool_size) {
        unsigned new_size = (payload_pool_size == 0) ? POOL_INITIAL_SIZE : (payload_pool_size * 2);
        struct payload_buf *tmp = realloc(payload_pool, new_size * sizeof(struct payload_buf));
        if (!tmp) {
            pthread_mutex_unlock(&payload_mutex);
            return NULL;
        }
        memset(tmp + payload_pool_size, 0, (new_size - payload_pool_size) * sizeof(struct payload_buf));
        payload_pool = tmp;
        payload_pool_size = new_size;
    }

    struct payload_buf *ret = payload_pool + payload_pool_count++;
    ret->in_use = 1;
    ret->len = 0;
    pthread_mutex_unlock(&payload_mutex);
    return ret;
}

void release_payload_buf(struct payload_buf *p) {
    if (p) {
        pthread_mutex_lock(&payload_mutex);
        p->in_use = 0;
        p->len = 0;
        pthread_mutex_unlock(&payload_mutex);
    }
}

/* ============================================================
 * Pool de job_pool_entry
 * ============================================================ */
static struct job_pool_entry *job_pool = NULL;
static unsigned job_pool_size = 0;
static unsigned job_pool_count = 0;
static pthread_mutex_t job_mutex = PTHREAD_MUTEX_INITIALIZER;

struct job_pool_entry *get_job_entry(void) {
    pthread_mutex_lock(&job_mutex);
    register struct job_pool_entry *p = job_pool;
    register struct job_pool_entry *p1 = job_pool + job_pool_size;
    for (; p < p1; ++p) {
        if (!(p->in_use)) {
            p->in_use = 1;
            p->fd = -1;
            pthread_mutex_unlock(&job_mutex);
            return p;
        }
    }

    if (job_pool_count >= job_pool_size) {
        unsigned new_size = (job_pool_size == 0) ? POOL_INITIAL_SIZE : (job_pool_size * 2);
        struct job_pool_entry *tmp = realloc(job_pool, new_size * sizeof(struct job_pool_entry));
        if (!tmp) {
            pthread_mutex_unlock(&job_mutex);
            return NULL;
        }
        memset(tmp + job_pool_size, 0, (new_size - job_pool_size) * sizeof(struct job_pool_entry));
        job_pool = tmp;
        job_pool_size = new_size;
    }

    struct job_pool_entry *ret = job_pool + job_pool_count++;
    ret->in_use = 1;
    ret->fd = -1;
    pthread_mutex_unlock(&job_mutex);
    return ret;
}

void release_job_entry(struct job_pool_entry *p) {
    if (p) {
        pthread_mutex_lock(&job_mutex);
        p->in_use = 0;
        p->fd = -1;
        pthread_mutex_unlock(&job_mutex);
    }
}

/* ============================================================
 * Pool de control_cmd_pooled
 * ============================================================ */
static struct control_cmd_pooled *cmd_pool = NULL;
static unsigned cmd_pool_size = 0;
static unsigned cmd_pool_count = 0;
static pthread_mutex_t cmd_pool_mutex = PTHREAD_MUTEX_INITIALIZER;

struct control_cmd_pooled *get_control_cmd(void) {
    pthread_mutex_lock(&cmd_pool_mutex);
    register struct control_cmd_pooled *p = cmd_pool;
    register struct control_cmd_pooled *p1 = cmd_pool + cmd_pool_size;
    for (; p < p1; ++p) {
        if (!(p->in_use)) {
            p->in_use = 1;
            pthread_mutex_unlock(&cmd_pool_mutex);
            return p;
        }
    }

    if (cmd_pool_count >= cmd_pool_size) {
        unsigned new_size = (cmd_pool_size == 0) ? POOL_INITIAL_SIZE : (cmd_pool_size * 2);
        struct control_cmd_pooled *tmp = realloc(cmd_pool, new_size * sizeof(struct control_cmd_pooled));
        if (!tmp) {
            pthread_mutex_unlock(&cmd_pool_mutex);
            return NULL;
        }
        memset(tmp + cmd_pool_size, 0, (new_size - cmd_pool_size) * sizeof(struct control_cmd_pooled));
        cmd_pool = tmp;
        cmd_pool_size = new_size;
    }

    struct control_cmd_pooled *ret = cmd_pool + cmd_pool_count++;
    ret->in_use = 1;
    pthread_mutex_unlock(&cmd_pool_mutex);
    return ret;
}

void release_control_cmd(struct control_cmd_pooled *p) {
    if (p) {
        pthread_mutex_lock(&cmd_pool_mutex);
        p->in_use = 0;
        p->value = NULL;
        p->value_len = 0;
        p->key_len = 0;
        pthread_mutex_unlock(&cmd_pool_mutex);
    }
}

/* ============================================================
 * Pool de delete_entry (Garbage Collector)
 * ============================================================ */
static struct delete_entry *del_pool = NULL;
static unsigned del_pool_size = 0;
static unsigned del_pool_count = 0;
static pthread_mutex_t del_pool_mutex = PTHREAD_MUTEX_INITIALIZER;

struct delete_entry *get_delete_entry(void) {
    pthread_mutex_lock(&del_pool_mutex);
    register struct delete_entry *p = del_pool;
    register struct delete_entry *p1 = del_pool + del_pool_size;
    for (; p < p1; ++p) {
        if (!(p->in_use)) {
            p->in_use = 1;
            pthread_mutex_unlock(&del_pool_mutex);
            return p;
        }
    }

    if (del_pool_count >= del_pool_size) {
        unsigned new_size = (del_pool_size == 0) ? POOL_INITIAL_SIZE : (del_pool_size * 2);
        struct delete_entry *tmp = realloc(del_pool, new_size * sizeof(struct delete_entry));
        if (!tmp) {
            pthread_mutex_unlock(&del_pool_mutex);
            return NULL;
        }
        memset(tmp + del_pool_size, 0, (new_size - del_pool_size) * sizeof(struct delete_entry));
        del_pool = tmp;
        del_pool_size = new_size;
    }

    struct delete_entry *ret = del_pool + del_pool_count++;
    ret->in_use = 1;
    pthread_mutex_unlock(&del_pool_mutex);
    return ret;
}

void release_delete_entry(struct delete_entry *p) {
    if (p) {
        pthread_mutex_lock(&del_pool_mutex);
        p->in_use = 0;
        p->key_len = 0;
        pthread_mutex_unlock(&del_pool_mutex);
    }
}

/* ============================================================
 * Inicialização e finalização
 * ============================================================ */
int init_pools(void) {
    payload_pool = calloc(POOL_INITIAL_SIZE, sizeof(struct payload_buf));
    if (!payload_pool) return -1;
    payload_pool_size = POOL_INITIAL_SIZE;
    payload_pool_count = 0;

    job_pool = calloc(POOL_INITIAL_SIZE, sizeof(struct job_pool_entry));
    if (!job_pool) {
        free(payload_pool);
        return -1;
    }
    job_pool_size = POOL_INITIAL_SIZE;
    job_pool_count = 0;

    cmd_pool = calloc(POOL_INITIAL_SIZE, sizeof(struct control_cmd_pooled));
    if (!cmd_pool) {
        free(payload_pool);
        free(job_pool);
        return -1;
    }
    cmd_pool_size = POOL_INITIAL_SIZE;
    cmd_pool_count = 0;

    del_pool = calloc(POOL_INITIAL_SIZE, sizeof(struct delete_entry));
    if (!del_pool) {
        free(payload_pool);
        free(job_pool);
        free(cmd_pool);
        return -1;
    }
    del_pool_size = POOL_INITIAL_SIZE;
    del_pool_count = 0;

    return 0;
}

void destroy_pools(void) {
    free(payload_pool);   payload_pool = NULL;
    free(job_pool);       job_pool = NULL;
    free(cmd_pool);       cmd_pool = NULL;
    free(del_pool);       del_pool = NULL;

    pthread_mutex_destroy(&payload_mutex);
    pthread_mutex_destroy(&job_mutex);
    pthread_mutex_destroy(&cmd_pool_mutex);
    pthread_mutex_destroy(&del_pool_mutex);
}
