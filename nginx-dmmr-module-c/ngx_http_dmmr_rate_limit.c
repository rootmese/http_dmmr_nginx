#include "ngx_http_dmmr_module.h"
#include <ngx_crc32.h>
#include <ngx_time.h>

#define DMMR_DEFAULT_RATE_ZONE_SIZE (10 * 1024 * 1024)

typedef struct {
    ngx_rbtree_t  rbtree;
    ngx_rbtree_node_t sentinel;
    ngx_queue_t   lru;
} ngx_http_dmmr_rate_shctx_t;

typedef struct {
    ngx_rbtree_node_t node;
    ngx_queue_t       queue;
    ngx_msec_t        window_start;
    ngx_uint_t        count;
    u_char            key_len;
    u_char            key[NGX_SOCKADDR_STRLEN];
} ngx_http_dmmr_rate_node_t;

static void ngx_http_dmmr_rate_rbtree_insert(ngx_rbtree_node_t *temp,
    ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel);
static ngx_int_t ngx_http_dmmr_rate_zone_init(ngx_shm_zone_t *shm_zone,
    void *data);

static void
ngx_http_dmmr_rate_rbtree_insert(ngx_rbtree_node_t *temp,
    ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel)
{
    ngx_rbtree_node_t **p;
    ngx_http_dmmr_rate_node_t *new_node;
    ngx_http_dmmr_rate_node_t *current;
    ngx_int_t cmp;

    new_node = (ngx_http_dmmr_rate_node_t *) node;

    for ( ;; ) {
        if (node->key < temp->key) {
            p = &temp->left;
        } else if (node->key > temp->key) {
            p = &temp->right;
        } else {
            current = (ngx_http_dmmr_rate_node_t *) temp;
            cmp = (ngx_int_t) new_node->key_len - (ngx_int_t) current->key_len;
            if (cmp == 0) {
                cmp = ngx_memcmp(new_node->key, current->key, new_node->key_len);
            }
            p = (cmp < 0) ? &temp->left : &temp->right;
        }

        if (*p == sentinel) {
            *p = node;
            node->parent = temp;
            node->left = sentinel;
            node->right = sentinel;
            ngx_rbt_red(node);
            return;
        }
        temp = *p;
    }
}

static ngx_http_dmmr_rate_node_t *
ngx_http_dmmr_rate_lookup(ngx_http_dmmr_rate_shctx_t *sh,
    ngx_str_t *key, ngx_rbtree_key_t hash)
{
    ngx_rbtree_node_t *node = sh->rbtree.root;
    ngx_rbtree_node_t *sentinel = sh->rbtree.sentinel;
    ngx_http_dmmr_rate_node_t *entry;
    ngx_int_t cmp;

    while (node != sentinel) {
        if (hash < node->key) {
            node = node->left;
        } else if (hash > node->key) {
            node = node->right;
        } else {
            entry = (ngx_http_dmmr_rate_node_t *) node;
            cmp = (ngx_int_t) key->len - (ngx_int_t) entry->key_len;
            if (cmp == 0) {
                cmp = ngx_memcmp(key->data, entry->key, key->len);
            }
            if (cmp == 0) {
                return entry;
            }
            node = (cmp < 0) ? node->left : node->right;
        }
    }

    return NULL;
}

static void
ngx_http_dmmr_rate_expire(ngx_http_dmmr_rate_shctx_t *sh,
    ngx_slab_pool_t *shpool, ngx_msec_t now, ngx_msec_t window)
{
    ngx_queue_t *q;
    ngx_http_dmmr_rate_node_t *entry;

    while (!ngx_queue_empty(&sh->lru)) {
        q = ngx_queue_head(&sh->lru);
        entry = ngx_queue_data(q, ngx_http_dmmr_rate_node_t, queue);
        if (now - entry->window_start <= window) {
            break;
        }
        ngx_queue_remove(q);
        ngx_rbtree_delete(&sh->rbtree, &entry->node);
        ngx_slab_free_locked(shpool, entry);
    }
}

static ngx_int_t
ngx_http_dmmr_rate_zone_init(ngx_shm_zone_t *shm_zone, void *data)
{
    ngx_slab_pool_t *shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;
    ngx_http_dmmr_rate_shctx_t *sh;

    if (data != NULL) {
        shm_zone->data = data;
        return NGX_OK;
    }

    sh = ngx_slab_alloc(shpool, sizeof(*sh));
    if (sh == NULL) {
        return NGX_ERROR;
    }
    ngx_rbtree_init(&sh->rbtree, &sh->sentinel,
                    ngx_http_dmmr_rate_rbtree_insert);
    ngx_queue_init(&sh->lru);
    shm_zone->data = sh;
    return NGX_OK;
}

char *
ngx_http_dmmr_rate_zone(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_dmmr_conf_t *main_conf = conf;
    ngx_str_t *value;
    ngx_str_t name;
    ngx_str_t size_value;
    ssize_t size;
    u_char *p;

    (void) cmd;
    if (main_conf->rate_zone != NULL) {
        return "is duplicate";
    }

    value = cf->args->elts;
    p = (u_char *) ngx_strlchr(value[1].data,
                               value[1].data + value[1].len, ':');
    if (p == NULL || p == value[1].data || p == value[1].data + value[1].len - 1) {
        return "must be in the form name:size";
    }

    name.data = value[1].data;
    name.len = p - value[1].data;
    size_value.data = p + 1;
    size_value.len = value[1].len - name.len - 1;
    size = ngx_parse_size(&size_value);
    if (size == NGX_ERROR || size < (ssize_t) (8 * ngx_pagesize)) {
        return "zone size must be at least 8 pages";
    }

    main_conf->rate_zone = ngx_shared_memory_add(cf, &name, (size_t) size,
                                                  &ngx_http_dmmr_module);
    if (main_conf->rate_zone == NULL) {
        return NGX_CONF_ERROR;
    }
    main_conf->rate_zone->init = ngx_http_dmmr_rate_zone_init;
    return NGX_CONF_OK;
}

ngx_int_t
ngx_http_dmmr_rate_init(ngx_conf_t *cf, ngx_http_dmmr_conf_t *main_conf)
{
    ngx_str_t name = ngx_string("dmmr_rate_limit");

    if (main_conf->rate_zone == NULL) {
        main_conf->rate_zone = ngx_shared_memory_add(cf, &name,
                                                      DMMR_DEFAULT_RATE_ZONE_SIZE,
                                                      &ngx_http_dmmr_module);
        if (main_conf->rate_zone == NULL) {
            return NGX_ERROR;
        }
        main_conf->rate_zone->init = ngx_http_dmmr_rate_zone_init;
    }
    return NGX_OK;
}

ngx_int_t
ngx_http_dmmr_rate_limit(ngx_http_request_t *r, ngx_http_dmmr_ctx_t *ctx)
{
    ngx_http_dmmr_conf_t *conf;
    ngx_http_dmmr_conf_t *main_conf;
    ngx_http_dmmr_rate_shctx_t *sh;
    ngx_slab_pool_t *shpool;
    ngx_http_dmmr_rate_node_t *entry;
    ngx_str_t client_key;
    ngx_rbtree_key_t hash;
    ngx_msec_t now;
    ngx_uint_t rate_limit;
    ngx_msec_t rate_window;
    ngx_flag_t limited;
    u_char buf[NGX_SOCKADDR_STRLEN];

    (void) ctx;
    conf = ngx_http_get_module_loc_conf(r, ngx_http_dmmr_module);
    main_conf = ngx_http_get_module_main_conf(r, ngx_http_dmmr_module);
    if (main_conf == NULL || main_conf->rate_zone == NULL ||
        main_conf->rate_zone->data == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    rate_limit = (conf != NULL && conf->rate_limit > 0) ? conf->rate_limit : 100;
    rate_window = (conf != NULL && conf->rate_window > 0) ? conf->rate_window : 60000;
    client_key.data = buf;
    client_key.len = ngx_sock_ntop(r->connection->sockaddr,
                                   r->connection->socklen, buf, sizeof(buf), 0);
    if (client_key.len == 0 || client_key.len > NGX_SOCKADDR_STRLEN) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    now = ngx_current_msec;
    hash = ngx_crc32_short(client_key.data, client_key.len);
    sh = main_conf->rate_zone->data;
    shpool = (ngx_slab_pool_t *) main_conf->rate_zone->shm.addr;

    ngx_shmtx_lock(&shpool->mutex);
    entry = ngx_http_dmmr_rate_lookup(sh, &client_key, hash);
    if (entry != NULL) {
        limited = 0;
        if (now - entry->window_start > rate_window) {
            entry->window_start = now;
            entry->count = 1;
        } else if (entry->count >= rate_limit) {
            limited = 1;
        } else {
            entry->count++;
        }
        ngx_queue_remove(&entry->queue);
        ngx_queue_insert_tail(&sh->lru, &entry->queue);
        ngx_shmtx_unlock(&shpool->mutex);
        if (limited) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "dmmr: rate limit exceeded for IP %V", &client_key);
            return NGX_HTTP_TOO_MANY_REQUESTS;
        }
        return NGX_OK;
    }

    ngx_http_dmmr_rate_expire(sh, shpool, now, rate_window);
    entry = ngx_slab_alloc_locked(shpool, sizeof(*entry));
    if (entry == NULL && !ngx_queue_empty(&sh->lru)) {
        ngx_queue_t *oldest = ngx_queue_head(&sh->lru);
        ngx_http_dmmr_rate_node_t *victim;

        victim = ngx_queue_data(oldest, ngx_http_dmmr_rate_node_t, queue);
        ngx_queue_remove(oldest);
        ngx_rbtree_delete(&sh->rbtree, &victim->node);
        ngx_slab_free_locked(shpool, victim);
        entry = ngx_slab_alloc_locked(shpool, sizeof(*entry));
    }
    if (entry == NULL) {
        ngx_shmtx_unlock(&shpool->mutex);
        return NGX_HTTP_SERVICE_UNAVAILABLE;
    }
    ngx_memzero(entry, sizeof(*entry));
    entry->node.key = hash;
    entry->window_start = now;
    entry->count = 1;
    entry->key_len = (u_char) client_key.len;
    ngx_memcpy(entry->key, client_key.data, client_key.len);
    ngx_rbtree_insert(&sh->rbtree, &entry->node);
    ngx_queue_insert_tail(&sh->lru, &entry->queue);
    ngx_shmtx_unlock(&shpool->mutex);
    return NGX_OK;
}
