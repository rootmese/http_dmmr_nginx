#include "ngx_http_dmmr_module.h"
#include <ngx_time.h>

/* Estrutura para armazenar contadores em uma Patricia trie compacta */
typedef struct ngx_http_dmmr_rate_node_s ngx_http_dmmr_rate_node_t;

struct ngx_http_dmmr_rate_node_s {
    ngx_http_dmmr_rate_node_t  *child[2];
    ngx_uint_t                  bit;
    ngx_flag_t                  terminal;
    ngx_str_t                   key;
    ngx_msec_t                  timestamp;
    ngx_uint_t                  count;
};

static ngx_http_dmmr_rate_node_t *rate_root = NULL;

static ngx_uint_t
ngx_http_dmmr_rate_bit_at(ngx_str_t *key, ngx_uint_t bit)
{
    if (bit >= key->len * 8) {
        return 0;
    }

    return (key->data[bit >> 3] >> (7 - (bit & 0x7))) & 0x1;
}

static ngx_uint_t
ngx_http_dmmr_rate_first_diff_bit(ngx_str_t *left, ngx_str_t *right)
{
    ngx_uint_t bit;
    ngx_uint_t max_bits;

    max_bits = ngx_max(left->len, right->len) * 8;

    for (bit = 0; bit < max_bits; bit++) {
        if (ngx_http_dmmr_rate_bit_at(left, bit) !=
            ngx_http_dmmr_rate_bit_at(right, bit))
        {
            return bit;
        }
    }

    return max_bits;
}

static ngx_int_t
ngx_http_dmmr_rate_key_equal(ngx_str_t *left, ngx_str_t *right)
{
    return left->len == right->len
           && ngx_strncmp(left->data, right->data, left->len) == 0;
}

static ngx_http_dmmr_rate_node_t *
ngx_http_dmmr_rate_create_node(ngx_http_request_t *r, ngx_str_t *key,
                               ngx_msec_t now)
{
    ngx_http_dmmr_rate_node_t *node;

    node = ngx_alloc(sizeof(ngx_http_dmmr_rate_node_t), r->connection->log);
    if (node == NULL) {
        return NULL;
    }

    ngx_memzero(node, sizeof(ngx_http_dmmr_rate_node_t));

    node->key.data = ngx_alloc(key->len, r->connection->log);
    if (node->key.data == NULL) {
        ngx_free(node);
        return NULL;
    }

    ngx_memcpy(node->key.data, key->data, key->len);
    node->key.len = key->len;
    node->timestamp = now;
    node->count = 1;
    node->terminal = 1;

    return node;
}

static ngx_http_dmmr_rate_node_t *
ngx_http_dmmr_rate_prune_node(ngx_http_request_t *r, ngx_http_dmmr_rate_node_t *node,
                              ngx_msec_t now)
{
    if (node == NULL) {
        return NULL;
    }

    if (node->terminal) {
        if (now - node->timestamp > rate_window) {
            ngx_free(node->key.data);
            ngx_free(node);
            return NULL;
        }

        return node;
    }

    node->child[0] = ngx_http_dmmr_rate_prune_node(r, node->child[0], now);
    node->child[1] = ngx_http_dmmr_rate_prune_node(r, node->child[1], now);

    if (node->child[0] == NULL && node->child[1] == NULL) {
        ngx_free(node);
        return NULL;
    }

    return node;
}

static ngx_http_dmmr_rate_node_t *
ngx_http_dmmr_rate_insert_node(ngx_http_request_t *r, ngx_str_t *key,
                               ngx_msec_t now, ngx_int_t *found)
{
    ngx_http_dmmr_rate_node_t *parent;
    ngx_http_dmmr_rate_node_t *node;
    ngx_http_dmmr_rate_node_t *new_leaf;
    ngx_http_dmmr_rate_node_t *branch;
    ngx_uint_t bit;
    ngx_uint_t child_bit;

    *found = 0;

    if (rate_root == NULL) {
        new_leaf = ngx_http_dmmr_rate_create_node(r, key, now);
        if (new_leaf == NULL) {
            return NULL;
        }

        rate_root = new_leaf;
        return new_leaf;
    }

    parent = NULL;
    node = rate_root;

    while (node != NULL && node->terminal == 0) {
        parent = node;
        child_bit = ngx_http_dmmr_rate_bit_at(key, node->bit);
        node = node->child[child_bit];
    }

    if (node != NULL && ngx_http_dmmr_rate_key_equal(&node->key, key)) {
        *found = 1;
        return node;
    }

    new_leaf = ngx_http_dmmr_rate_create_node(r, key, now);
    if (new_leaf == NULL) {
        return NULL;
    }

    if (node == NULL) {
        if (parent == NULL) {
            rate_root = new_leaf;
            return new_leaf;
        }

        child_bit = ngx_http_dmmr_rate_bit_at(key, parent->bit);
        parent->child[child_bit] = new_leaf;
        return new_leaf;
    }

    branch = ngx_alloc(sizeof(ngx_http_dmmr_rate_node_t), r->connection->log);
    if (branch == NULL) {
        ngx_free(new_leaf->key.data);
        ngx_free(new_leaf);
        return NULL;
    }

    ngx_memzero(branch, sizeof(ngx_http_dmmr_rate_node_t));
    branch->bit = ngx_http_dmmr_rate_first_diff_bit(key, &node->key);
    branch->terminal = 0;

    if (ngx_http_dmmr_rate_bit_at(key, branch->bit) == 0) {
        branch->child[0] = new_leaf;
        branch->child[1] = node;
    } else {
        branch->child[0] = node;
        branch->child[1] = new_leaf;
    }

    if (parent == NULL) {
        rate_root = branch;
    } else {
        child_bit = ngx_http_dmmr_rate_bit_at(key, parent->bit);
        parent->child[child_bit] = branch;
    }

    return new_leaf;
}

ngx_int_t ngx_http_dmmr_rate_init(ngx_cycle_t *cycle)
{
    rate_root = NULL;
    return NGX_OK;
}

ngx_int_t
ngx_http_dmmr_rate_limit(ngx_http_request_t *r, ngx_http_dmmr_ctx_t *ctx)
{
    ngx_http_dmmr_conf_t *conf;
    ngx_str_t client_key;
    ngx_http_dmmr_rate_node_t *node;
    ngx_msec_t now;
    ngx_int_t found;
    ngx_uint_t rate_limit;
    ngx_msec_t rate_window;
    u_char buf[NGX_INET_ADDRSTRLEN];

    conf = ngx_http_get_module_loc_conf(r, ngx_http_dmmr_module);
    rate_limit = (conf != NULL && conf->rate_limit > 0) ? conf->rate_limit : 100;
    rate_window = (conf != NULL && conf->rate_window > 0) ? conf->rate_window : 60000;

    /* Cria chave = IP + período (minuto) */
    client_key.data = buf;
    client_key.len = ngx_sock_ntop(r->connection->sockaddr,
                                   r->connection->socklen,
                                   buf, NGX_INET_ADDRSTRLEN, 0);
    if (client_key.len == 0) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    now = ngx_current_msec;
    rate_root = ngx_http_dmmr_rate_prune_node(r, rate_root, now);

    node = ngx_http_dmmr_rate_insert_node(r, &client_key, now, &found);
    if (node == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    if (found) {
        if (now - node->timestamp > rate_window) {
            node->count = 1;
            node->timestamp = now;
            return NGX_OK;
        }

        if (node->count >= rate_limit) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "dmmr: rate limit exceeded for IP %V", &client_key);
            return NGX_HTTP_TOO_MANY_REQUESTS;
        }

        node->count++;
        return NGX_OK;
    }

    return NGX_OK;
}
