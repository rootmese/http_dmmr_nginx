#include "ngx_http_dmmr_module.h"
#include <ngx_rbtree.h>
#include <ngx_time.h>

/* Estrutura para armazenar contadores */
typedef struct {
    ngx_rbtree_node_t  node;
    ngx_str_t          key;          /* chave = IP + período */
    ngx_msec_t         timestamp;
    ngx_uint_t         count;
} ngx_http_dmmr_rate_node_t;

static ngx_rbtree_t  rate_tree;
static ngx_rbtree_node_t  rate_sentinel;
static ngx_msec_t    rate_window = 60000; /* 1 minuto */
static ngx_uint_t    rate_limit = 100;     /* 100 req/minuto */

static void
ngx_http_dmmr_rate_prune(ngx_rbtree_node_t *node, ngx_msec_t now)
{
    ngx_http_dmmr_rate_node_t *rate_node;

    if (node == NULL || node == &rate_sentinel) {
        return;
    }

    ngx_http_dmmr_rate_prune(node->left, now);
    ngx_http_dmmr_rate_prune(node->right, now);

    rate_node = (ngx_http_dmmr_rate_node_t *) node;
    if (now - rate_node->timestamp > rate_window) {
        ngx_rbtree_delete(&rate_tree, &rate_node->node);
        ngx_free(rate_node->key.data);
        ngx_free(rate_node);
    }
}

ngx_int_t ngx_http_dmmr_rate_init(ngx_cycle_t *cycle)
{
    ngx_rbtree_init(&rate_tree, &rate_sentinel, ngx_rbtree_insert_value);
    return NGX_OK;
}

ngx_int_t
ngx_http_dmmr_rate_limit(ngx_http_request_t *r, ngx_http_dmmr_ctx_t *ctx)
{
    ngx_str_t client_key;
    ngx_http_dmmr_rate_node_t *node, *new_node;
    ngx_rbtree_node_t *rb_node;
    ngx_msec_t now;
    u_char buf[NGX_INET_ADDRSTRLEN];

    /* Cria chave = IP + período (minuto) */
    client_key.data = buf;
    client_key.len = ngx_sock_ntop(r->connection->sockaddr,
                                   r->connection->socklen,
                                   buf, NGX_INET_ADDRSTRLEN, 0);
    if (client_key.len == 0) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    now = ngx_current_msec;
    ngx_http_dmmr_rate_prune(rate_tree.root, now);
    rb_node = rate_tree.root;

    while (rb_node != &rate_sentinel && rb_node != NULL) {
        node = (ngx_http_dmmr_rate_node_t *) rb_node;
        
        ngx_int_t cmp = ngx_strncmp(client_key.data, node->key.data,
                                    client_key.len < node->key.len ? client_key.len : node->key.len);
        if (cmp == 0) {
            if (client_key.len < node->key.len) {
                cmp = -1;
            } else if (client_key.len > node->key.len) {
                cmp = 1;
            }
        }

        if (cmp == 0) {
            /* Encontrou */
            if (now - node->timestamp > rate_window) {
                /* Reset */
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

        if (cmp < 0) {
            rb_node = rb_node->left;
        } else {
            rb_node = rb_node->right;
        }
    }

    /* Não encontrou: insere novo nó */
    new_node = ngx_alloc(sizeof(ngx_http_dmmr_rate_node_t), r->connection->log);
    if (new_node == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    new_node->key.data = ngx_alloc(client_key.len, r->connection->log);
    if (new_node->key.data == NULL) {
        ngx_free(new_node);
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    ngx_memcpy(new_node->key.data, client_key.data, client_key.len);
    new_node->key.len = client_key.len;
    new_node->count = 1;
    new_node->timestamp = now;
    new_node->node.key = (uintptr_t) new_node->key.data;
    new_node->node.left = NULL;
    new_node->node.right = NULL;
    new_node->node.parent = NULL;
    new_node->node.color = 1; /* RED */

    ngx_rbtree_insert(&rate_tree, &new_node->node);

    return NGX_OK;
}
