#include "ngx_http_dmmr_module.h"

ngx_int_t
ngx_http_dmmr_upstream(ngx_http_request_t *r, ngx_http_dmmr_ctx_t *ctx)
{
    ngx_http_dmmr_service_t *service = ctx->service;
    ngx_str_t *upstream_val;

    if (service == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "dmmr_upstream: service is NULL");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
    "DMMR: selected service=%V host=%V port=%V",
    &service->name,
    &service->host,
    &service->port);

    // Se já foi setado anteriormente (ex: no router ou na própria variável), não sobrescreve à toa
    if (ctx->upstream_name != NULL) {
        if (ctx->new_uri != NULL) {
            r->uri = *ctx->new_uri;
        }
        return NGX_OK;
    }

    upstream_val = ngx_palloc(r->pool, sizeof(ngx_str_t));
    if (upstream_val == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "dmmr_upstream: palloc upstream_val failed");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    upstream_val->len = service->host.len + 1 + service->port.len;
    upstream_val->data = ngx_palloc(r->pool, upstream_val->len);
    if (upstream_val->data == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "dmmr_upstream: palloc upstream_val->data failed");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    u_char *p = ngx_cpymem(upstream_val->data, service->host.data, service->host.len);
    *p++ = ':';
    ngx_memcpy(p, service->port.data, service->port.len);

    ctx->upstream_name = upstream_val;

    ngx_log_debug3(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "dmmr upstream: host=%V port=%V final=%V",
                   &service->host, &service->port, upstream_val);

    if (ctx->new_uri != NULL) {
        r->uri = *ctx->new_uri;
    }

    return NGX_OK;
}

ngx_int_t
ngx_http_dmmr_upstream_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{
    ngx_http_dmmr_ctx_t *ctx;

    ctx = ngx_http_get_module_ctx(r, ngx_http_dmmr_module);

    if (ctx == NULL) {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "dmmr: $dmmr_upstream ctx is NULL");
        v->not_found = 1;
        return NGX_OK;
    }

    // CORREÇÃO ESSENCIAL: Se a variável for avaliada pelo proxy_pass antes da fase 5
    // (ou se as fases anteriores falharem), nós forçamos a geração dinâmica do valor aqui.
    if (ctx->upstream_name == NULL && ctx->service != NULL) {
        (void) ngx_http_dmmr_upstream(r, ctx);
    }

    if (ctx->upstream_name == NULL) {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "dmmr: $dmmr_upstream not set");
        v->not_found = 1;
        return NGX_OK;
    }

    v->len = ctx->upstream_name->len;
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    v->data = ctx->upstream_name->data;

    return NGX_OK;
}
