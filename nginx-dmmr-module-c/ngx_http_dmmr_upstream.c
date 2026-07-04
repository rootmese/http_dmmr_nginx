#include "ngx_http_dmmr_module.h"

ngx_int_t
ngx_http_dmmr_upstream(ngx_http_request_t *r, ngx_http_dmmr_ctx_t *ctx)
{
    ngx_http_dmmr_service_t *service = ctx->service;
    ngx_str_t *upstream_val;

    if (service == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    /* Aloca a string de upstream: host:port */
    upstream_val = ngx_palloc(r->pool, sizeof(ngx_str_t));
    if (upstream_val == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    upstream_val->len = service->host.len + 1 + service->port.len;
    upstream_val->data = ngx_palloc(r->pool, upstream_val->len);
    if (upstream_val->data == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    u_char *p = ngx_cpymem(upstream_val->data, service->host.data, service->host.len);
    *p++ = ':';
    ngx_memcpy(p, service->port.data, service->port.len);

    ctx->upstream_name = upstream_val;

    /* Se tiver new_uri (strip_path), substitui */
    if (ctx->new_uri != NULL) {
        r->uri = *ctx->new_uri;
    }

    return NGX_OK;
}

ngx_int_t
ngx_http_dmmr_upstream_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{
    ngx_http_dmmr_ctx_t  *ctx;

    ctx = ngx_http_get_module_ctx(r, ngx_http_dmmr_module);
    if (ctx == NULL || ctx->upstream_name == NULL || ctx->upstream_name->len == 0) {
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
