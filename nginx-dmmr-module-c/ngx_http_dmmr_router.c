#include "ngx_http_dmmr_module.h"


/* ------------------------------------------------------------------ */
/* IMPLEMENTAÇÕES DAS FUNÇÕES AUXILIARES DE MATCH                      */
/* ------------------------------------------------------------------ */

ngx_int_t
ngx_http_dmmr_match_path(ngx_str_t *uri, ngx_str_t *path)
{
    if (path->len == 0) {
        return NGX_OK;
    }
    if (uri->len < path->len) {
        return NGX_DECLINED;
    }
    if (ngx_strncmp(uri->data, path->data, path->len) == 0) {
        return NGX_OK;
    }
    return NGX_DECLINED;
}

ngx_int_t
ngx_http_dmmr_match_method(ngx_str_t *method, ngx_str_t *route_method)
{
    if (route_method->len == 0) {
        return NGX_OK;
    }
    if (method->len != route_method->len) {
        return NGX_DECLINED;
    }
    if (ngx_strncasecmp(method->data, route_method->data, method->len) == 0) {
        return NGX_OK;
    }
    return NGX_DECLINED;
}

ngx_int_t
ngx_http_dmmr_match_host(ngx_str_t *host, ngx_array_t *hosts)
{
    ngx_str_t *h;
    ngx_uint_t i;

    if (hosts == NULL || hosts->nelts == 0) {
        return NGX_OK;
    }
    if (host == NULL || host->len == 0) {
        return NGX_DECLINED;
    }

    h = hosts->elts;
    for (i = 0; i < hosts->nelts; i++) {
        if (h[i].len == host->len &&
            ngx_strncasecmp(h[i].data, host->data, host->len) == 0)
        {
            return NGX_OK;
        }
    }
    return NGX_DECLINED;
}

ngx_int_t
ngx_http_dmmr_router(ngx_http_request_t *r, ngx_http_dmmr_ctx_t *ctx)
{
    ngx_http_dmmr_conf_t *kcf;
    ngx_http_dmmr_route_t *routes, *best_route = NULL;
    ngx_uint_t i;
    ngx_int_t rc;

    kcf = ngx_http_get_module_main_conf(r, ngx_http_dmmr_module);

    if (kcf == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "DMMR: main conf is NULL");
        return NGX_DECLINED;
    }

    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "DMMR: routes=%ui services=%ui",
                  kcf->routes ? kcf->routes->nelts : 0,
                  kcf->services ? kcf->services->nelts : 0);

    if (kcf->routes == NULL || kcf->routes->nelts == 0) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "DMMR: no routes configured");
        return NGX_DECLINED;
    }

    routes = kcf->routes->elts;

    for (i = 0; i < kcf->routes->nelts; i++) {

        rc = ngx_http_dmmr_match_path(&r->uri, &routes[i].path);

        if (rc != NGX_OK) {
            continue;
        }

        if (routes[i].method.len > 0) {
            rc = ngx_http_dmmr_match_method(&r->method_name,
                                            &routes[i].method);

            if (rc != NGX_OK) {
                continue;
            }
        }

        if (routes[i].hosts != NULL &&
            routes[i].hosts->nelts > 0) {

            if (r->headers_in.host == NULL) {
                continue;
            }

            rc = ngx_http_dmmr_match_host(&r->headers_in.host->value,
                                          routes[i].hosts);

            if (rc != NGX_OK) {
                continue;
            }
        }

        if (best_route == NULL ||
            routes[i].priority > best_route->priority) {

            best_route = &routes[i];
        }
    }


    if (best_route == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "DMMR: no route matched uri=%V",
                      &r->uri);

        return NGX_HTTP_NOT_FOUND;
    }


    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "DMMR: matched route=%V",
                  &best_route->path);


    ctx->route = best_route;


    if (best_route->service_name != NULL &&
        kcf->services != NULL) {

        ngx_http_dmmr_service_t *services = kcf->services->elts;

        for (i = 0; i < kcf->services->nelts; i++) {

            if (services[i].name.len ==
                best_route->service_name->len &&
                ngx_memcmp(services[i].name.data,
                           best_route->service_name->data,
                           services[i].name.len) == 0) {

                ctx->service = &services[i];

                ngx_log_error(NGX_LOG_ERR,
                              r->connection->log,
                              0,
                              "DMMR: service=%V host=%V port=%V",
                              &services[i].name,
                              &services[i].host,
                              &services[i].port);

                break;
            }
        }


        if (ctx->service == NULL) {

            ngx_log_error(NGX_LOG_ERR,
                          r->connection->log,
                          0,
                          "DMMR: service '%V' not found",
                          best_route->service_name);

            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
    }


    if (best_route->strip_path) {

        ngx_str_t *new_uri;

        new_uri = ngx_palloc(r->pool, sizeof(ngx_str_t));

        if (new_uri == NULL) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        new_uri->len = r->uri.len - best_route->path.len;

        if (new_uri->len == 0) {

            new_uri->data = (u_char *) "/";
            new_uri->len = 1;

        } else {

            new_uri->data = ngx_pstrdup(r->pool, &r->uri);

            if (new_uri->data == NULL) {
                return NGX_HTTP_INTERNAL_SERVER_ERROR;
            }

            ngx_memmove(new_uri->data,
                        new_uri->data + best_route->path.len,
                        new_uri->len);
        }

        ctx->new_uri = new_uri;
    }


    return NGX_OK;
}

