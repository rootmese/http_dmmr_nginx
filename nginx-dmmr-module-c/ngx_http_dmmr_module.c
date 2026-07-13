#include "ngx_http_dmmr_module.h"

/* Handler principal */
ngx_int_t
ngx_http_dmmr_handler(ngx_http_request_t *r)
{
    ngx_http_dmmr_ctx_t *ctx;
    ngx_int_t rc;

    ngx_http_dmmr_conf_t *loc_conf;

    loc_conf = ngx_http_get_module_loc_conf(r, ngx_http_dmmr_module);

    if (loc_conf == NULL || loc_conf->enable != 1) {
        return NGX_DECLINED;
    }

    /* Aloca contexto */
    ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_dmmr_ctx_t));
    if (ctx == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    ngx_http_set_ctx(r, ctx, ngx_http_dmmr_module);

    /* 1. Roteamento */
    rc = ngx_http_dmmr_router(r, ctx);
    if (rc != NGX_OK) {
        if (rc == NGX_HTTP_NOT_FOUND) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "dmmr: no route matched for URI '%V'",
                          &r->uri);
            return NGX_HTTP_NOT_FOUND;
        }
        return rc;
    }

    /* 2. Autenticação */
    rc = ngx_http_dmmr_auth(r, ctx);
    if (rc != NGX_OK) {
        if (rc == NGX_HTTP_UNAUTHORIZED ||
            rc == NGX_HTTP_FORBIDDEN) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "dmmr: authentication failed");
            return rc;
        }
        return rc;
    }

    /* 3. Rate Limiting */
    rc = ngx_http_dmmr_rate_limit(r, ctx);
    if (rc != NGX_OK) {
        if (rc == NGX_HTTP_TOO_MANY_REQUESTS) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "dmmr: rate limit exceeded");
            return rc;
        }
        return rc;
    }

    /* 4. Plugins */
    rc = ngx_http_dmmr_plugins(r, ctx);
    if (rc != NGX_OK) {
        return rc;
    }

    /* 5. Define upstream */
    rc = ngx_http_dmmr_upstream(r, ctx);
    if (rc != NGX_OK) {
        return rc;
    }

    /*
     * O proxy será executado pela diretiva proxy_pass usando
     * $dmmr_upstream
     */
    return NGX_OK;
}

/* ------------------------------------------------------------------ */
/* FUNÇÕES DE CONFIGURAÇÃO (diretivas)                                 */
/* ------------------------------------------------------------------ */

static char *
ngx_http_dmmr_enable(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_dmmr_conf_t *kcf = conf;
    ngx_str_t *value;

    value = cf->args->elts;
    if (cf->args->nelts != 2) {
        return "invalid number of arguments";
    }

    if (ngx_strcmp(value[1].data, "on") == 0) {
        kcf->enable = 1;
    } else if (ngx_strcmp(value[1].data, "off") == 0) {
        kcf->enable = 0;
    } else {
        return "invalid value, must be 'on' or 'off'";
    }

    return NGX_CONF_OK;
}

static char *
ngx_http_dmmr_service(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_dmmr_conf_t *kcf = conf;
    ngx_http_dmmr_service_t *service;
    ngx_str_t *value;

    if (kcf->services == NULL) {
        kcf->services = ngx_array_create(cf->pool, 4, sizeof(ngx_http_dmmr_service_t));
        if (kcf->services == NULL) return NGX_CONF_ERROR;
    }

    service = ngx_array_push(kcf->services);
    if (service == NULL) return NGX_CONF_ERROR;

    value = cf->args->elts;

    service->name = value[1];
    service->host = value[2];
    service->port = value[3];   // agora recebe a porta
    service->protocol.len = 4;
    service->protocol.data = (u_char *) "http";

    return NGX_CONF_OK;
}

static char *
ngx_http_dmmr_route(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_dmmr_conf_t *kcf = conf;   // main_conf
    ngx_http_dmmr_route_t *route;
    ngx_str_t *value;
    ngx_str_t *sname;

    if (kcf->routes == NULL) {
        kcf->routes = ngx_array_create(cf->pool, 4,
                                       sizeof(ngx_http_dmmr_route_t));
        if (kcf->routes == NULL) {
            return NGX_CONF_ERROR;
        }
    }

    route = ngx_array_push(kcf->routes);
    if (route == NULL) {
        return NGX_CONF_ERROR;
    }

    value = cf->args->elts;

    route->path.len = value[1].len;
    route->path.data = ngx_pnalloc(cf->pool, value[1].len);
    if (route->path.data == NULL) return NGX_CONF_ERROR;
    ngx_memcpy(route->path.data, value[1].data, value[1].len);

    sname = ngx_palloc(cf->pool, sizeof(ngx_str_t));
    if (sname == NULL) return NGX_CONF_ERROR;
    sname->len = value[2].len;
    sname->data = ngx_pnalloc(cf->pool, value[2].len + 1);
    if (sname->data == NULL) return NGX_CONF_ERROR;
    ngx_memcpy(sname->data, value[2].data, value[2].len);
    sname->data[sname->len] = '\0';
    route->service_name = sname;

    route->method.len = 0;
    route->method.data = NULL;
    route->hosts = NULL;
    route->strip_path = 0;
    route->preserve_host = 0;
    route->priority = 0;

    return NGX_CONF_OK;
}

static char *
ngx_http_dmmr_rate_limit_set(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_dmmr_conf_t *kcf = conf;
    ngx_str_t *value;
    ngx_int_t limit;

    value = cf->args->elts;
    if (cf->args->nelts != 2) {
        return "invalid number of arguments";
    }

    limit = ngx_atoi(value[1].data, value[1].len);
    if (limit <= 0) {
        return "invalid value";
    }

    kcf->rate_limit = (ngx_uint_t) limit;
    return NGX_CONF_OK;
}

static char *
ngx_http_dmmr_rate_window_set(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_dmmr_conf_t *kcf = conf;
    ngx_str_t *value;
    ngx_int_t window;

    value = cf->args->elts;
    if (cf->args->nelts != 2) {
        return "invalid number of arguments";
    }

    window = ngx_atoi(value[1].data, value[1].len);
    if (window <= 0) {
        return "invalid value";
    }

    kcf->rate_window = (ngx_msec_t) window;
    return NGX_CONF_OK;
}

/* ------------------------------------------------------------------ */
/* COMANDOS DE CONFIGURAÇÃO                                            */
/* ------------------------------------------------------------------ */

static ngx_command_t ngx_http_dmmr_commands[] = {
    { ngx_string("dmmr_enable"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_dmmr_enable,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("dmmr_service"),
    NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE3,
    ngx_http_dmmr_service,
    NGX_HTTP_MAIN_CONF_OFFSET,
    0,
    NULL },

    { ngx_string("dmmr_route"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE2,
      ngx_http_dmmr_route,
      NGX_HTTP_MAIN_CONF_OFFSET,   // essencial
      0,
      NULL },

    { ngx_string("dmmr_cache_addr"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_dmmr_conf_t, cache_addr),
      NULL },

    { ngx_string("dmmr_rate_limit"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_dmmr_rate_limit_set,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("dmmr_rate_window"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_dmmr_rate_window_set,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    ngx_null_command
};

/* ------------------------------------------------------------------ */
/* CRIAÇÃO DE CONFIGURAÇÕES                                            */
/* ------------------------------------------------------------------ */

static void *
ngx_http_dmmr_create_main_conf(ngx_conf_t *cf)
{
    ngx_http_dmmr_conf_t *kcf;

    kcf = ngx_pcalloc(cf->pool, sizeof(ngx_http_dmmr_conf_t));
    if (kcf == NULL) {
        return NULL;
    }

    kcf->enable = NGX_CONF_UNSET;
    kcf->services = NULL;
    kcf->routes = NULL;
    kcf->plugins = NULL;
    kcf->rate_limit = 100;
    kcf->rate_window = 60000;

    return kcf;
}

static void *
ngx_http_dmmr_create_srv_conf(ngx_conf_t *cf)
{
    return ngx_http_dmmr_create_main_conf(cf);
}

static void *
ngx_http_dmmr_create_loc_conf(ngx_conf_t *cf)
{
    return ngx_http_dmmr_create_main_conf(cf);
}

static char *
ngx_http_dmmr_init_main_conf(ngx_conf_t *cf, void *conf)
{
    return NGX_CONF_OK;
}

/* ------------------------------------------------------------------ */
/* MESCLAGEM DE CONFIGURAÇÕES                                          */
/* ------------------------------------------------------------------ */

static char *
ngx_http_dmmr_merge_srv_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_dmmr_conf_t *prev = parent;
    ngx_http_dmmr_conf_t *conf = child;

    if (conf->services == NULL) {
        conf->services = prev->services;
    }
    if (conf->routes == NULL) {
        conf->routes = prev->routes;
    }
    if (conf->plugins == NULL) {
        conf->plugins = prev->plugins;
    }

    if (conf->cache_addr.len == 0) {
        conf->cache_addr = prev->cache_addr;
    }
    if (conf->enable == NGX_CONF_UNSET) {
        conf->enable = prev->enable;
    }
    if (conf->rate_limit == 100 && prev->rate_limit != 100) {
        conf->rate_limit = prev->rate_limit;
    }
    if (conf->rate_window == 60000 && prev->rate_window != 60000) {
        conf->rate_window = prev->rate_window;
    }

    return NGX_CONF_OK;
}

static char *
ngx_http_dmmr_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_dmmr_conf_t *prev = parent;
    ngx_http_dmmr_conf_t *conf = child;

    ngx_conf_merge_value(conf->enable, prev->enable, 0);

    if (conf->services == NULL) {
        conf->services = prev->services;
    }
    if (conf->routes == NULL) {
        conf->routes = prev->routes;
    }
    if (conf->plugins == NULL) {
        conf->plugins = prev->plugins;
    }

    ngx_conf_merge_str_value(conf->cache_addr, prev->cache_addr, "unix:/tmp/dmmr_cache.sock");
    ngx_conf_merge_uint_value(conf->rate_limit, prev->rate_limit, 100);
    ngx_conf_merge_msec_value(conf->rate_window, prev->rate_window, 60000);

    return NGX_CONF_OK;
}

/* ------------------------------------------------------------------ */
/* VARIÁVEIS E INICIALIZAÇÃO                                           */
/* ------------------------------------------------------------------ */

ngx_int_t ngx_http_dmmr_upstream_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data);

static ngx_http_variable_t ngx_http_dmmr_vars[] = {
    { ngx_string("dmmr_upstream"), NULL,
      ngx_http_dmmr_upstream_variable, 0,
      NGX_HTTP_VAR_NOCACHEABLE, 0 },
    ngx_http_null_variable
};

static ngx_int_t
ngx_http_dmmr_preconfiguration(ngx_conf_t *cf)
{
    ngx_http_variable_t  *var, *v;

    for (v = ngx_http_dmmr_vars; v->name.len; v++) {
        var = ngx_http_add_variable(cf, &v->name, v->flags);
        if (var == NULL) {
            return NGX_ERROR;
        }
        var->get_handler = v->get_handler;
        var->data = v->data;
    }

    return NGX_OK;
}

static ngx_int_t
ngx_http_dmmr_postconfiguration(ngx_conf_t *cf)
{
    ngx_http_handler_pt *h;
    ngx_http_core_main_conf_t *cmcf;

    if (ngx_http_dmmr_rate_init(cf->cycle) != NGX_OK) {
        return NGX_ERROR;
    }

    cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);
    h = ngx_array_push(&cmcf->phases[NGX_HTTP_ACCESS_PHASE].handlers);
    if (h == NULL) {
        return NGX_ERROR;
    }

    *h = ngx_http_dmmr_handler;

    return NGX_OK;
}

/* ------------------------------------------------------------------ */
/* CONTEXTO DO MÓDULO                                                  */
/* ------------------------------------------------------------------ */

static ngx_http_module_t ngx_http_dmmr_module_ctx = {
    ngx_http_dmmr_preconfiguration,     /* preconfiguration */
    ngx_http_dmmr_postconfiguration,    /* postconfiguration */
    ngx_http_dmmr_create_main_conf,     /* create main conf */
    ngx_http_dmmr_init_main_conf,       /* init main conf */
    ngx_http_dmmr_create_srv_conf,      /* create server conf */
    ngx_http_dmmr_merge_srv_conf,       /* merge server conf */
    ngx_http_dmmr_create_loc_conf,      /* create location conf */
    ngx_http_dmmr_merge_loc_conf        /* merge location conf */
};

ngx_module_t ngx_http_dmmr_module = {
    NGX_MODULE_V1,
    &ngx_http_dmmr_module_ctx,
    ngx_http_dmmr_commands,
    NGX_HTTP_MODULE,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NGX_MODULE_V1_PADDING
};