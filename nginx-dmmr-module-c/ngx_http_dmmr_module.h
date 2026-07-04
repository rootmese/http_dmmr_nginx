#ifndef _NGX_HTTP_dmmr_MODULE_H_INCLUDED_
#define _NGX_HTTP_dmmr_MODULE_H_INCLUDED_

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

/* Estrutura de uma rota */
typedef struct {
    ngx_str_t            path;
    ngx_str_t            method;
    ngx_str_t           *service_name;
    ngx_array_t         *hosts;          /* lista de strings */
    ngx_flag_t           strip_path;
    ngx_flag_t           preserve_host;
    ngx_uint_t           priority;       /* para ordenação */
} ngx_http_dmmr_route_t;

/* Estrutura de um serviço (upstream) */
typedef struct {
    ngx_str_t            name;
    ngx_str_t            protocol;      /* http/https */
    ngx_str_t            host;
    ngx_str_t            port;
    ngx_str_t            path;          /* prefixo opcional */
    ngx_array_t         *upstream_servers; /* lista de ngx_http_upstream_server_t */
} ngx_http_dmmr_service_t;

/* Estrutura de um plugin */
typedef struct {
    ngx_str_t            name;
    ngx_str_t           *config;        /* JSON ou outro formato */
    ngx_flag_t           enabled;
    ngx_int_t           (*handler)(ngx_http_request_t *r, void *config);
    void                *config_data;   /* dados parseados */
} ngx_http_dmmr_plugin_t;

/* Contexto de requisição */
typedef struct {
    ngx_http_dmmr_route_t    *route;
    ngx_http_dmmr_service_t  *service;
    ngx_str_t                *upstream_name;  /* nome do upstream a ser usado */
    ngx_str_t                *new_uri;        /* URI modificada (strip_path) */
    ngx_flag_t                authenticated;
    ngx_str_t                *auth_user;      /* usuário autenticado */
    void                    *plugin_data;     /* dados específicos do plugin */
} ngx_http_dmmr_ctx_t;

/* Configuração do módulo (por location) */
typedef struct {
    ngx_flag_t           enable;
    ngx_array_t         *services;       /* lista de ngx_http_dmmr_service_t */
    ngx_array_t         *routes;         /* lista de ngx_http_dmmr_route_t */
    ngx_array_t         *plugins;        /* lista de ngx_http_dmmr_plugin_t */
    ngx_str_t            config_file;    /* opcional: arquivo JSON com config */
    ngx_str_t            cache_addr;     /* endereço do cache (ex: unix:/tmp/dmmr_cache.sock ou 127.0.0.1:9080) */
    ngx_uint_t           rate_limit;     /* limite de requisições por janela */
    ngx_msec_t           rate_window;    /* janela de rate limit em ms */
} ngx_http_dmmr_conf_t;

/* Funções públicas */
ngx_int_t ngx_http_dmmr_handler(ngx_http_request_t *r);
ngx_int_t ngx_http_dmmr_router(ngx_http_request_t *r, ngx_http_dmmr_ctx_t *ctx);
ngx_int_t ngx_http_dmmr_auth(ngx_http_request_t *r, ngx_http_dmmr_ctx_t *ctx);
ngx_int_t ngx_http_dmmr_rate_init(ngx_cycle_t *cycle);
ngx_int_t ngx_http_dmmr_rate_limit(ngx_http_request_t *r, ngx_http_dmmr_ctx_t *ctx);
ngx_int_t ngx_http_dmmr_plugins(ngx_http_request_t *r, ngx_http_dmmr_ctx_t *ctx);
ngx_int_t ngx_http_dmmr_upstream(ngx_http_request_t *r, ngx_http_dmmr_ctx_t *ctx);

/* Funções auxiliares */
ngx_int_t ngx_http_dmmr_parse_config(ngx_conf_t *cf, ngx_http_dmmr_conf_t *conf);
ngx_int_t ngx_http_dmmr_match_path(ngx_str_t *uri, ngx_str_t *path);
ngx_int_t ngx_http_dmmr_match_host(ngx_str_t *host, ngx_array_t *hosts);
ngx_int_t ngx_http_dmmr_match_method(ngx_str_t *method, ngx_str_t *route_method);

extern ngx_module_t ngx_http_dmmr_module;

#endif
