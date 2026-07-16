#ifndef DMMR_CONFIG_H
#define DMMR_CONFIG_H

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <limits.h>

#ifdef DEBUG
#define DMMR_LOG_DEBUG(fmt, ...) fprintf(stderr, "[DEBUG] " fmt "\n", ##__VA_ARGS__)
#else
#define DMMR_LOG_DEBUG(fmt, ...) do {} while(0)
#endif

#define DMMR_MAGIC 0xD4D4u
#define DMMR_VERSION 1
#define DMMR_PROTO_OP_GET 1
#define DMMR_PROTO_OP_SET 2
#define DMMR_PROTO_OP_DEL 3
#define DMMR_PROTO_STATUS_OK 0
#define DMMR_PROTO_STATUS_NOT_FOUND 1
#define DMMR_PROTO_STATUS_ERROR 2

#define PORT 9080
#define CLUSTER_PORT 9081
#define DB_PATH "./apikeys.db"
#define SOCK_PATH "/tmp/dmmr_cache.sock"
#define DEFAULT_WORKERS 4
#define QUEUE_MAX 128
#define MAX_KEY_LEN 1024
#define MAX_VALUE_LEN (1024 * 1024)

#define HIGH_WATERMARK  50   // ativa workers quando a fila ultrapassar este valor
#define LOW_WATERMARK   10   // desativa quando cair abaixo
#define BROADCAST_WORKERS 2  // número máximo de workers auxiliares

#define TTL_SCAN_CHUNK_SIZE 100   // chaves por ciclo de varredura de TTL
#define GC_FLUSH_INTERVAL_MS 100  // intervalo do garbage collector (ms)
#define POOL_INITIAL_SIZE 0x400   // tamanho inicial dos pools (1024 entradas)

/*
 * Container/runtime configuration.  Defaults intentionally preserve the
 * command-line behaviour used by the existing test suite.
 */
static inline const char *
dmmr_env_string(const char *name, const char *default_value)
{
    const char *value = getenv(name);
    return (value != NULL && value[0] != '\0') ? value : default_value;
}

static inline int
dmmr_env_int(const char *name, int default_value, int min_value, int max_value)
{
    const char *value = getenv(name);
    char *end;
    long parsed;

    if (value == NULL || value[0] == '\0') {
        return default_value;
    }

    errno = 0;
    parsed = strtol(value, &end, 10);
    if (errno != 0 || *end != '\0' || parsed < min_value || parsed > max_value) {
        fprintf(stderr, "Ignoring invalid %s=%s; using %d\n", name, value, default_value);
        return default_value;
    }

    return (int) parsed;
}

static inline unsigned int
dmmr_env_mode(const char *name, unsigned int default_value)
{
    const char *value = getenv(name);
    char *end;
    unsigned long parsed;

    if (value == NULL || value[0] == '\0') {
        return default_value;
    }

    errno = 0;
    parsed = strtoul(value, &end, 8);
    if (errno != 0 || *end != '\0' || parsed > 0777) {
        fprintf(stderr, "Ignoring invalid %s=%s; using %04o\n",
                name, value, default_value);
        return default_value;
    }

    return (unsigned int) parsed;
}

#endif /* DMMR_CONFIG_H */
