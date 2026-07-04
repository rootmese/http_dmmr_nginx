#ifndef DMMR_CONFIG_H
#define DMMR_CONFIG_H

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

#endif /* DMMR_CONFIG_H */