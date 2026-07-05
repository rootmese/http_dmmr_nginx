#include "dmmr_db.h"
#include "dmmr_config.h"
#include "dmmr_protocol.h"
#include <db.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern DB *dbp; /* definido no servidor principal */

int init_db(void) {
    int ret;
    if ((ret = db_create(&dbp, NULL, 0)) != 0) {
        fprintf(stderr, "db_create: %s\n", db_strerror(ret));
        return -1;
    }
    if ((ret = dbp->open(dbp, NULL, DB_PATH, NULL, DB_BTREE, DB_CREATE | DB_THREAD, 0644)) != 0) {
        dbp->err(dbp, ret, "Database open failed");
        return -1;
    }
    return 0;
}

void close_db(void) {
    if (dbp != NULL) {
        dbp->close(dbp, 0);
        dbp = NULL;
    }
}

int db_get_with_meta(const char *key, size_t key_len,
                     uint64_t *ts_out, uint64_t *node_id_out,
                     void **value_out, size_t *value_len_out,
                     uint64_t *expire_at_out) {
    DBT key_dbt, data_dbt;
    memset(&key_dbt, 0, sizeof(key_dbt));
    memset(&data_dbt, 0, sizeof(data_dbt));
    key_dbt.data = (void *) key;
    key_dbt.size = (u_int32_t) key_len;
    data_dbt.flags = DB_DBT_MALLOC;

    int rc = dbp->get(dbp, NULL, &key_dbt, &data_dbt, 0);
    if (rc == DB_NOTFOUND) return -1;
    if (rc != 0) return -2;
    if (data_dbt.size < sizeof(struct cache_entry)) {
        free(data_dbt.data);
        return -3;
    }

    struct cache_entry *entry = (struct cache_entry *) data_dbt.data;
    *ts_out = entry->timestamp;
    *node_id_out = entry->node_id;
    *value_len_out = entry->value_len;
    if (expire_at_out)
        *expire_at_out = entry->expire_at;

    if (value_out != NULL) {
        *value_out = malloc(entry->value_len);
        if (*value_out == NULL) {
            free(data_dbt.data);
            return -4;
        }
        memcpy(*value_out, (uint8_t *) data_dbt.data + sizeof(struct cache_entry), entry->value_len);
    }

    free(data_dbt.data);
    return 0;
}

int db_set_with_meta(const char *key, size_t key_len,
                     uint64_t ts, uint64_t node_id,
                     const void *value, size_t value_len,
                     uint64_t expire_at) {
    DBT key_dbt, data_dbt, old_dbt;
    memset(&key_dbt, 0, sizeof(key_dbt));
    memset(&data_dbt, 0, sizeof(data_dbt));
    memset(&old_dbt, 0, sizeof(old_dbt));

    key_dbt.data = (void *) key;
    key_dbt.size = (u_int32_t) key_len;
    old_dbt.flags = DB_DBT_MALLOC;

    int rc = dbp->get(dbp, NULL, &key_dbt, &old_dbt, 0);
    if (rc == 0 && old_dbt.size >= sizeof(struct cache_entry)) {
        struct cache_entry *existing = (struct cache_entry *) old_dbt.data;
        if (existing->timestamp > ts || (existing->timestamp == ts && existing->node_id >= node_id)) {
            free(old_dbt.data);
            return 0; /* não atualiza */
        }
    } else if (rc != DB_NOTFOUND) {
        if (old_dbt.data != NULL) free(old_dbt.data);
        return -1;
    }

    size_t total_len = sizeof(struct cache_entry) + value_len;
    uint8_t *buf = malloc(total_len);
    if (buf == NULL) {
        if (old_dbt.data != NULL) free(old_dbt.data);
        return -2;
    }

    struct cache_entry *entry = (struct cache_entry *) buf;
    entry->timestamp = ts;
    entry->node_id = node_id;
    entry->value_len = (uint32_t) value_len;
    entry->expire_at = expire_at;   
    if (value_len )
        memcpy(buf + sizeof(struct cache_entry), value, value_len);

    data_dbt.data = buf;
    data_dbt.size = (u_int32_t) total_len;
    rc = dbp->put(dbp, NULL, &key_dbt, &data_dbt, 0);
    free(buf);
    if (old_dbt.data != NULL) free(old_dbt.data);
    return (rc == 0) ? 0 : -3;
}

int db_del_key(const char *key, size_t key_len) {
    DBT key_dbt;
    memset(&key_dbt, 0, sizeof(key_dbt));
    key_dbt.data = (void *) key;
    key_dbt.size = (u_int32_t) key_len;
    return dbp->del(dbp, NULL, &key_dbt, 0);
}