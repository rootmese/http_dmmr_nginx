#include "dmmr_db.h"
#include "dmmr_config.h"
#include "dmmr_protocol.h"
#include "dmmr_server.h"
#include <db.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern DB *dbp; /* definido no servidor principal */
extern uint64_t my_node_id;

/* Berkeley DB's DB_THREAD protects the handle, not get/compare/put as LWW. */
static pthread_mutex_t db_mutex = PTHREAD_MUTEX_INITIALIZER;

static int entry_is_valid(const DBT *data, const struct cache_entry *entry)
{
    uint32_t value_len;

    if (data->size < sizeof(*entry)) {
        return 0;
    }

    value_len = dmmr_record_value_len(entry);
    if (value_len > MAX_VALUE_LEN ||
        (size_t)data->size < sizeof(*entry) + value_len) {
        return 0;
    }

    if (dmmr_record_is_tombstone(entry) && value_len != 0) {
        return 0;
    }

    return 1;
}

static int incoming_wins(const struct cache_entry *existing,
                         uint64_t timestamp, uint64_t node_id)
{
    return existing->timestamp < timestamp ||
           (existing->timestamp == timestamp && existing->node_id < node_id);
}

static int put_record_locked(const char *key, size_t key_len,
                             uint64_t timestamp, uint64_t node_id,
                             uint64_t expire_at, int tombstone,
                             const void *value, size_t value_len)
{
    DBT key_dbt;
    DBT data_dbt;
    uint8_t *buffer;
    struct cache_entry *entry;
    size_t total_len;
    int rc;

    total_len = sizeof(*entry) + (tombstone ? 0 : value_len);
    buffer = malloc(total_len);
    if (buffer == NULL) {
        return DMMR_DB_ERROR;
    }

    entry = (struct cache_entry *)buffer;
    entry->timestamp = timestamp;
    entry->node_id = node_id;
    entry->value_len = (uint32_t)(tombstone ? DMMR_RECORD_TOMBSTONE : value_len);
    entry->expire_at = expire_at;
    if (!tombstone && value_len > 0) {
        memcpy(buffer + sizeof(*entry), value, value_len);
    }

    memset(&key_dbt, 0, sizeof(key_dbt));
    memset(&data_dbt, 0, sizeof(data_dbt));
    key_dbt.data = (void *)key;
    key_dbt.size = (u_int32_t)key_len;
    data_dbt.flags = DB_DBT_MALLOC;
    data_dbt.data = buffer;
    data_dbt.size = (u_int32_t)total_len;
    rc = dbp->put(dbp, NULL, &key_dbt, &data_dbt, 0);
    free(buffer);
    return rc == 0 ? DMMR_DB_APPLIED : DMMR_DB_ERROR;
}

int init_db(void)
{
    int ret;

    if ((ret = db_create(&dbp, NULL, 0)) != 0) {
        fprintf(stderr, "db_create: %s\n", db_strerror(ret));
        return -1;
    }
    if ((ret = dbp->open(dbp, NULL, dmmr_env_string("DMMR_DB_PATH", DB_PATH), NULL,
                         DB_BTREE, DB_CREATE | DB_THREAD, 0644)) != 0) {
        dbp->err(dbp, ret, "Database open failed");
        return -1;
    }
    return 0;
}

void close_db(void)
{
    pthread_mutex_lock(&db_mutex);
    if (dbp != NULL) {
        dbp->close(dbp, 0);
        dbp = NULL;
    }
    pthread_mutex_unlock(&db_mutex);
}

int db_get_with_meta(const char *key, size_t key_len,
                     uint64_t *ts_out, uint64_t *node_id_out,
                     void **value_out, size_t *value_len_out,
                     uint64_t *expire_at_out)
{
    DBT key_dbt;
    DBT data_dbt;
    struct cache_entry *entry;
    uint32_t value_len;
    int rc;

    if (key == NULL || key_len == 0 || key_len > MAX_KEY_LEN ||
        ts_out == NULL || node_id_out == NULL || value_len_out == NULL) {
        return DMMR_DB_ERROR;
    }

    if (value_out != NULL) {
        *value_out = NULL;
    }

    memset(&key_dbt, 0, sizeof(key_dbt));
    memset(&data_dbt, 0, sizeof(data_dbt));
    key_dbt.data = (void *)key;
    key_dbt.size = (u_int32_t)key_len;
    data_dbt.flags = DB_DBT_MALLOC;

    pthread_mutex_lock(&db_mutex);
    if (dbp == NULL) {
        pthread_mutex_unlock(&db_mutex);
        return DMMR_DB_ERROR;
    }
    rc = dbp->get(dbp, NULL, &key_dbt, &data_dbt, 0);
    if (rc == DB_NOTFOUND) {
        pthread_mutex_unlock(&db_mutex);
        return DMMR_DB_NOT_FOUND;
    }
    if (rc != 0) {
        pthread_mutex_unlock(&db_mutex);
        return DMMR_DB_ERROR;
    }

    entry = (struct cache_entry *)data_dbt.data;
    if (!entry_is_valid(&data_dbt, entry)) {
        free(data_dbt.data);
        pthread_mutex_unlock(&db_mutex);
        return DMMR_DB_ERROR;
    }
    if (dmmr_record_is_tombstone(entry)) {
        free(data_dbt.data);
        pthread_mutex_unlock(&db_mutex);
        return DMMR_DB_NOT_FOUND;
    }

    value_len = dmmr_record_value_len(entry);
    *ts_out = entry->timestamp;
    *node_id_out = entry->node_id;
    *value_len_out = value_len;
    if (expire_at_out != NULL) {
        *expire_at_out = entry->expire_at;
    }
    if (value_out != NULL && value_len > 0) {
        *value_out = malloc(value_len);
        if (*value_out == NULL) {
            free(data_dbt.data);
            pthread_mutex_unlock(&db_mutex);
            return DMMR_DB_ERROR;
        }
        memcpy(*value_out, (uint8_t *)data_dbt.data + sizeof(*entry), value_len);
    }

    free(data_dbt.data);
    pthread_mutex_unlock(&db_mutex);
    return DMMR_DB_APPLIED;
}

int db_apply_record(const char *key, size_t key_len,
                    uint64_t timestamp, uint64_t node_id, uint64_t expire_at,
                    bool tombstone, const void *value, size_t value_len)
{
    DBT key_dbt;
    DBT old_dbt;
    struct cache_entry *existing;
    int rc;

    if (key == NULL || key_len == 0 || key_len > MAX_KEY_LEN ||
        value_len > MAX_VALUE_LEN || (tombstone && value_len != 0) ||
        (!tombstone && value_len > 0 && value == NULL)) {
        return DMMR_DB_ERROR;
    }

    memset(&key_dbt, 0, sizeof(key_dbt));
    memset(&old_dbt, 0, sizeof(old_dbt));
    key_dbt.data = (void *)key;
    key_dbt.size = (u_int32_t)key_len;
    old_dbt.flags = DB_DBT_MALLOC;

    pthread_mutex_lock(&db_mutex);
    if (dbp == NULL) {
        pthread_mutex_unlock(&db_mutex);
        return DMMR_DB_ERROR;
    }
    rc = dbp->get(dbp, NULL, &key_dbt, &old_dbt, 0);
    if (rc == 0) {
        existing = (struct cache_entry *)old_dbt.data;
        if (!entry_is_valid(&old_dbt, existing)) {
            free(old_dbt.data);
            pthread_mutex_unlock(&db_mutex);
            return DMMR_DB_ERROR;
        }
        if (!incoming_wins(existing, timestamp, node_id)) {
            free(old_dbt.data);
            pthread_mutex_unlock(&db_mutex);
            return DMMR_DB_IGNORED;
        }
        free(old_dbt.data);
    } else if (rc != DB_NOTFOUND) {
        if (old_dbt.data != NULL) {
            free(old_dbt.data);
        }
        pthread_mutex_unlock(&db_mutex);
        return DMMR_DB_ERROR;
    }

    rc = put_record_locked(key, key_len, timestamp, node_id, expire_at,
                           tombstone, value, value_len);
    pthread_mutex_unlock(&db_mutex);
    return rc;
}

int db_set_with_meta(const char *key, size_t key_len,
                     uint64_t timestamp, uint64_t node_id,
                     const void *value, size_t value_len, uint64_t expire_at)
{
    return db_apply_record(key, key_len, timestamp, node_id, expire_at,
                           false, value, value_len);
}

int db_tombstone_if_match(const char *key, size_t key_len,
                          uint64_t observed_ts, uint64_t observed_node_id,
                          uint64_t observed_expire_at,
                          uint64_t tombstone_ts, uint64_t tombstone_node_id)
{
    DBT key_dbt;
    DBT old_dbt;
    struct cache_entry *existing;
    int rc;

    if (key == NULL || key_len == 0 || key_len > MAX_KEY_LEN) {
        return DMMR_DB_ERROR;
    }

    memset(&key_dbt, 0, sizeof(key_dbt));
    memset(&old_dbt, 0, sizeof(old_dbt));
    key_dbt.data = (void *)key;
    key_dbt.size = (u_int32_t)key_len;
    old_dbt.flags = DB_DBT_MALLOC;

    pthread_mutex_lock(&db_mutex);
    if (dbp == NULL) {
        pthread_mutex_unlock(&db_mutex);
        return DMMR_DB_ERROR;
    }
    rc = dbp->get(dbp, NULL, &key_dbt, &old_dbt, 0);
    if (rc == DB_NOTFOUND) {
        pthread_mutex_unlock(&db_mutex);
        return DMMR_DB_NOT_FOUND;
    }
    if (rc != 0) {
        pthread_mutex_unlock(&db_mutex);
        return DMMR_DB_ERROR;
    }

    existing = (struct cache_entry *)old_dbt.data;
    if (!entry_is_valid(&old_dbt, existing) ||
        dmmr_record_is_tombstone(existing) ||
        existing->timestamp != observed_ts ||
        existing->node_id != observed_node_id ||
        existing->expire_at != observed_expire_at) {
        free(old_dbt.data);
        pthread_mutex_unlock(&db_mutex);
        return DMMR_DB_IGNORED;
    }
    free(old_dbt.data);

    rc = put_record_locked(key, key_len, tombstone_ts, tombstone_node_id,
                           0, true, NULL, 0);
    pthread_mutex_unlock(&db_mutex);
    return rc;
}

int db_snapshot_foreach(dmmr_db_snapshot_cb callback, void *arg)
{
    DBC *cursor = NULL;
    DBT key_dbt;
    DBT data_dbt;
    int flags = DB_FIRST;
    int rc = DMMR_DB_APPLIED;

    if (callback == NULL) {
        return DMMR_DB_ERROR;
    }

    pthread_mutex_lock(&db_mutex);
    if (dbp == NULL || dbp->cursor(dbp, NULL, &cursor, 0) != 0) {
        pthread_mutex_unlock(&db_mutex);
        return DMMR_DB_ERROR;
    }
    pthread_mutex_unlock(&db_mutex);

    for (;;) {
        struct cache_entry *entry;
        struct dmmr_db_record record;
        uint32_t value_len;

        memset(&key_dbt, 0, sizeof(key_dbt));
        memset(&data_dbt, 0, sizeof(data_dbt));
        data_dbt.flags = DB_DBT_MALLOC;

        pthread_mutex_lock(&db_mutex);
        rc = cursor->get(cursor, &key_dbt, &data_dbt, flags);
        pthread_mutex_unlock(&db_mutex);
        flags = DB_NEXT;

        if (rc == DB_NOTFOUND) {
            rc = DMMR_DB_APPLIED;
            break;
        }
        if (rc != 0) {
            rc = DMMR_DB_ERROR;
            break;
        }

        entry = (struct cache_entry *)data_dbt.data;
        if (!entry_is_valid(&data_dbt, entry) || key_dbt.size == 0 ||
            key_dbt.size > MAX_KEY_LEN) {
            free(data_dbt.data);
            rc = DMMR_DB_ERROR;
            break;
        }

        value_len = dmmr_record_value_len(entry);
        memset(&record, 0, sizeof(record));
        record.key = malloc(key_dbt.size);
        if (record.key == NULL) {
            free(data_dbt.data);
            rc = DMMR_DB_ERROR;
            break;
        }
        memcpy(record.key, key_dbt.data, key_dbt.size);
        record.key_len = key_dbt.size;
        record.timestamp = entry->timestamp;
        record.node_id = entry->node_id;
        record.expire_at = entry->expire_at;
        record.tombstone = dmmr_record_is_tombstone(entry);
        record.value_len = value_len;
        if (value_len > 0) {
            record.value = malloc(value_len);
            if (record.value == NULL) {
                free(record.key);
                free(data_dbt.data);
                rc = DMMR_DB_ERROR;
                break;
            }
            memcpy(record.value, (uint8_t *)data_dbt.data + sizeof(*entry), value_len);
        }
        free(data_dbt.data);

        rc = callback(&record, arg);
        free(record.key);
        free(record.value);
        if (rc != 0) {
            rc = DMMR_DB_ERROR;
            break;
        }
    }

    pthread_mutex_lock(&db_mutex);
    cursor->close(cursor);
    pthread_mutex_unlock(&db_mutex);
    return rc;
}

int db_del_key(const char *key, size_t key_len)
{
    uint64_t ts = now_micros();
    return db_apply_record(key, key_len, ts, my_node_id, 0, true, NULL, 0);
}
