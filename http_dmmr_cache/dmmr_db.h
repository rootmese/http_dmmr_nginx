#ifndef DMMR_DB_H
#define DMMR_DB_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

enum dmmr_db_result {
    DMMR_DB_APPLIED = 0,
    DMMR_DB_IGNORED = 1,
    DMMR_DB_NOT_FOUND = 2,
    DMMR_DB_ERROR = -1,
};

struct dmmr_db_record {
    char *key;
    size_t key_len;
    uint8_t *value;
    size_t value_len;
    uint64_t timestamp;
    uint64_t node_id;
    uint64_t expire_at;
    bool tombstone;
};

typedef int (*dmmr_db_snapshot_cb)(const struct dmmr_db_record *record, void *arg);

int init_db(void);
void close_db(void);

int db_get_with_meta(const char *key, size_t key_len,
                     uint64_t *ts_out, uint64_t *node_id_out,
                     void **value_out, size_t *value_len_out,
                     uint64_t *expire_at_out);

int db_set_with_meta(const char *key, size_t key_len,
                     uint64_t ts, uint64_t node_id,
                     const void *value, size_t value_len, uint64_t expire_at);

int db_apply_record(const char *key, size_t key_len,
                    uint64_t ts, uint64_t node_id, uint64_t expire_at,
                    bool tombstone, const void *value, size_t value_len);

int db_tombstone_if_match(const char *key, size_t key_len,
                          uint64_t observed_ts, uint64_t observed_node_id,
                          uint64_t observed_expire_at,
                          uint64_t tombstone_ts, uint64_t tombstone_node_id);

int db_snapshot_foreach(dmmr_db_snapshot_cb callback, void *arg);

int db_del_key(const char *key, size_t key_len);

#endif /* DMMR_DB_H */
