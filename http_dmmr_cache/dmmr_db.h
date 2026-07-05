#ifndef DMMR_DB_H
#define DMMR_DB_H

#include <stddef.h>
#include <stdint.h>

int init_db(void);
void close_db(void);

int db_get_with_meta(const char *key, size_t key_len,
                     uint64_t *ts_out, uint64_t *node_id_out,
                     void **value_out, size_t *value_len_out,
                     uint64_t *expire_at_out);

int db_set_with_meta(const char *key, size_t key_len,
                     uint64_t ts, uint64_t node_id,
                     const void *value, size_t value_len, uint64_t expire_at);

int db_del_key(const char *key, size_t key_len);

#endif /* DMMR_DB_H */