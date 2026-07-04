#ifndef DMMR_NET_H
#define DMMR_NET_H

#include <sys/types.h>
#include <stddef.h>

ssize_t recv_full(int fd, void *buf, size_t len, int flags);
ssize_t send_full(int fd, const void *buf, size_t len, int flags);

#endif /* DMMR_NET_H */