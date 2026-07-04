#include "dmmr_net.h"
#include <errno.h>
#include <unistd.h>

ssize_t recv_full(int fd, void *buf, size_t len, int flags) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = recv(fd, (char *) buf + total, len - total, flags);
        if (n <= 0) {
            if (n == 0) return (ssize_t) total;
            if (errno == EINTR) continue;
            return -1;
        }
        total += (size_t) n;
    }
    return (ssize_t) total;
}

ssize_t send_full(int fd, const void *buf, size_t len, int flags) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = send(fd, (const char *) buf + total, len - total, flags);
        if (n <= 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        total += (size_t) n;
    }
    return (ssize_t) total;
}