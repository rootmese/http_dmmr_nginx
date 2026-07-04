#include <microhttpd.h>
#include <db.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <signal.h>
#include <arpa/inet.h>

#define DMMR_PROTO_OP_GET 1
#define DMMR_PROTO_STATUS_OK 0
#define DMMR_PROTO_STATUS_NOT_FOUND 1
#define DMMR_PROTO_STATUS_ERROR 2

#define PORT 9080
#define DB_PATH "./apikeys.db"
#define SOCK_PATH "/tmp/dmmr_cache.sock"

DB *dbp = NULL;

/* Inicializa/abre o ambiente Berkeley DB */
int init_db() {
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

void close_db() {
    if (dbp) {
        dbp->close(dbp, 0);
    }
}

static int
handle_binary_request(int client_fd)
{
    unsigned char req_buf[1024];
    unsigned char resp_buf[4096];
    uint16_t opcode;
    uint16_t key_len;
    uint16_t status;
    uint16_t payload_len;
    char *key;
    DBT key_dbt, data_dbt;
    ssize_t n;
    int rc;

    n = recv(client_fd, req_buf, sizeof(req_buf), 0);
    if (n < 4) {
        return -1;
    }

    opcode = ntohs(*(uint16_t *) req_buf);
    key_len = ntohs(*(uint16_t *) (req_buf + sizeof(uint16_t)));

    if (opcode != DMMR_PROTO_OP_GET || key_len == 0 || (size_t) key_len > sizeof(req_buf) - 4) {
        status = DMMR_PROTO_STATUS_ERROR;
        payload_len = 0;
        goto respond;
    }

    key = (char *) (req_buf + 4);
    key[key_len] = '\0';

    memset(&key_dbt, 0, sizeof(key_dbt));
    memset(&data_dbt, 0, sizeof(data_dbt));
    key_dbt.data = key;
    key_dbt.size = key_len;

    rc = dbp->get(dbp, NULL, &key_dbt, &data_dbt, 0);
    if (rc == 0) {
        status = DMMR_PROTO_STATUS_OK;
        payload_len = (uint16_t) data_dbt.size;
        if (payload_len > sizeof(resp_buf) - 4) {
            payload_len = (uint16_t) (sizeof(resp_buf) - 4);
        }
        memcpy(resp_buf + 4, data_dbt.data, payload_len);
    } else if (rc == DB_NOTFOUND) {
        status = DMMR_PROTO_STATUS_NOT_FOUND;
        payload_len = 0;
    } else {
        status = DMMR_PROTO_STATUS_ERROR;
        payload_len = 0;
    }

respond:
    status = htons(status);
    payload_len = htons(payload_len);
    memcpy(resp_buf, &status, sizeof(status));
    memcpy(resp_buf + sizeof(status), &payload_len, sizeof(payload_len));

    send(client_fd, resp_buf, 4 + payload_len, 0);
    return 0;
}

static int
accept_connection(int listen_fd)
{
    int client_fd;
    struct sockaddr_storage addr;
    socklen_t addrlen = sizeof(addr);

    client_fd = accept(listen_fd, (struct sockaddr *) &addr, &addrlen);
    if (client_fd < 0) {
        return -1;
    }

    handle_binary_request(client_fd);
    close(client_fd);
    return 0;
}

int main(int argc, char *argv[]) {
    struct MHD_Daemon *daemon_unix = NULL;
    struct MHD_Daemon *daemon_tcp = NULL;
    int use_unix = 0, use_tcp = 0;
    int fd = -1;
    struct sockaddr_un addr;

    /* Processa argumentos */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--unix") == 0) {
            use_unix = 1;
        } else if (strcmp(argv[i], "--tcp") == 0) {
            use_tcp = 1;
        } else if (strcmp(argv[i], "--both") == 0) {
            use_unix = 1;
            use_tcp = 1;
        } else if (strcmp(argv[i], "--help") == 0) {
            printf("Uso: %s [--unix] [--tcp] [--both]\n", argv[0]);
            printf("Padrão: apenas UNIX socket\n");
            return 0;
        }
    }

    /* Padrão: Unix se nada for escolhido */
    if (!use_unix && !use_tcp) {
        use_unix = 1;
    }

    if (init_db() != 0) {
        return 1;
    }

    /* Inicia Unix socket se pedido */
    if (use_unix) {
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            perror("socket");
            close_db();
            return 1;
        }

        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);
        unlink(SOCK_PATH);

        if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            perror("bind");
            close(fd);
            close_db();
            return 1;
        }

        if (listen(fd, SOMAXCONN) < 0) {
            perror("listen");
            close(fd);
            unlink(SOCK_PATH);
            close_db();
            return 1;
        }

        while (1) {
            accept_connection(fd);
        }
    }

    /* Inicia TCP se pedido */
    if (use_tcp) {
        int tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in tcp_addr;
        memset(&tcp_addr, 0, sizeof(tcp_addr));
        tcp_addr.sin_family = AF_INET;
        tcp_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        tcp_addr.sin_port = htons(PORT);

        if (bind(tcp_fd, (struct sockaddr *)&tcp_addr, sizeof(tcp_addr)) < 0) {
            perror("bind tcp");
            close_db();
            return 1;
        }

        if (listen(tcp_fd, SOMAXCONN) < 0) {
            perror("listen tcp");
            close(tcp_fd);
            close_db();
            return 1;
        }

        printf("Escutando em TCP: porta %d\n", PORT);
        while (1) {
            accept_connection(tcp_fd);
        }
    }

    close_db();
    return 0;
}