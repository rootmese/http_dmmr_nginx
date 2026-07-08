TARGET = dmmr_cache
OBJS = dmmr_net.o dmmr_db.o dmmr_cluster.o dmmr_pool.o dmmr_server.o

CFLAGS_COMMON = -Wall -Wextra -pthread
LDFLAGS = -ldb -lpthread

# Configurações
CFLAGS_RELEASE = $(CFLAGS_COMMON) -O2 -DNDEBUG
CFLAGS_DEBUG  = $(CFLAGS_COMMON) -O0 -g3 -DDEBUG

# Release é o padrão
CFLAGS = $(CFLAGS_RELEASE)

all: release

release: CFLAGS=$(CFLAGS_RELEASE)
release: clean $(TARGET)

debug: CFLAGS=$(CFLAGS_DEBUG)
debug: clean $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all release debug clean