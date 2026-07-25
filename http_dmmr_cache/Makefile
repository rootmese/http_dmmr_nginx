TARGET = dmmr_cache
OBJS = dmmr_net.o dmmr_db.o dmmr_cluster.o dmmr_pool.o dmmr_string.o dmmr_server.o

# Prefer the libc implementation when it is declared and linkable.  Otherwise
# dmmr_string.c supplies the OpenBSD-compatible fallback.
HAVE_STRLCPY = $(shell printf '%s\n' '#include <string.h>' 'int main(void) { char dst[2]; return strlcpy(dst, "x", sizeof(dst)) != 1; }' | $(CC) $(CPPFLAGS) $(CFLAGS) -Werror=implicit-function-declaration -x c -o /dev/null - >/dev/null 2>&1 && echo 1)
ifeq ($(HAVE_STRLCPY),1)
CPPFLAGS += -DDMMR_HAVE_STRLCPY=1
endif

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
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all release debug clean
