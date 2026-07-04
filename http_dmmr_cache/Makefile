CC = gcc
CFLAGS = -Wall -O2
LDFLAGS = -ldb -lpthread

all: http_dmmr_cache

http_dmmr_cache: http_dmmr_cache.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f http_dmmr_cache apikeys.db
