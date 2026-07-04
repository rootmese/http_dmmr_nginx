CC = gcc
CFLAGS = -Wall -Wextra -O2 -pthread
LDFLAGS = -ldb -lpthread

TARGET = dmmr_cache
OBJS = dmmr_net.o dmmr_db.o dmmr_cluster.o dmmr_server.o

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean