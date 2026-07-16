FROM debian:bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libdb-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN make release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends libdb5.3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home dmmr \
    && mkdir /data /run/dmmr \
    && chown dmmr:dmmr /data /run/dmmr

COPY --from=build /src/dmmr_cache /usr/local/bin/dmmr_cache

USER dmmr
WORKDIR /data

ENV DMMR_DB_PATH=/data/apikeys.db \
    DMMR_BIND_ADDRESS=0.0.0.0 \
    DMMR_CACHE_PORT=9080 \
    DMMR_SOCKET_PATH=/run/dmmr/dmmr_cache.sock \
    DMMR_SOCKET_MODE=0660

VOLUME ["/data"]
EXPOSE 9080
ENTRYPOINT ["/usr/local/bin/dmmr_cache"]
CMD ["--tcp"]
