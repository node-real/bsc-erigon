# syntax = docker/dockerfile:1.2
FROM docker.io/library/golang:1.20-alpine3.17 AS builder

RUN apk --no-cache add build-base linux-headers git bash ca-certificates libstdc++

WORKDIR /app
ADD go.mod go.mod
ADD go.sum go.sum

RUN go mod download
ADD . .

RUN --mount=type=cache,target=/root/.cache \
    --mount=type=cache,target=/tmp/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    make all


FROM docker.io/library/golang:1.20-alpine3.17 AS tools-builder
RUN apk --no-cache add build-base linux-headers git bash ca-certificates libstdc++
WORKDIR /app

ADD Makefile Makefile
ADD tools.go tools.go
ADD go.mod go.mod
ADD go.sum go.sum

RUN mkdir -p /app/build/bin

RUN --mount=type=cache,target=/root/.cache \
    --mount=type=cache,target=/tmp/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    make db-tools

FROM docker.io/library/alpine:3.17

# install required runtime libs, along with some helpers for debugging
RUN apk add --no-cache ca-certificates libstdc++ tzdata
RUN apk add --no-cache curl jq bind-tools

# Setup user and group
#
# from the perspective of the container, uid=1000, gid=1000 is a sensible choice
# (mimicking Ubuntu Server), but if caller creates a .env (example in repo root),
# these defaults will get overridden when make calls docker-compose
ARG UID=1000
ARG GID=1000
RUN adduser -D -u $UID -g $GID erigon
USER erigon
RUN mkdir -p ~/.local/share/erigon

# copy compiled artifacts from builder
## first do the mdbx ones - since these wont change as often
COPY --from=tools-builder /app/build/bin/mdbx_chk /usr/local/bin/mdbx_chk
COPY --from=tools-builder /app/build/bin/mdbx_copy /usr/local/bin/mdbx_copy
COPY --from=tools-builder /app/build/bin/mdbx_drop /usr/local/bin/mdbx_drop
COPY --from=tools-builder /app/build/bin/mdbx_dump /usr/local/bin/mdbx_dump
COPY --from=tools-builder /app/build/bin/mdbx_load /usr/local/bin/mdbx_load
COPY --from=tools-builder /app/build/bin/mdbx_stat /usr/local/bin/mdbx_stat

## then give each binary its own layer
COPY --from=builder /app/build/bin/devnet /usr/local/bin/devnet
COPY --from=builder /app/build/bin/downloader /usr/local/bin/downloader
COPY --from=builder /app/build/bin/erigon /usr/local/bin/erigon
COPY --from=builder /app/build/bin/erigon-cl /usr/local/bin/erigon-cl
COPY --from=builder /app/build/bin/evm /usr/local/bin/evm
COPY --from=builder /app/build/bin/hack /usr/local/bin/hack
COPY --from=builder /app/build/bin/integration /usr/local/bin/integration
COPY --from=builder /app/build/bin/observer /usr/local/bin/observer
COPY --from=builder /app/build/bin/pics /usr/local/bin/pics
COPY --from=builder /app/build/bin/rpcdaemon /usr/local/bin/rpcdaemon
COPY --from=builder /app/build/bin/rpctest /usr/local/bin/rpctest
COPY --from=builder /app/build/bin/sentinel /usr/local/bin/sentinel
COPY --from=builder /app/build/bin/sentry /usr/local/bin/sentry
COPY --from=builder /app/build/bin/state /usr/local/bin/state
COPY --from=builder /app/build/bin/txpool /usr/local/bin/txpool
COPY --from=builder /app/build/bin/verkle /usr/local/bin/verkle
COPY --from=builder /app/build/bin/caplin-phase1 /usr/local/bin/caplin-phase1



EXPOSE 8545 \
       8551 \
       8546 \
       30303 \
       30303/udp \
       42069 \
       42069/udp \
       8080 \
       9090 \
       6060

#ENTRYPOINT ["erigon", "--config", "/server/config/config.toml", "--txpool.disable", "--pprof"]
#ENTRYPOINT ["erigon", "--pprof", "--torrent.download.rate=256mb", "--metrics", "--metrics.addr=0.0.0.0", "--bootnodes=enode://0637d1e62026e0c8685b1db0ca1c767c78c95c3fab64abc468d1a64b12ca4b530b46b8f80c915aec96f74f7ffc5999e8ad6d1484476f420f0c10e3d42361914b@52.199.214.252:30311,enode://330d768f6de90e7825f0ea6fe59611ce9d50712e73547306846a9304663f9912bf1611037f7f90f21606242ded7fb476c7285cb7cd792836b8c0c5ef0365855c@18.181.52.189:30311,enode://df1e8eb59e42cad3c4551b2a53e31a7e55a2fdde1287babd1e94b0836550b489ba16c40932e4dacb16cba346bd442c432265a299c4aca63ee7bb0f832b9f45eb@52.51.80.128:30311,enode://0bd566a7fd136ecd19414a601bfdc530d5de161e3014033951dd603e72b1a8959eb5b70b06c87a5a75cbf45e4055c387d2a842bd6b1bd8b5041b3a61bab615cf@34.242.33.165:30311,enode://ecd664250ca19b1074dcfbfb48576a487cc18d052064222a363adacd2650f8e08fb3db9de7a7aecb48afa410eaeb3285e92e516ead01fb62598553aed91ee15e@3.209.122.123:30311,enode://665cf77ca26a8421cfe61a52ac312958308d4912e78ce8e0f61d6902e4494d4cc38f9b0dd1b23a427a7a5734e27e5d9729231426b06bb9c73b56a142f83f6b68@52.72.123.113:30311"]
ENTRYPOINT ["erigon", "--pprof", "--torrent.download.rate=256mb", "--metrics", "--metrics.addr=0.0.0.0"]
