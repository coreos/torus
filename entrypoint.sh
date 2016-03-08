#!/bin/bash
set -e

: ${LISTEN_HOST:=0.0.0.0}
: ${LISTEN_PEER_PORT:=40000}
: ${LISTEN_HTTP_PORT:=4321}
: ${ETCD_HOST:=127.0.0.1}
: ${ETCD_PORT:=2378}
: ${DEBUG:=0}
: ${STORAGE_SIZE:=2GiB}

AGRO_FLAGS=""
if [ ${DEBUG} -eq "1" ]; then
  AGRO_FLAGS="$AGRO_FLAGS --debug"
fi

agro --etcd $ETCD_HOST:$ETCD_PORT --host $LISTEN_HOST --port $LISTEN_HTTP_PORT --datadir /data --peer-address $LISTEN_HOST:$LISTEN_PEER_PORT --size $STORAGE_SIZE $AGRO_FLAGS
