#!/bin/bash
set -e

: ${LISTEN_HOST:=0.0.0.0}
: ${ETCD_HOST:=127.0.0.1}
: ${ETCD_PORT:=2378}
: ${DEBUG:=0}
: ${STORAGE_SIZE:=2GiB}

AGRO_FLAGS=""
if [ ${DEBUG} -eq "1" ]; then
  AGRO_FLAGS="$AGRO_FLAGS --debug"
fi

agro --etcd $ETCD_HOST:$ETCD_PORT --host $LISTEN_HOST --port 4321 --datadir /data --peer-address $LISTEN_HOST:40000 --size $STORAGE_SIZE $AGRO_FLAGS
