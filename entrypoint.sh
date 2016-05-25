#!/bin/bash
set -e

: ${LISTEN_HOST:=0.0.0.0}
: ${LISTEN_PEER_PORT:=40000}
: ${LISTEN_HTTP_PORT:=4321}
: ${ETCD_HOST:=127.0.0.1}
: ${ETCD_PORT:=2379}
: ${DEBUG:=0}
: ${STORAGE_SIZE:=2GiB}
: ${AUTO_JOIN:=0}
: ${DEBUG_INIT:=0}
: ${DROP_MOUNT_BIN:=0}
: ${LOG_FLAGS:=""}

AGRO_FLAGS=""
if [ ${DEBUG} -eq "1" ]; then
  AGRO_FLAGS="$AGRO_FLAGS --debug"
fi

if [ ${AUTO_JOIN} -eq "1" ]; then
  AGRO_FLAGS="$AGRO_FLAGS --auto-join"
fi

if [ ${DEBUG_INIT} -eq "1" ]; then
  AGRO_FLAGS="$AGRO_FLAGS --debug-init"
fi

if [ ${DROP_MOUNT_BIN} -eq "1" ]; then
  mkdir -p /plugin/coreos.com~agro
  cp `which agroblock` /plugin/coreos.com~agro/agro
fi

if [ ${LOG_FLAGS} != "" ]; then
  AGRO_FLAGS="$AGRO_FLAGS --logpkg=${LOG_FLAGS}"
fi

agro --etcd $ETCD_HOST:$ETCD_PORT --host $LISTEN_HOST --port $LISTEN_HTTP_PORT --datadir /data --peer-address http://$LISTEN_HOST:$LISTEN_PEER_PORT --size $STORAGE_SIZE $AGRO_FLAGS
