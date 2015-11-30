#!/usr/bin/env bash

curl https://raw.githubusercontent.com/coreos/etcd/master/etcdserver/etcdserverpb/rpc.proto > etcdserverpb/rpc.proto
curl https://raw.githubusercontent.com/coreos/etcd/master/storage/storagepb/kv.proto > storagepb/kv.proto

sed -i 's!etcd/storage/storagepb/kv.proto!storagepb/kv.proto!' etcdserverpb/rpc.proto

GOGOPROTO_ROOT="${GOPATH}/src/github.com/gogo/protobuf"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"
protoc --gogofaster_out=plugins=grpc:. -I=.:"${GOGOPROTO_PATH}" storagepb/kv.proto
protoc --gogofaster_out=plugins=grpc:. -I=.:"${GOGOPROTO_PATH}" etcdserverpb/rpc.proto

sed -i 's!"storagepb"!"github.com/barakmich/agro/internal/etcdproto/storagepb"!' etcdserverpb/rpc.pb.go
