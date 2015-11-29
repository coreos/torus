#!/bin/sh

GOGOPROTO_ROOT="${GOPATH}/src/github.com/gogo/protobuf"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"
protoc --gogofaster_out=. -I=.:"${GOGOPROTO_PATH}" agro.proto
