#!/usr/bin/env bash

set -euo pipefail

BIND_ADDRESS="127.0.0.1"
APPENDONLY="no"
SAVE=
DAEMONIZE="yes"
UNIXSOCKET="/tmp/redis.sock"
UNIXSOCKETPERM="700"
redis-server \
  --bind "$BIND_ADDRESS" \
  --appendonly "$APPENDONLY" \
  --save "$SAVE" \
  --daemonize "$DAEMONIZE" \
  --unixsocket "$UNIXSOCKET" \
  --unixsocketperm "$UNIXSOCKETPERM"

echo "redis-server started on ${BIND_ADDRESS}:6379 and ${UNIXSOCKET}"
