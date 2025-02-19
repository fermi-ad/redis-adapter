#!/bin/bash

BIND_ADDRESS="127.0.0.1"
APPENDONLY="no"
SAVE=
DAEMONIZE="yes"
UNIXSOCKET="/tmp/redis.sock"
UNIXSOCKETPERM="700"
REDIS_CONFIG=

redis-server --bind $BIND_ADDRESS --appendonly $APPENDONLY --save "$SAVE" --daemonize $DAEMONIZE --unixsocket $UNIXSOCKET --unixsocketperm $UNIXSOCKETPERM $REDIS_CONFIG

if [ $? -eq 0 ]; then
  echo "redis-server started"
else
  echo "redis-server failed to start"
fi
