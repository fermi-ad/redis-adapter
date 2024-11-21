#!/bin/bash

# Default values for Redis Server
BIND_ADDRESS="127.0.0.1"
APPENDONLY="no"
SAVE=""
DAEMONIZE="yes"
HOSTNAME=$(hostname -s)
UNIXSOCKET="/tmp/redis${HOSTNAME}.sock"
UNIXSOCKETPERM="700"

# Define the path to the Redis configuration file
# Default value is empty
REDIS_CONFIG=

# Start the Redis server with the specified command
redis-server --bind $BIND_ADDRESS --appendonly $APPENDONLY --save "$SAVE" --daemonize $DAEMONIZE --unixsocket $UNIXSOCKET --unixsocketperm $UNIXSOCKETPERM $REDIS_CONFIG

echo "Redis server started"


