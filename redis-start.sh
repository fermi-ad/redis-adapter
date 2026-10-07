#!/usr/bin/env bash
set -euo pipefail

# This legacy fixture uses fixed endpoints; refuse to borrow an existing server.
python3 - <<'PY'
from pathlib import Path
import socket
import sys

endpoint = Path('/tmp/redis.sock')
if endpoint.exists() or endpoint.is_symlink():
    sys.exit('Refusing to start: /tmp/redis.sock already exists')
try:
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 6379))
except OSError as error:
    sys.exit('Refusing to start: loopback port 6379 is occupied: ' + str(error))
PY

redis-server --bind 127.0.0.1 --port 6379 --appendonly no --save '' \
  --daemonize yes --unixsocket /tmp/redis.sock --unixsocketperm 700
redis-cli -h 127.0.0.1 -p 6379 ping | grep -qx PONG
printf 'redis-server started on 127.0.0.1:6379 and /tmp/redis.sock\n'
