#!/usr/bin/env python3
"""Run a command against a private, temporary TCP/Unix-socket Redis fixture."""
import argparse
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-server", default="redis-server")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a command is required")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    with tempfile.TemporaryDirectory(prefix="redis-adapter-test-") as temporary:
        folder = Path(temporary)
        unix_socket = folder / "redis.sock"
        with (folder / "redis.log").open("w") as log:
            redis = subprocess.Popen([args.redis_server, "--bind", "127.0.0.1", "--port", str(port),
                                      "--save", "", "--appendonly", "no", "--unixsocket", str(unix_socket),
                                      "--unixsocketperm", "700"], stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 5
                while True:
                    if redis.poll() is not None:
                        raise RuntimeError("Redis fixture exited: " + (folder / "redis.log").read_text())
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.2) as connection:
                            connection.sendall(b"*1\r\n$4\r\nPING\r\n")
                            if connection.recv(64) == b"+PONG\r\n" and unix_socket.exists():
                                break
                    except OSError:
                        pass
                    if time.monotonic() > deadline:
                        raise TimeoutError("private Redis fixture did not become ready")
                    time.sleep(0.01)
                environment = dict(os.environ, REDIS_ADAPTER_TEST_PORT=str(port),
                                   REDIS_ADAPTER_TEST_SOCKET=str(unix_socket), REDIS_ADAPTER_ISOLATED_TEST="1")
                return subprocess.run(command, env=environment).returncode
            finally:
                redis.terminate()
                try:
                    redis.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    redis.kill()
                    redis.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
