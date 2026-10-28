#!/usr/bin/env python3
"""Count write round trips through an isolated one-way-latency proxy."""
import os
import socket
import socketserver
import subprocess
import sys
import threading
import time


def expect(condition, message):
    if not condition:
        raise RuntimeError(message)


class LatencyProxy(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, upstream_port, delay_ms):
        self.upstream_port = upstream_port
        self.delay = delay_ms / 1000
        super().__init__(("127.0.0.1", 0), Delay)


class Delay(socketserver.BaseRequestHandler):
    # Every client chunk waits once before it is forwarded. A client that
    # waits for each reply pays the delay per command; a pipelined client
    # pays it once for everything it sent together.
    def handle(self):
        with socket.create_connection(("127.0.0.1", self.server.upstream_port), timeout=10) as upstream:
            replies = threading.Thread(target=self.pump, args=(upstream, self.request))
            replies.start()
            try:
                while True:
                    data = self.request.recv(65536)
                    if not data:
                        break
                    time.sleep(self.server.delay)
                    upstream.sendall(data)
            except OSError:
                pass
            finally:
                try:
                    upstream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                replies.join()

    @staticmethod
    def pump(source, destination):
        try:
            while True:
                data = source.recv(65536)
                if not data:
                    break
                destination.sendall(data)
        except OSError:
            pass


def run(mode, binary, upstream, delay):
    with LatencyProxy(upstream, delay) as proxy:
        server = threading.Thread(target=proxy.serve_forever)
        server.start()
        try:
            child = subprocess.run([binary], timeout=25,
                env=dict(os.environ, REDIS_ADAPTER_WRITE_SCENARIO=mode,
                         REDIS_ADAPTER_TEST_PORT=str(proxy.server_address[1]),
                         REDIS_ADAPTER_CONTROL_PORT=str(upstream),
                         REDIS_ADAPTER_PROXY_LATENCY_MS=str(delay)))
            expect(child.returncode == 0, f"{mode} scenario failed with {child.returncode}")
        finally:
            proxy.shutdown()
            server.join()
    print(f"{mode}: passed with {delay} ms one-way latency")


def main():
    expect(os.environ.get("REDIS_ADAPTER_ISOLATED_TEST") == "1", "private Redis fixture required")
    upstream = int(os.environ["REDIS_ADAPTER_TEST_PORT"])
    for mode in ("batch-round-trip", "pipeline-round-trip"):
        run(mode, sys.argv[1], upstream, 50)


if __name__ == "__main__":
    main()
