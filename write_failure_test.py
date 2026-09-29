#!/usr/bin/env python3
"""Drop selected replies from the test Redis to exercise ambiguous acceptance."""
import collections
import os
import socket
import socketserver
import subprocess
import sys
import threading


def frame(reader):
    line = reader.readline()
    if not line:
        raise EOFError
    prefix, value = line[:1], line[1:-2]
    if prefix == b"$":
        size = int(value)
        payload = reader.read(size + 2) if size >= 0 else b""
        return line + payload, payload[:-2] if size >= 0 else None
    if prefix == b"*":
        raw, items = bytearray(line), []
        for _ in range(max(0, int(value))):
            encoded, decoded = frame(reader)
            raw.extend(encoded)
            items.append(decoded)
        return bytes(raw), items
    return line, value


class Proxy(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, upstream_port):
        self.upstream_port = upstream_port
        self.lock = threading.Lock()
        self.attempts = collections.Counter()
        self.cluster_probes = 0
        self.dropped = 0
        super().__init__(("127.0.0.1", 0), Forward)


class Forward(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            self.request.settimeout(5)
            with socket.create_connection(("127.0.0.1", self.server.upstream_port), timeout=5) as upstream:
                with self.request.makefile("rb") as client, upstream.makefile("rb") as reply:
                    while True:
                        request, args = frame(client)
                        drop = False
                        with self.server.lock:
                            if args[0].upper() == b"CLUSTER":
                                self.server.cluster_probes += 1
                            if args[0].upper() == b"XADD":
                                key = args[1].split(b":", 1)[1]
                                self.server.attempts[key] += 1
                                drop = ((key == b"lost-reply" and self.server.attempts[key] == 1)
                                        or (key == b"mixed-transport" and self.server.attempts[key] == 2))
                        upstream.sendall(request)
                        response, _ = frame(reply)
                        if drop:
                            # Consume the real successful Redis response before closing.
                            assert response.startswith(b"$")
                            with self.server.lock:
                                self.server.dropped += 1
                            return
                        self.request.sendall(response)
        except (EOFError, ConnectionError, TimeoutError, OSError):
            pass


def main():
    with Proxy(int(os.environ.get("REDIS_ADAPTER_TEST_PORT", "6379"))) as proxy:
        thread = threading.Thread(target=proxy.serve_forever)
        thread.start()
        try:
            subprocess.run([sys.argv[1]], check=True, timeout=20,
                           env=dict(os.environ, REDIS_ADAPTER_FAULT_TEST="1",
                                    REDIS_ADAPTER_TEST_PORT=str(proxy.server_address[1])))
            assert proxy.dropped == 2, proxy.dropped
            assert proxy.attempts[b"lost-reply"] == 1, proxy.attempts
            assert proxy.attempts[b"mixed-transport"] == 3, proxy.attempts
            assert proxy.attempts[b"healthy"] == 2, proxy.attempts
            # Constructor plus recovery from each genuinely broken connection.
            # An accepted item must not hide another item's transport failure.
            assert proxy.cluster_probes == 3, proxy.cluster_probes
        finally:
            proxy.shutdown()
            thread.join()
    print("real Redis accepted both lost-reply writes; no command was replayed")


if __name__ == "__main__":
    main()
