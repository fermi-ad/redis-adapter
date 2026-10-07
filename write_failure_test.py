#!/usr/bin/env python3
"""Exercise write faults through an isolated proxy and explicit verdicts."""
import collections
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

    def __init__(self, upstream_port, mode, delay_ms):
        self.upstream_port, self.mode = upstream_port, mode
        self.delay = delay_ms / 1000
        self.lock = threading.Lock()
        self.attempts = collections.Counter()
        self.commands = collections.Counter()
        self.connections, self.ping_probes, self.dropped = 0, 0, 0
        self.ping_connections = set()
        self.cluster_generations = []
        self.failures = []
        super().__init__(("127.0.0.1", 0), Forward)


class Forward(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            self.request.settimeout(10)
            with self.server.lock:
                self.server.connections += 1
                connection = self.server.connections
            generation = None
            with socket.create_connection(("127.0.0.1", self.server.upstream_port), timeout=10) as upstream:
                with self.request.makefile("rb") as client, upstream.makefile("rb") as reply:
                    while True:
                        request, args = frame(client)
                        drop, readonly = False, False
                        command = args[0].upper()
                        with self.server.lock:
                            self.server.commands[command] += 1
                            if command == b"PING":
                                self.server.ping_probes += 1
                                self.server.ping_connections.add(connection)
                                if generation is None:
                                    generation = self.server.ping_probes
                            if command == b"CLUSTER":
                                expect(args == [b"CLUSTER", b"INFO"], "unexpected Cluster discovery command")
                                self.server.cluster_generations.append(generation)
                            if command in (b"XADD", b"XTRIM"):
                                key = args[1].split(b":", 1)[1]
                                self.server.attempts[(command, key)] += 1
                                count = self.server.attempts[(command, key)]
                                drop = ((self.server.mode == "fault" and command == b"XADD" and
                                         ((key == b"lost-reply" and count == 1) or
                                          (key == b"mixed-transport" and count == 2))) or
                                        (self.server.mode == "trim" and command == b"XTRIM" and key == b"trim-lost"))
                                readonly = (self.server.mode == "readonly" and command == b"XADD" and
                                            key == b"readonly" and (generation is None or generation < 2))
                        if self.server.delay:
                            time.sleep(self.server.delay)
                        if self.server.mode == "cluster-refusal" and command == b"CLUSTER":
                            info = b"cluster_state:ok\r\n"
                            self.request.sendall(b"$" + str(len(info)).encode() + b"\r\n" + info + b"\r\n")
                            continue
                        if readonly:
                            self.request.sendall(b"-READONLY You can't write against a read only replica.\r\n")
                            continue
                        upstream.sendall(request)
                        response, _ = frame(reply)
                        if drop:
                            expect(response.startswith(b"$" if command == b"XADD" else b":"), "fault must consume an accepted Redis response")
                            with self.server.lock:
                                self.server.dropped += 1
                            return
                        self.request.sendall(response)
        except (EOFError, ConnectionError, TimeoutError, OSError):
            pass
        except Exception as error:
            with self.server.lock:
                self.server.failures.append(str(error))


def foreign_pings(port, stopped):
    while not stopped.is_set():
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1) as client:
                client.sendall(b"*1\r\n$4\r\nPING\r\n")
                client.recv(64)
        except OSError:
            pass
        stopped.wait(0.03)


def run(mode, binary, upstream, delay):
    with Proxy(upstream, mode, delay) as proxy:
        server = threading.Thread(target=proxy.serve_forever)
        server.start()
        stopped = threading.Event()
        foreign = threading.Thread(target=foreign_pings, args=(upstream, stopped))
        foreign.start()
        try:
            child = subprocess.run([binary], timeout=25,
                env=dict(os.environ, REDIS_ADAPTER_WRITE_SCENARIO=mode,
                         REDIS_ADAPTER_TEST_PORT=str(proxy.server_address[1]),
                         REDIS_ADAPTER_CONTROL_PORT=str(upstream)))
            expect(child.returncode == 0, f"{mode} scenario failed with {child.returncode}")
            # The child destructor joins the reconnect thread: probe counts are
            # final here, without sleeps or unrelated server-wide statistics.
            attempts = proxy.attempts
            expect(not proxy.failures, str(proxy.failures))
            expect(proxy.ping_probes == len(proxy.ping_connections), "connection generation was probed more than once")
            # Constructors classify the server once; reconnect generations must
            # keep the cached result even after ambiguous faults or READONLY.
            expected_classifications = {"constructor": [1], "cluster-refusal": [1], "fault": [1, 3, 5],
                                        "trim": [1, 3], "rejection": [1, 2], "readonly": [1]}
            expect(proxy.cluster_generations == expected_classifications[mode],
                   f"server mode was probed on unexpected generations: {proxy.cluster_generations}")
            if mode == "constructor":
                expect(proxy.connections == 1, "constructor opened an unnecessary connection")
                expect(proxy.ping_probes == 1, "constructor did not use one standalone PING")
                expect(attempts[(b"XADD", b"healthy")] == 1, "constructor connection could not write")
            elif mode == "cluster-refusal":
                expect(proxy.ping_probes == 2, "cached Cluster refusal was not checked across reconnect")
                expect(proxy.commands[b"XADD"] == 0, "unsupported Cluster connection published a writer")
            elif mode == "fault":
                expect(proxy.dropped == 2, f"expected 2 ambiguous faults, got {proxy.dropped}")
                expect(attempts[(b"XADD", b"lost-reply")] == 1, "lost reply was replayed")
                expect(attempts[(b"XADD", b"mixed-transport")] == 2, "batch continued after unavailable item or replayed it")
                expect(attempts[(b"XADD", b"healthy")] == 2, "future writes failed")
                expect(proxy.ping_probes == 5, f"mixed failure did not refresh: {proxy.ping_probes} PING probes")
            elif mode == "trim":
                expect(proxy.dropped == 1, "final trim was not exercised")
                expect(attempts[(b"XTRIM", b"trim-lost")] == 1, "trim was omitted or replayed")
                expect(proxy.ping_probes == 3, "trim transport failure did not refresh")
            elif mode == "rejection":
                expect(attempts[(b"XTRIM", b"trim-denied")] == 1, "rejected final trim was not exercised")
                expect(proxy.ping_probes == 2, f"known rejection triggered reconnect: {proxy.ping_probes} PING probes")
            else:
                expect(proxy.ping_probes >= 2, "READONLY did not refresh later connections")
        finally:
            stopped.set()
            foreign.join()
            proxy.shutdown()
            server.join()
    print(f"{mode}: own proxy counters passed with foreign health traffic and {delay} ms reply delay")


def main():
    expect(os.environ.get("REDIS_ADAPTER_ISOLATED_TEST") == "1", "private Redis fixture required")
    upstream = int(os.environ["REDIS_ADAPTER_TEST_PORT"])
    delay = int(os.environ.get("REDIS_ADAPTER_PROXY_DELAY_MS", "0"))
    for mode in ("constructor", "cluster-refusal", "fault", "trim", "rejection", "readonly"):
        run(mode, sys.argv[1], upstream, delay)


if __name__ == "__main__":
    main()
