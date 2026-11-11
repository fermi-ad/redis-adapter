#!/usr/bin/env python3
"""Inject faults between a watchdog's write and its expiration through a proxy.

A watchdog field must never be left without an expiration: whatever fails between
setting the field and setting its expiration, once the process is gone the field
must disappear within its 1 s expiration. For each scenario the child keeps an
automatic watchdog alive through the proxy for a few seconds, is SIGKILLed, and the
field must be gone 1.5 s later.

  cut-before-expiry   the connection is cut after the field is sent but before its
                      expiration takes effect (EXEC of the transaction, or a bare
                      HEXPIRE following an HSET)
  unknown-hexpire     HEXPIRE is answered with "unknown command", as Redis < 7.4 does
  expire-fails        inside the transaction, HEXPIRE is replaced by a command that
                      fails when EXEC runs, so the field is set but its expiration isn't
  killed              no fault: the field is alive with an expiration, then disappears
"""
import os
import signal
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
    if prefix in (b"$",):
        size = int(value)
        payload = reader.read(size + 2) if size >= 0 else b""
        return line + payload, payload[:-2] if size >= 0 else None
    if prefix in (b"*",):
        raw, items = bytearray(line), []
        for _ in range(max(0, int(value))):
            encoded, decoded = frame(reader)
            raw.extend(encoded)
            items.append(decoded)
        return bytes(raw), items
    return line, value


def encode(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


class Redis:
    """Minimal direct client for checking state, bypassing the proxy."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.reader = self.sock.makefile("rb")

    def __call__(self, *args):
        self.sock.sendall(encode(*args))
        _, value = frame(self.reader)
        return value


class Proxy(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, upstream_port, mode):
        self.upstream_port, self.mode = upstream_port, mode
        self.lock = threading.Lock()
        self.faults, self.failures = 0, []
        super().__init__(("127.0.0.1", 0), Forward)


class Forward(socketserver.BaseRequestHandler):
    def handle(self):
        mode = self.server.mode
        try:
            self.request.settimeout(10)
            with socket.create_connection(("127.0.0.1", self.server.upstream_port), timeout=10) as upstream:
                with self.request.makefile("rb") as client, upstream.makefile("rb") as reply:
                    in_multi, previous = False, b""
                    while True:
                        request, args = frame(client)
                        command = args[0].upper()
                        fault = None
                        if command == b"MULTI":
                            in_multi = True
                        if mode == "cut-before-expiry" and (
                                (command == b"EXEC" and in_multi) or
                                (command == b"HEXPIRE" and not in_multi and previous == b"HSET")):
                            fault = "cut"
                        elif mode == "unknown-hexpire" and command == b"HEXPIRE":
                            fault = "unknown"
                        elif mode == "expire-fails" and command == b"HEXPIRE" and in_multi:
                            fault = "rewrite"
                        if command in (b"EXEC", b"DISCARD"):
                            in_multi = False
                        previous = command

                        if fault:
                            with self.server.lock:
                                self.server.faults += 1
                        if fault == "cut":
                            return                       # closes both connections
                        if fault == "unknown":
                            self.request.sendall(b"-ERR unknown command 'hexpire', with args beginning with: \r\n")
                            continue
                        if fault == "rewrite":
                            # INCR on a hash queues fine and fails with WRONGTYPE when EXEC runs
                            request = encode(b"INCR", args[1])
                        upstream.sendall(request)
                        response, _ = frame(reply)
                        self.request.sendall(response)
        except (EOFError, ConnectionError, TimeoutError, OSError):
            pass
        except Exception as error:
            with self.server.lock:
                self.server.failures.append(repr(error))


def run(mode, binary, upstream):
    base = f"watchdog-fault-{mode}-{os.getpid()}"
    key = "{%s}:watchdog" % base
    direct = Redis(upstream)
    direct("DEL", key)
    with Proxy(upstream, mode) as proxy:
        server = threading.Thread(target=proxy.serve_forever, daemon=True)
        server.start()
        child = subprocess.Popen([binary], env=dict(
            os.environ, REDIS_ADAPTER_TEST_PORT=str(proxy.server_address[1]), REDIS_ADAPTER_WATCHDOG_BASE=base))
        try:
            samples = []
            end = time.monotonic() + 3.0
            while time.monotonic() < end:
                ttl = direct("HPTTL", key, "FIELDS", "1", "dog")
                samples.append(int(ttl[0]) if isinstance(ttl, list) and ttl else None)
                expect(child.poll() is None, f"{mode}: child exited early with {child.returncode}")
                time.sleep(0.05)
            # Never alive without an expiration (-1), whatever was injected.
            expect(-1 not in samples, f"{mode}: watchdog field had no expiration: {samples}")
            if mode == "killed":
                expect(sum(1 for s in samples if s is not None and s > 0) > len(samples) // 2,
                       f"{mode}: watchdog was not kept alive: {samples}")
            else:
                expect(proxy.faults > 0, f"{mode}: no fault was injected")
            child.send_signal(signal.SIGKILL)
            child.wait()
            time.sleep(1.5)
            exists = int(direct("HEXISTS", key, "dog"))
            expect(exists == 0, f"{mode}: watchdog field outlived the process")
            expect(not proxy.failures, f"{mode}: proxy errors {proxy.failures}")
        finally:
            if child.poll() is None:
                child.send_signal(signal.SIGKILL)
                child.wait()
            proxy.shutdown()
            server.join()
            direct("DEL", key)
    print(f"{mode}: no field left without an expiration ({proxy.faults} faults injected)")


def main():
    expect(os.environ.get("REDIS_ADAPTER_ISOLATED_TEST") == "1", "private Redis fixture required")
    upstream = int(os.environ["REDIS_ADAPTER_TEST_PORT"])
    for mode in ("killed", "cut-before-expiry", "unknown-hexpire", "expire-fails"):
        run(mode, sys.argv[1], upstream)


if __name__ == "__main__":
    main()
