#!/usr/bin/env python3
"""Run a command against three private loopback Redis Cluster nodes on Linux."""
import argparse
import os
from pathlib import Path
import random
import socket
import subprocess
import tempfile
import time
import uuid


def run(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--image', default='redis@sha256:02419de7eddf55aa5bcf49efb74e88fa8d931b4d77c07eff8a6b2144472b6952')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('a command is required')
    ports = []
    for _ in range(3):
        for _attempt in range(100):
            candidate = random.randint(20000, 30000)
            if candidate in ports:
                continue
            try:
                with socket.socket() as client, socket.socket() as bus:
                    client.bind(('127.0.0.1', candidate))
                    bus.bind(('127.0.0.1', candidate + 10000))
                ports.append(candidate)
                break
            except OSError:
                continue
        else:
            raise RuntimeError('no free loopback Cluster ports')
    with tempfile.TemporaryDirectory(prefix='redis-adapter-cluster-') as temporary:
        containers = []
        identity = uuid.uuid4().hex[:12]
        try:
            for index, port in enumerate(ports):
                name = f'redis-adapter-cluster-{identity}-{index}'
                folder = Path(temporary) / str(index)
                folder.mkdir(mode=0o700)
                run('docker', 'run', '--detach', '--rm', '--name', name,
                    '--user', f'{os.getuid()}:{os.getgid()}', '--network', 'host',
                    '--mount', f'type=bind,source={folder},target=/data', args.image,
                    'redis-server', '--bind', '127.0.0.1', '--port', str(port),
                    '--save', '', '--appendonly', 'no', '--cluster-enabled', 'yes',
                    '--cluster-config-file', '/data/nodes.conf', '--cluster-node-timeout', '2000',
                    '--cluster-announce-ip', '127.0.0.1', '--cluster-announce-port', str(port),
                    '--cluster-announce-bus-port', str(port + 10000), stdout=subprocess.DEVNULL)
                containers.append(name)
                deadline = time.monotonic() + 5
                while True:
                    try:
                        with socket.create_connection(('127.0.0.1', port), timeout=0.2) as connection:
                            connection.sendall(b'*1\r\n$4\r\nPING\r\n')
                            if connection.recv(64) == b'+PONG\r\n':
                                break
                    except OSError:
                        pass
                    if time.monotonic() > deadline:
                        raise RuntimeError('private Cluster node did not start')
                    time.sleep(0.05)
            admin = ['docker', 'exec', containers[0], 'redis-cli']
            run(*admin, '--cluster', 'create', *(f'127.0.0.1:{port}' for port in ports),
                '--cluster-replicas', '0', '--cluster-yes')
            deadline = time.monotonic() + 10
            while True:
                info = run(*admin, '-p', str(ports[0]), 'cluster', 'info', capture_output=True).stdout
                if 'cluster_state:ok' in info and 'cluster_slots_assigned:16384' in info:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError('private Cluster did not become ready: ' + info)
                time.sleep(0.1)
            environment = dict(os.environ, REDIS_ADAPTER_ISOLATED_TEST='1',
                               REDIS_ADAPTER_CLUSTER_TEST_PORT=str(ports[0]))
            return subprocess.run(command, env=environment).returncode
        finally:
            for name in containers:
                subprocess.run(['docker', 'rm', '--force', name],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == '__main__':
    raise SystemExit(main())
