#!/usr/bin/env python3
"""Run reader recovery against three private Redis nodes, then stop only those processes.

Usage: reader_cluster_test.py /path/to/redis-server /path/to/redis-cli /path/to/reader-test
"""
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

if len(sys.argv) != 4:
    raise SystemExit(__doc__)
server, cli, executable = sys.argv[1:]
processes = []
with tempfile.TemporaryDirectory(prefix='adapter-cluster-') as directory:
    root = Path(directory)
    reservations = []
    ports = []
    for i in range(6):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        ports.append(listener.getsockname()[1])
        reservations.append(listener)
    for listener in reservations:
        listener.close()
    client_ports = ports[:3]
    try:
        for index, port in enumerate(client_ports):
            node = root / str(index)
            node.mkdir()
            log = (node / 'redis.log').open('w')
            process = subprocess.Popen([server, '--bind', '127.0.0.1', '--port', str(port),
                                        '--cluster-enabled', 'yes', '--cluster-port', str(ports[index + 3]),
                                        '--cluster-announce-ip', '127.0.0.1', '--cluster-node-timeout', '1000',
                                        '--cluster-config-file', 'nodes.conf', '--dir', str(node),
                                        '--save', '', '--appendonly', 'no'], stdout=log, stderr=subprocess.STDOUT)
            log.close()
            processes.append(process)
        for port in client_ports:
            for attempt in range(100):
                ping = subprocess.run([cli, '-h', '127.0.0.1', '-p', str(port), 'PING'],
                                      capture_output=True, text=True, timeout=2)
                if ping.returncode == 0 and 'PONG' in ping.stdout:
                    break
                time.sleep(.05)
            else:
                raise RuntimeError('cluster node startup timed out')
        creation = subprocess.run([cli, '--cluster', 'create',
                                   *[f'127.0.0.1:{port}' for port in client_ports],
                                   '--cluster-replicas', '0', '--cluster-yes'], capture_output=True, text=True, timeout=30)
        assert creation.returncode == 0, creation.stdout + creation.stderr
        print(creation.stdout)
        for port in client_ports:
            for attempt in range(100):
                status = subprocess.check_output([cli, '-h', '127.0.0.1', '-p', str(port), 'CLUSTER', 'INFO'],
                                                 text=True, timeout=2)
                if 'cluster_state:ok' in status:
                    break
                time.sleep(.05)
            else:
                raise RuntimeError('cluster not ready')
        test = subprocess.run([executable], env=dict(os.environ, REDIS_ADAPTER_TEST_PORT=str(client_ports[0]),
                              REDIS_ADAPTER_CLUSTER_TEST='1'), timeout=30)
        assert test.returncode == 0, test.returncode
    except BaseException:
        for log in root.glob('*/redis.log'):
            print(log, log.read_text(), file=sys.stderr)
        raise
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
