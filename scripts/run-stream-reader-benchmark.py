#!/usr/bin/env python3
"""Compare reader backends using a fresh private Redis process for every case."""
import argparse
from datetime import datetime, timezone
import itertools
import json
import math
import os
from pathlib import Path
import platform
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time


def csv_values(value, converter=str):
    values = [converter(item.strip()) for item in value.split(",") if item.strip()]
    if not values:
        raise argparse.ArgumentTypeError("the list must not be empty")
    return values


def positive_int(value):
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def redis_command(path, *arguments):
    """Small RESP client for fixture setup; no redis-py or redis-cli dependency."""
    encoded = [str(argument).encode() for argument in arguments]
    request = b"*%d\r\n" % len(encoded)
    request += b"".join(b"$%d\r\n" % len(value) + value + b"\r\n" for value in encoded)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(0.5)
        connection.connect(str(path))
        connection.sendall(request)
        with connection.makefile("rb") as response:
            line = response.readline()
            if line[:1] == b"-":
                raise RuntimeError(line[1:].decode().strip())
            if line[:1] in (b"+", b":"):
                return line[1:-2].decode()
            if line[:1] == b"$":
                length = int(line[1:-2])
                return None if length < 0 else response.read(length).decode()
            raise RuntimeError("unexpected fixture reply: " + repr(line))


def git_metadata(folder):
    def query(*arguments):
        result = subprocess.run(["git", "-C", str(folder), *arguments], capture_output=True, text=True, check=False)
        return result.stdout.strip() if result.returncode == 0 else None
    return {"revision": query("rev-parse", "HEAD"), "working_tree_status": query("status", "--short")}


def save_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.with_name(path.name + ".tmp")
    staging.write_text(json.dumps(report, indent=2) + "\n")
    staging.replace(path)


def run_case(args, server, benchmark, case):
    started = time.monotonic()
    # /tmp avoids the long macOS TMPDIR prefix exceeding sockaddr_un.sun_path.
    with tempfile.TemporaryDirectory(prefix="ra-stream-", dir="/tmp") as temporary:
        folder = Path(temporary)
        unix_socket = folder / "redis.sock"
        log_path = folder / "redis.log"
        token = secrets.token_hex(24)
        with log_path.open("w") as log:
            redis = subprocess.Popen([
                server, "--port", "0", "--bind", "127.0.0.1", "--protected-mode", "yes",
                "--save", "", "--appendonly", "no", "--dir", str(folder),
                "--unixsocket", str(unix_socket), "--unixsocketperm", "700",
                "--notify-keyspace-events", "Kt" if case["mode"] == "keyspace" else "",
            ], stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 10
                while True:
                    if redis.poll() is not None:
                        raise RuntimeError("private Redis exited: " + log_path.read_text())
                    try:
                        if redis_command(unix_socket, "PING") == "PONG":
                            break
                    except OSError:
                        pass
                    if time.monotonic() >= deadline:
                        raise TimeoutError("private Redis did not become ready: " + log_path.read_text())
                    time.sleep(0.02)
                redis_command(unix_socket, "SET", "stream-reader-benchmark:fixture", token)
                command = [
                    str(benchmark), "--socket", str(unix_socket),
                    "--mode", case["mode"], "--pattern", case["pattern"],
                    "--keys", str(case["keys"]), "--readers", str(case["readers"]),
                    "--adapters", str(args.adapters), "--workers", str(args.workers),
                    "--rate", str(args.rate), "--warmup", str(args.warmup),
                    "--duration", str(args.duration), "--drain", str(args.drain),
                    "--payload-bytes", str(args.payload_bytes), "--batch-count", str(args.batch_count),
                    "--reconcile-ms", str(args.reconcile_ms), "--burst-ms", str(args.burst_ms),
                    "--seed", str(args.seed),
                ]
                environment = dict(os.environ, REDIS_ADAPTER_STREAM_BENCHMARK_FIXTURE=token)
                try:
                    completed = subprocess.run(command, env=environment, capture_output=True, text=True,
                                               timeout=args.case_timeout, check=False)
                    result = {"case": case, "exit_code": completed.returncode,
                              "elapsed_seconds": time.monotonic() - started,
                              "stderr": completed.stderr, "result": None}
                    try:
                        result["result"] = json.loads(completed.stdout)
                    except json.JSONDecodeError:
                        result["error"] = "benchmark did not emit valid JSON"
                        result["stdout"] = completed.stdout
                    if completed.returncode:
                        result["redis_log"] = log_path.read_text()
                    return result
                except subprocess.TimeoutExpired as error:
                    return {"case": case, "exit_code": 124, "elapsed_seconds": time.monotonic() - started,
                            "error": "benchmark exceeded its case timeout", "result": None,
                            "stderr": (error.stderr or b"").decode(errors="replace")
                            if isinstance(error.stderr, bytes) else (error.stderr or ""),
                            "redis_log": log_path.read_text()}
            finally:
                # Only the process created above is signalled. No existing endpoint is accepted.
                redis.terminate()
                try:
                    redis.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    redis.kill()
                    redis.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--benchmark", type=Path, default=Path("build-stream-benchmark/stream_reader_benchmark"), help="benchmark executable")
    parser.add_argument("--redis-server", default="redis-server", help="Redis executable or PATH name")
    parser.add_argument("--output", type=Path, default=Path("stream-reader-benchmark-results.json"), help="incrementally saved JSON report")
    parser.add_argument("--keys", type=lambda value: csv_values(value, positive_int), default=[769, 1025], help="comma-separated keys per adapter")
    parser.add_argument("--readers", type=lambda value: csv_values(value, positive_int), default=[1, 4, 16], help="comma-separated reader counts")
    parser.add_argument("--modes", type=csv_values, default=["blocking", "keyspace"], help="comma-separated reader modes")
    parser.add_argument("--patterns", type=csv_values, default=["sparse", "burst"], help="comma-separated workload patterns")
    parser.add_argument("--adapters", type=positive_int, default=7, help="independent adapter instances")
    parser.add_argument("--workers", type=positive_int, default=1, help="callback workers per adapter")
    parser.add_argument("--rate", type=float, default=5400, help="aggregate requested updates/second")
    parser.add_argument("--warmup", type=float, default=10, help="warmup seconds per case")
    parser.add_argument("--duration", type=float, default=30, help="measurement seconds per case")
    parser.add_argument("--drain", type=float, default=10, help="maximum post-measurement drain seconds")
    parser.add_argument("--payload-bytes", type=int, default=64, help="payload bytes excluding validation metadata")
    parser.add_argument("--batch-count", type=positive_int, default=64, help="reader entries per key per read")
    parser.add_argument("--reconcile-ms", type=positive_int, default=1000, help="notification backend reconciliation interval")
    parser.add_argument("--burst-ms", type=positive_int, default=100, help="burst window milliseconds")
    parser.add_argument("--seed", type=int, default=139, help="reproducible workload seed")
    parser.add_argument("--repeat", type=positive_int, default=1, help="full matrix repetitions")
    parser.add_argument("--case-timeout", type=float, help="override the default warmup + duration + drain + 120 seconds")
    args = parser.parse_args()
    if set(args.modes) - {"blocking", "keyspace"}:
        parser.error("--modes must contain blocking and/or keyspace")
    if set(args.patterns) - {"sparse", "burst"}:
        parser.error("--patterns must contain sparse and/or burst")
    if not all(math.isfinite(value) for value in (args.rate, args.warmup, args.duration, args.drain)) or \
            args.rate <= 0 or args.warmup < 0 or args.duration <= 0 or args.drain < 0 or args.payload_bytes < 0:
        parser.error("invalid workload duration, rate, or payload size")
    if args.case_timeout is None:
        args.case_timeout = args.warmup + args.duration + args.drain + 120
    if not math.isfinite(args.case_timeout) or args.case_timeout <= 0:
        parser.error("--case-timeout must be positive")
    server = shutil.which(args.redis_server)
    if server is None:
        parser.error("redis-server was not found; provide --redis-server /path/to/redis-server")
    benchmark = args.benchmark.resolve()
    if not benchmark.is_file() or not os.access(benchmark, os.X_OK):
        parser.error("build the stream_reader_benchmark executable or provide --benchmark")
    cases = [{"keys": keys, "pattern": pattern, "readers": readers, "mode": mode, "repetition": repetition}
             for repetition, keys, pattern, readers, mode in itertools.product(
                 range(1, args.repeat + 1), args.keys, args.patterns, args.readers, args.modes)]
    repository = Path(__file__).resolve().parent.parent
    report = {"schema_version": 1, "started_utc": datetime.now(timezone.utc).isoformat(),
              "host": {"platform": platform.platform(), "machine": platform.machine(), "cpu_count": os.cpu_count()},
              "source": git_metadata(repository), "benchmark": str(benchmark), "redis_server": server,
              "fixture": "one private Unix-only Redis process per case; persistence disabled",
              "planned_cases": len(cases), "cases": []}
    print(f"Running {len(cases)} cases; each has {args.warmup:g}s warmup + {args.duration:g}s measurement.", file=sys.stderr)
    failed = False
    for index, case in enumerate(cases, 1):
        print(f"[{index}/{len(cases)}] {case}", file=sys.stderr, flush=True)
        try:
            result = run_case(args, server, benchmark, case)
        except (OSError, RuntimeError, TimeoutError) as error:
            result = {"case": case, "exit_code": 1, "error": str(error), "result": None}
        report["cases"].append(result)
        failed |= result["exit_code"] != 0 or result["result"] is None
        save_report(args.output, report)
        if result["result"] is not None:
            data = result["result"]
            print(f"  exit={result['exit_code']} correctness={data['correctness']['pass']} "
                  f"CPU={data['redis']['cpu_percent_one_core']:.1f}% "
                  f"rate={data['producer']['accepted_per_second']:.1f}/s "
                  f"p99={data['latency_ms']['p99']:.3f}ms", file=sys.stderr, flush=True)
        else:
            print("  " + result.get("error", "benchmark failed"), file=sys.stderr, flush=True)
    report["completed_utc"] = datetime.now(timezone.utc).isoformat()
    save_report(args.output, report)
    print(f"Saved {args.output.resolve()}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
