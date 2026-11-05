# Stream reader benchmark

This harness compares blocking readers and keyspace-notification readers using
the real adapter and a separate, private Redis process for every case. The
default matrix has seven adapters, 769 and 1,025 keys per adapter, 5,400 aggregate
updates per second, and reader counts of 1, 4, and 16. It exercises both sparse
asynchronous traffic and synchronized bursts. Every case has ten seconds of
warmup followed by thirty seconds of measurement. The full 24-case matrix takes
about sixteen minutes plus setup, draining, and teardown.

Build with initialized hiredis and redis-plus-plus submodules:

```sh
cmake -S . -B build-stream-benchmark \
  -DCMAKE_BUILD_TYPE=Release -DREDIS_ADAPTER_STREAM_BENCHMARK=ON
cmake --build build-stream-benchmark --target stream_reader_benchmark --parallel
python3 scripts/run-stream-reader-benchmark.py \
  --output /tmp/stream-reader-benchmark-results.json
```

The standalone target does not require Google Benchmark. Redis 7.4 or newer must
be on `PATH`, or selected explicitly with `--redis-server /path/to/redis-server`.
Use `--benchmark` when the executable is in a different build directory.

A bounded smoke test uses fewer keys and shorter windows:

```sh
python3 scripts/run-stream-reader-benchmark.py \
  --adapters 2 --keys 16 --readers 1,4 --rate 200 \
  --warmup 0.5 --duration 1 --drain 3 \
  --output /tmp/stream-reader-smoke.json
```

For a production-shaped, longer Linux comparison:

```sh
python3 scripts/run-stream-reader-benchmark.py \
  --keys 769,1025 --readers 1,4,16 --modes blocking,keyspace \
  --patterns sparse,burst --warmup 10 --duration 60 --repeat 3 \
  --output /tmp/stream-reader-linux.json
```

Use `--modes`, `--patterns`, `--keys`, and `--readers` to select comma-separated
subsets. Other workload controls include `--workers`, `--payload-bytes`,
`--batch-count`, `--reconcile-ms`, `--burst-ms`, and `--seed`. The rate is aggregate
across adapters, not per adapter. `--help` lists the defaults.

## Isolation and workload

The runner creates a mode-0700 temporary directory under `/tmp`, launches Redis
with TCP disabled and persistence disabled, and uses only its private Unix
socket. It writes a random fixture token which the benchmark executable verifies
before writing stream data. There is no existing-host, port, or socket option in
the runner. Cleanup signals only the child Redis process it launched. No fixture
or benchmark requires a shared Redis instance, `FLUSHALL`, or a production
configuration change.

The blocking case disables keyspace notifications; the notification case enables
`Kt`. This includes notification-generation overhead in the new backend's cost.
Other Redis configuration is identical. Each matrix case starts with fresh data,
clients, and counters, and result JSON is saved after each completed case.

Each adapter owns a disjoint key set and an independent producer connection.
Producers choose keys uniformly with a reproducible seed. Sparse traffic has a
separately jittered, paced schedule for each producer; it does not wake every key
in lockstep. Burst traffic groups that same workload into synchronized windows
(100 ms by default), pipelining at most 256 writes at a time. The requested rate
is a pacing target. The report exposes schedule delay, unsent writes, and actual
accepted throughput if the producer or server cannot sustain it.

Raw `XADD` writes use explicit, monotonically increasing per-key IDs (`1-0`,
`2-0`, and so on). Subscriptions start at the explicit `0-0` cursor. Entries
include a producer timestamp, sequence number, phase marker, and configurable
payload. No writes trim the streams: all accepted entries remain available for
the entire case. This intentionally isolates reader behavior from retention
loss; it does not model the API's default approximate trim of one entry.

## Reading the results

- `correctness` compares every accepted entry, including warmup, with callbacks
  after a bounded drain. It reports omissions, duplicates, unexpected entries,
  malformed payloads, and per-key order violations. This is a finite-run check,
  not an exactly-once guarantee across process crashes or unbounded outages.
- `producer` reports scheduled, attempted/accepted, and unsent work. Accepted
  measurement entries are selected by their producer-side send timestamp.
  Check actual rate and schedule lag before comparing CPU results.
- `latency_ms` measures producer preparation through callback entry using the
  same process's monotonic clock. It includes client/pipeline queueing, Redis
  work, reading, and callback scheduling. Measurement-phase entries delivered
  during the drain remain in the latency distribution. Empty samples are
  explicitly counted as zero; their zero-valued percentiles are not evidence of
  low latency.
- `redis` uses `INFO` counter deltas between measurement boundaries. CPU is
  user plus system CPU, expressed as a percentage of one core; it is not a
  percentage of all host CPUs. Input bytes include the small monitoring traffic.
  Command statistics exclude `usec_per_call`, which is not an additive counter.
  Connections, Redis memory, and Redis RSS are sampled at approximately one-second
  intervals; peaks between samples may be missed.
- `benchmark_process_peak_rss_bytes` is the benchmark process's lifetime peak
  resident memory, including producers, readers, and validation bookkeeping.
  Redis memory is reported separately. Stored streams grow throughout each case,
  so compare matching workloads and durations.
- `backend_matched_at_all_measurement_samples` and
  `backend_status_after_drain` expose fallback. A keyspace request that falls
  back to blocking must not be presented as notification-backend performance.
  Status is sampled, so very brief intervening transitions may be missed. The
  numeric fallback reason uses `ReaderFallbackReason` from `RedisAdapter.hpp`.

The executable returns 2 for a correctness/write failure and 3 when the requested
backend was not effective at a measurement sample. The runner preserves all case
results, continues the matrix, and returns nonzero if any case failed. Timeout or
setup failures also appear in the report. No hardware-dependent CPU, throughput,
or latency threshold is imposed by the harness or added to CI.

Compare identical build revisions and Redis versions on the same otherwise quiet
host. Use repeated runs and inspect actual generated load, callback completeness,
lag, CPU, input bytes, and command mix together. Local smoke tests validate the
harness; they do not establish production capacity, packet-loss behavior, or the
performance of an older deployed adapter revision.
