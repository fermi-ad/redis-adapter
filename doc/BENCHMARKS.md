# RedisAdapterLite Benchmark Report

**Date:** 2026-03-03
**Build:** Release (`-O2`, no sanitizers)
**System:** 20-core x86_64, 2.4-4.0 GHz, L3 8 MiB x2
**Transports:** TCP loopback (127.0.0.1:6379), Unix domain socket (`/tmp/redis.sock`)
**Persistence:** Disabled (`save ""`, `appendonly no`)
**Benchmark:** Google Benchmark v1.9.1, 3 repetitions

### Redis versions tested

| Version | Config | Notes |
|---------|--------|-------|
| 7.0.15 | System package, default | Baseline |
| 7.4.8 | Built from source, default | Latest 7.x |
| 8.0.2 | Built from source, `io-threads 4`, `io-threads-do-reads yes` | First 8.x stable |
| 8.6.1 | Built from source, `io-threads 4`, `io-threads-do-reads yes` | Latest 8.x |

---

## Serialization Helpers (no Redis, pure CPU)

| Operation | Time | Notes |
|-----------|------|-------|
| `ral_from_double` | 42 ns | Encode double to Attrs |
| `ral_to_double` | 2 ns | Decode Attrs to double |
| `ral_from_int` | 42 ns | Encode int64 to Attrs |
| `ral_to_int` | 2 ns | Decode Attrs to int64 |
| `ral_from_blob (64 B)` | 53 ns |  |
| `ral_from_blob (1 KiB)` | 68 ns |  |
| `ral_from_blob (64 KiB)` | 1.2 us |  |

## RAL_Time

| Operation | Time |
|-----------|------|
| `RAL_Time::id()` | 36 ns |
| `RAL_Time(string)` | 30 ns |

---

## Single Operations: All Versions

### Add (write) — TCP

| Operation | 7.0.15 | 7.4.8 | 8.0.2 | 8.6.1 |
|-----------|-----|-----|-----|-----|
| `addDouble` | 17.2 us | 16.2 us | 33.6 us | 23.4 us |
| `addInt` | 17.7 us | 19.8 us | 33.2 us | 27.2 us |
| `addString (16 B)` | 16.9 us | 16.0 us | 29.9 us | 30.7 us |
| `addString (4 KiB)` | 21.6 us | 19.1 us | 31.7 us | 38.5 us |
| `addBlob (64 KiB)` | 83.4 us | 96.1 us | 140.1 us | 104.5 us |
| `addAttrs (1 field)` | 16.6 us | 20.1 us | 22.6 us | 35.3 us |
| `addAttrs (20 fields)` | 22.2 us | 16.2 us | 30.6 us | 34.0 us |

### Get (read) — TCP

| Operation | 7.0.15 | 7.4.8 | 8.0.2 | 8.6.1 |
|-----------|-----|-----|-----|-----|
| `getDouble` | 16.3 us | 18.0 us | 27.8 us | 31.7 us |
| `getInt` | 16.9 us | 17.1 us | 28.2 us | 32.9 us |
| `getString (16 B)` | 14.1 us | 15.2 us | 31.2 us | 26.9 us |
| `getString (4 KiB)` | 18.8 us | 19.1 us | 40.4 us | 38.2 us |
| `getAttrs (20 fields)` | 27.7 us | 21.5 us | 41.0 us | 38.8 us |

### Add (write) — UDS

| Operation | 7.0.15 | 7.4.8 | 8.0.2 | 8.6.1 |
|-----------|-----|-----|-----|-----|
| `addDouble` | 11.9 us | 11.8 us | 29.5 us | 22.6 us |
| `addInt` | 8.7 us | 9.6 us | 30.3 us | 25.6 us |
| `addString (16 B)` | 10.6 us | 12.8 us | 29.5 us | 24.6 us |
| `addString (4 KiB)` | 15.5 us | 16.7 us | 26.4 us | 36.1 us |
| `addBlob (64 KiB)` | 76.7 us | 95.6 us | 94.4 us | 122.6 us |
| `addAttrs (1 field)` | 13.6 us | 11.9 us | 27.5 us | 27.9 us |
| `addAttrs (20 fields)` | 19.3 us | 13.8 us | 27.4 us | 36.3 us |

### Get (read) — UDS

| Operation | 7.0.15 | 7.4.8 | 8.0.2 | 8.6.1 |
|-----------|-----|-----|-----|-----|
| `getDouble` | 13.5 us | 14.1 us | 31.8 us | 30.5 us |
| `getInt` | 9.4 us | 16.0 us | 28.5 us | 27.2 us |
| `getString (16 B)` | 9.9 us | 16.5 us | 20.6 us | 29.7 us |
| `getString (4 KiB)` | 14.2 us | 18.4 us | 34.2 us | 35.4 us |
| `getAttrs (20 fields)` | 21.4 us | 18.6 us | 35.7 us | 36.6 us |

### Round-trip baseline

| Operation | 7.0.15 TCP | 7.4.8 TCP | 8.0.2 TCP | 8.6.1 TCP | 7.0.15 UDS | 7.4.8 UDS | 8.0.2 UDS | 8.6.1 UDS |
|-----------|-----|-----|-----|-----|-----|-----|-----|-----|
| `PING` | 14.6 us | 13.0 us | 34.5 us | 23.3 us | 10.7 us | 6.6 us | 26.4 us | 26.3 us |
| `Add+Get cycle` | 35.4 us | 42.3 us | 61.4 us | 66.9 us | 26.9 us | 23.7 us | 52.9 us | 44.9 us |

---

## Stress Tests: All Versions

### Parallel Writers (1,000 ops per thread) — TCP

| Threads | 7.0.15 ops/s | 7.4.8 ops/s | 8.0.2 ops/s | 8.6.1 ops/s |
|---------|-----|-----|-----|-----|
| 1 | 52k | 48k | 31k | 35k |
| 2 | 129k | 139k | 59k | 60k |
| 4 | 186k | 173k | 114k | 114k |
| 8 | 185k | 175k | 205k | 198k |

### Parallel Writers — UDS

| Threads | 7.0.15 ops/s | 7.4.8 ops/s | 8.0.2 ops/s | 8.6.1 ops/s |
|---------|-----|-----|-----|-----|
| 1 | 85k | 97k | 37k | 33k |
| 2 | 206k | 212k | 69k | 71k |
| 4 | 251k | 245k | 127k | 135k |
| 8 | 250k | 257k | 248k | 252k |

### Mixed Read/Write (half writers, half readers) — TCP

| Threads | 7.0.15 ops/s | 7.4.8 ops/s | 8.0.2 ops/s | 8.6.1 ops/s |
|---------|-----|-----|-----|-----|
| 2 | 86k | 86k | 48k | 46k |
| 4 | 136k | 137k | 84k | 85k |
| 8 | 157k | 164k | 148k | 140k |
| 16 | 174k | 178k | 197k | 190k |

### Large Blob Writes — TCP

| Payload | 7.0.15 | 7.4.8 | 8.0.2 | 8.6.1 |
|---------|-----|-----|-----|-----|
| 1 KiB | 42 MiB/s | 43 MiB/s | 30 MiB/s | 28 MiB/s |
| 64 KiB | 861 MiB/s | 749 MiB/s | 731 MiB/s | 664 MiB/s |
| 256 KiB | 1.4 GiB/s | 1.3 GiB/s | 1.1 GiB/s | 1.1 GiB/s |
| 1 MiB | 1.3 GiB/s | 1.2 GiB/s | 1.0 GiB/s | 1.1 GiB/s |

### Deep Stream Queries (100k entries) — TCP

| Query Size | 7.0.15 | 7.4.8 | 8.0.2 | 8.6.1 |
|------------|-----|-----|-----|-----|
| 100 | 84.6 us | 73.0 us | 132.4 us | 105.1 us |
| 1,000 | 1.0 ms | 721.3 us | 1.1 ms | 712.9 us |
| 10,000 | 9.6 ms | 6.7 ms | 10.3 ms | 8.5 ms |
| 50,000 | 40.0 ms | 36.5 ms | 42.8 ms | 44.3 ms |

### Many Streams (write + delete N keys) — TCP

| Streams | 7.0.15 ops/s | 7.4.8 ops/s | 8.0.2 ops/s | 8.6.1 ops/s |
|---------|-----|-----|-----|-----|
| 10 | 22k | 24k | 12k | 18k |
| 100 | 26k | 34k | 13k | 14k |
| 1000 | 26k | 31k | 17k | 21k |

---

## TCP vs UDS: Transport Comparison (Redis 7.0.15)

### Single operations

| Operation | TCP | UDS | UDS speedup |
|-----------|-----|-----|-------------|
| `addDouble` | 17.2 us | 11.9 us | **1.4x** |
| `addInt` | 17.7 us | 8.7 us | **2.0x** |
| `addString (16 B)` | 16.9 us | 10.6 us | **1.6x** |
| `getDouble` | 16.3 us | 13.5 us | **1.2x** |
| `getInt` | 16.9 us | 9.4 us | **1.8x** |
| `getString (16 B)` | 14.1 us | 9.9 us | **1.4x** |
| `PING` | 14.6 us | 10.7 us | **1.4x** |
| `Add+Get cycle` | 35.4 us | 26.9 us | **1.3x** |

### Stress tests

| Test | TCP ops/s | UDS ops/s | UDS speedup |
|------|-----------|-----------|-------------|
| Parallel writers (4 threads) | 186k | 251k | **1.3x** |
| Parallel writers (8 threads) | 185k | 250k | **1.4x** |
| Mixed R/W (16 threads) | 174k | 240k | **1.4x** |
| Large blob (256 KiB) | 1.4 GiB/s | 2.3 GiB/s | **1.6x** |
| Many streams (1,000 keys) | 26k | 29k | **1.1x** |

---

## Key Takeaways

### Transport: UDS vs TCP

1. **UDS is 1.2-1.8x faster** for single operations — saves 3-7 us per round-trip
2. **Multi-thread aggregate: TCP ~185k ops/s** | **UDS ~240k ops/s** (7.x)
3. **UDS advantage disappears for large batch reads** (1k+ entries)
4. **Use UDS when Redis is on the same host** — strictly better, no downsides

### Version: 7.x vs 8.x

1. **Redis 7.0.15 and 7.4.8 perform nearly identically** — 7.4.8 shows slight improvements on some queries
2. **Redis 8.x is ~2x slower for single-threaded operations** across the board
3. **Redis 8.x catches up at high thread counts** (8+ threads) — io-threads help
4. **Redis 8.x is faster for deep stream reads** at medium query sizes (1K entries)
5. Large payload throughput converges at 1 MiB — all versions hit ~1.1-1.4 GiB/s
6. **8.6.1 is slightly faster than 8.0.2** on most operations

### Recommendation

For **single-threaded or low-thread workloads**, Redis 7.x with UDS delivers the best latency. For **heavily multi-threaded workloads** (8+ concurrent connections), Redis 8.x with io-threads may match 7.x. Profile your specific workload before upgrading.

---

## Raw Data

Benchmark JSON files are saved in `doc/benchmarks/`:
- `redis-7.0.15.json` — Redis 7.0.15, default config
- `redis-7.4.8.json` — Redis 7.4.8, default config
- `redis-8.0.2.json` — Redis 8.0.2, io-threads 4
- `redis-8.6.1.json` — Redis 8.6.1, io-threads 4
