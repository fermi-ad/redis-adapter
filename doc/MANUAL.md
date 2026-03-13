# RedisAdapterLite Manual

## Table of contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Building](#building)
4. [Testing](#testing)
5. [Coverage](#coverage)
6. [Benchmarks](#benchmarks)
7. [Profiling](#profiling)
8. [Key naming](#key-naming)
9. [Timestamps (RAL_Time)](#timestamps-ral_time)
10. [Types (RAL_Types)](#types-ral_types)
11. [Serialization helpers (RAL_Helpers)](#serialization-helpers-ral_helpers)
12. [Connection options](#connection-options)
13. [Construction](#construction)
14. [Add operations](#add-operations)
15. [Get operations](#get-operations)
16. [Range queries](#range-queries)
17. [Reverse range queries](#reverse-range-queries)
18. [Bulk add](#bulk-add)
19. [Stream readers](#stream-readers)
20. [Pub/Sub](#pubsub)
21. [Watchdog](#watchdog)
22. [Key management](#key-management)
23. [RedisCache](#rediscache)
24. [Threading model](#threading-model)
25. [Error handling](#error-handling)
26. [Compatibility wrapper (RedisAdapter)](#compatibility-wrapper-redisadapter)

---

## Overview

RedisAdapterLite is a C++17 library that provides typed access to Redis Streams using the hiredis C client directly. It is designed for instrumentation and data acquisition systems where multiple processes exchange timestamped values through Redis.

Each adapter instance owns a **base key** (e.g. `"SENSOR1"`) and reads/writes **sub keys** underneath it. The resulting Redis key is `baseKey:subKey`. All stream entries use nanosecond-precision timestamps encoded in the Redis Stream ID format.

## Architecture

```
                        RedisAdapterLite
                       /       |        \
              HiredisConnection  ThreadPool  ReaderInfo[]
                    |
               redisContext*
                    |
               Redis Server
```

**HiredisConnection** wraps a `redisContext*` with RAII, mutex protection, and all Redis commands (XADD, XRANGE, XREVRANGE, XREAD, XTRIM, DEL, RENAME, COPY, HSET, HEXPIRE, HKEYS, PUBLISH). Binary-safe operations use `redisCommandArgv`.

**HiredisReply** provides `ReplyPtr` (a `unique_ptr<redisReply, freeReplyObject>`) and functions to parse stream reply arrays into `TimeAttrsList`.

**ThreadPool** dispatches reader callbacks to worker threads, preventing blocking on the reader's XREAD loop.

Source files:

| File | Purpose |
|------|---------|
| `RedisAdapterLite.hpp/cpp` | Main public API and implementation |
| `RAL_Types.hpp` | All type aliases and option structs |
| `RAL_Helpers.hpp` | memcpy-based serialization (from/to each type) |
| `RAL_Time.hpp/cpp` | Nanosecond timestamp type and stream ID parsing |
| `HiredisConnection.hpp/cpp` | RAII hiredis wrapper with all Redis commands |
| `HiredisReply.hpp/cpp` | Reply parsing for stream data |
| `ThreadPool.hpp` | Simple worker thread pool |
| `RedisCache.hpp` | Double-buffered cache over a stream reader |

## Building

### Prerequisites

- C++17 compiler (GCC 7+, Clang 5+, MSVC 2017+)
- CMake 3.14+
- Redis server (6.0+ for streams, 7.4+ for watchdog)
- hiredis (included as a git submodule)

### Clone and build

```bash
git clone --recurse-submodules <repo-url>
cd redis-adapter

# Library only
cmake -S . -B build
cmake --build build
```

### CMake options

| Option | Default | Description |
|--------|---------|-------------|
| `RAL_BUILD_TESTS` | `OFF` | Build unit, integration, and regression test suites (fetches Google Test 1.15.2) |
| `RAL_BUILD_BENCHMARKS` | `OFF` | Build the Google Benchmark suite (fetches Google Benchmark 1.9.1) |
| `RAL_COVERAGE` | `OFF` | Add `--coverage -fprofile-arcs -ftest-coverage` to library and test targets |
| `RAL_PROFILE` | `OFF` | Add `-pg` (gprof) to library and benchmark targets |

Options can be combined:

```bash
cmake -S . -B build \
    -DRAL_BUILD_TESTS=ON \
    -DRAL_BUILD_BENCHMARKS=ON \
    -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

### Build targets

| Target | Option required | Description |
|--------|----------------|-------------|
| `redis-adapter-lite` | — | Static library |
| `ral-test-unit` | `RAL_BUILD_TESTS` | Unit tests (no Redis needed) |
| `ral-test-integration` | `RAL_BUILD_TESTS` | Integration tests (requires Redis) |
| `ral-test-regressions` | `RAL_BUILD_TESTS` | Regression tests (requires Redis) |
| `ral-benchmark` | `RAL_BUILD_BENCHMARKS` | Google Benchmark suite (requires Redis) |

### Integration into your project

**As a CMake subdirectory (recommended):**

```cmake
add_subdirectory(redis-adapter)
target_link_libraries(your_target PRIVATE redis-adapter-lite)
```

**After install:**

```bash
cmake --install build --prefix /usr/local
```

```cmake
find_package(redis-adapter-lite REQUIRED)
target_link_libraries(your_target PRIVATE redis-adapter-lite)
```

Headers are installed to `include/redis-adapter-lite/`, the library to `lib/`, and a CMake config file to `lib/cmake/redis-adapter-lite/`.

## Testing

Tests are organized into three suites under `tests/`:

| Suite | Directory | Redis needed | What it covers |
|-------|-----------|-------------|----------------|
| **Unit** | `tests/unit/` | No | RAL_Time parsing, serialization helpers, ThreadPool |
| **Integration** | `tests/integration/` | Yes | Add/get, range queries, bulk, pub/sub, readers, concurrency, reconnect, watchdog |
| **Regression** | `tests/regressions/` | Yes | AUTH injection, subscriber deadlock, watchdog race, reader thread safety |

### Running tests

```bash
# Build
cmake -S . -B build -DRAL_BUILD_TESTS=ON
cmake --build build

# Run all suites via CTest
cd build && ctest --output-on-failure

# Or run each suite individually
./build/ral-test-unit
./build/ral-test-integration
./build/ral-test-regressions

# Run a single test by name
./build/ral-test-unit --gtest_filter="RAL_Time.*"
./build/ral-test-integration --gtest_filter="Concurrent.*"
```

Integration and regression tests expect Redis on `127.0.0.1:6379`. Test keys are prefixed with `TEST:` or `CONC_*:` and cleaned up after each test.

## Coverage

Generate an lcov/gcov HTML coverage report:

```bash
# 1. Build with coverage instrumentation (use Debug for accurate line counts)
cmake -S . -B build-coverage \
    -DRAL_BUILD_TESTS=ON \
    -DRAL_COVERAGE=ON \
    -DCMAKE_BUILD_TYPE=Debug
cmake --build build-coverage

# 2. Run all test suites to generate .gcda profiling data
./build-coverage/ral-test-unit
./build-coverage/ral-test-integration
./build-coverage/ral-test-regressions

# 3. Capture coverage data
lcov --capture --directory build-coverage --output-file build-coverage/coverage.info

# 4. Filter to src/ only (exclude test code, hiredis, gtest)
lcov --extract build-coverage/coverage.info '*/src/*' \
    --output-file build-coverage/coverage_src.info

# 5. Generate HTML report
genhtml build-coverage/coverage_src.info \
    --output-directory build-coverage/coverage-report

# 6. Open the report
open build-coverage/coverage-report/index.html    # macOS
xdg-open build-coverage/coverage-report/index.html  # Linux
```

The `RAL_COVERAGE` option adds `--coverage -fprofile-arcs -ftest-coverage` flags to both the library and all test executables.

## Benchmarks

The benchmark suite (`tests/benchmark/benchmark_lite.cpp`) uses [Google Benchmark](https://github.com/google/benchmark) to measure serialization, single-value add/get, range queries, bulk operations, and stress tests. A running Redis server is required.

```bash
# Build
cmake -S . -B build -DRAL_BUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build

# Run all benchmarks (console output)
./build/ral-benchmark

# Run with JSON output for reporting
./build/ral-benchmark \
    --benchmark_format=json \
    --benchmark_out=benchmark_results.json

# Run a subset
./build/ral-benchmark --benchmark_filter="BM_TCP_Add"
./build/ral-benchmark --benchmark_filter="BM_From|BM_To"
./build/ral-benchmark --benchmark_filter="Stress"
```

See [BENCHMARKS.md](BENCHMARKS.md) for performance data across Redis 7.0–8.6.

## Profiling

CPU profiling with gprof to identify hotspots:

```bash
# 1. Build with profiling + benchmarks
cmake -S . -B build-profile \
    -DRAL_BUILD_BENCHMARKS=ON \
    -DRAL_PROFILE=ON \
    -DCMAKE_BUILD_TYPE=Release
cmake --build build-profile

# 2. Run benchmarks (produces gmon.out in CWD)
cd build-profile
./ral-benchmark \
    --benchmark_format=json \
    --benchmark_out=benchmark_results.json

# 3. Generate raw gprof report
gprof ral-benchmark gmon.out > gprof_report.txt

# 4. Generate styled HTML report (includes benchmark throughput data)
cd ..
python3 scripts/generate_profile_report.py \
    --benchmark-json build-profile/benchmark_results.json \
    --gprof-txt      build-profile/gprof_report.txt \
    --output         build-profile/profile_report.html
```

The `RAL_PROFILE` option adds the `-pg` flag to the library and benchmark targets. The HTML report generator (`scripts/generate_profile_report.py`) parses both gprof output and Google Benchmark JSON to produce a single report with:

- **Summary stat cards** — add latency, get latency, serialization time, blob throughput
- **CPU hotspot table** — functions sorted by self time, with visual bars and subsystem tags (hiredis, RAL, STL, benchmark, system)
- **Benchmark results** — grouped by category (serialization, single ops, range queries, bulk, stress) with wall time, CPU time, iterations, and throughput
- **Optimization recommendations** — based on the profiling data

---

## Key naming

Every Redis key follows the format:

```
baseKey:subKey
```

- **baseKey** is set at construction (e.g. `"MOTOR1"`, `"DAQ"`)
- **subKey** identifies the data channel (e.g. `"position"`, `"temperature"`)
- The full Redis key for `redis("MOTOR1")` writing to `"position"` is `MOTOR1:position`

Some methods accept an optional `baseKey` override to read from other adapters' keys:

```cpp
redis.copy("position", "position_backup", "OTHER_MOTOR");
// copies OTHER_MOTOR:position -> MOTOR1:position_backup
```

## Timestamps (RAL_Time)

`RAL_Time` stores a nanosecond-precision timestamp as a signed 64-bit integer.

```cpp
struct RAL_Time
{
  constexpr RAL_Time(int64_t nanos = 0);  // 0 = uninitialized
  RAL_Time(const std::string& id);        // parse from stream ID "ms-nanos"

  bool ok() const;           // true if value > 0
  uint32_t err() const;      // error code if !ok() (negative of value)

  operator int64_t() const;  // returns value if ok, else 0
  operator uint64_t() const;

  std::string id() const;          // "milliseconds-nanosRemainder"
  std::string id_or_now() const;   // id() if ok, else current time
  std::string id_or_min() const;   // id() if ok, else "-" (stream minimum)
  std::string id_or_max() const;   // id() if ok, else "+" (stream maximum)

  int64_t value;
};

inline constexpr RAL_Time RAL_NOT_CONNECTED(-1);
```

### Stream ID format

Redis Stream IDs are `milliseconds-sequence`. RAL_Time encodes nanoseconds:

```
value = milliseconds * 1,000,000 + nanosRemainder
ID    = "milliseconds-nanosRemainder"
```

For example, `RAL_Time(1709500000123456789)` produces ID `"1709500000123-456789"`.

### Special values

| Value | Meaning |
|-------|---------|
| `> 0` | Valid timestamp. `ok()` returns true. |
| `0`   | Uninitialized / not found. `ok()` returns false. |
| `< 0` | Error code. `ok()` returns false. `err()` returns the code. |
| `-1`  | `RAL_NOT_CONNECTED` — the adapter is disconnected. |

## Types (RAL_Types)

### Data types

```cpp
using Attrs = std::unordered_map<std::string, std::string>;

using TimeString = std::pair<RAL_Time, std::string>;
using TimeDouble = std::pair<RAL_Time, double>;
using TimeInt    = std::pair<RAL_Time, int64_t>;
using TimeBlob   = std::pair<RAL_Time, std::vector<uint8_t>>;
using TimeAttrs  = std::pair<RAL_Time, Attrs>;

using TimeStringList = std::vector<TimeString>;
using TimeDoubleList = std::vector<TimeDouble>;
using TimeIntList    = std::vector<TimeInt>;
using TimeBlobList   = std::vector<TimeBlob>;
using TimeAttrsList  = std::vector<TimeAttrs>;
```

### Callback types

```cpp
// Stream reader callback — receives raw Attrs, use ral_to_* helpers to extract typed data
using ReaderCallback = std::function<void(
    const std::string& baseKey,
    const std::string& subKey,
    const TimeAttrsList& data)>;

// Pub/sub callback
using SubCallback = std::function<void(
    const std::string& baseKey,
    const std::string& subKey,
    const std::string& message)>;
```

### Argument structs

```cpp
struct RAL_GetArgs
{
  std::string baseKey;    // override base key (empty = adapter default)
  RAL_Time minTime;       // lower bound (0 = stream start)
  RAL_Time maxTime;       // upper bound (0 = stream end)
  uint32_t count = 0;     // max entries to return (0 = unlimited)
};

struct RAL_AddArgs
{
  RAL_Time time;          // timestamp (0 = current host time)
  uint32_t trim = 1;      // trim stream to this many entries (0 = no trim)
};
```

### Connection options

```cpp
struct RAL_Options
{
  std::string path;                 // Unix socket path (empty = use TCP)
  std::string host = "127.0.0.1";  // TCP host
  std::string user = "default";    // AUTH username
  std::string password;            // AUTH password
  uint32_t timeout = 500;          // connection timeout (ms)
  uint16_t port = 6379;            // TCP port
  std::string dogname;             // auto-watchdog name (empty = disabled)
  uint16_t workers = 1;            // worker thread pool size
  uint16_t readers = 1;            // reader thread count
};
```

## Construction

```cpp
RedisAdapterLite redis("MYAPP");
```

Connects to `127.0.0.1:6379` with default options.

```cpp
RAL_Options opts;
opts.host = "10.0.0.5";
opts.port = 6380;
opts.password = "secret";
opts.dogname = "MYAPP";       // starts auto-watchdog
opts.workers = 4;             // 4 callback worker threads
RedisAdapterLite redis("MYAPP", opts);
```

Unix socket:

```cpp
RAL_Options opts;
opts.path = "/tmp/redis.sock";
RedisAdapterLite redis("MYAPP", opts);
```

The destructor joins all threads (watchdog, readers, reconnect, subscriber).

## Add operations

All add methods write a single entry to the Redis Stream `baseKey:subKey` and return the `RAL_Time` of the new entry. Returns `RAL_NOT_CONNECTED` on failure.

```cpp
RAL_Time addString(subKey, data, args = {});
RAL_Time addDouble(subKey, data, args = {});
RAL_Time addInt(subKey, data, args = {});                    // int64_t
RAL_Time addBlob(subKey, void* data, size_t size, args = {}); // raw binary
RAL_Time addAttrs(subKey, Attrs data, args = {});            // key-value map
```

### Examples

```cpp
redis.addDouble("temperature", 23.5);
redis.addInt("count", 42);
redis.addString("status", "running");

// Binary blob (e.g., a float array)
std::vector<float> waveform = {1.0f, 2.0f, 3.0f};
redis.addBlob("waveform", waveform.data(), waveform.size() * sizeof(float));

// Key-value attributes
Attrs config = {{"rate", "1000"}, {"gain", "2.5"}};
redis.addAttrs("config", config);
```

### Controlling timestamp and trim

```cpp
// Use server-generated time (default), trim stream to 1000 entries
redis.addDouble("temp", 23.5, {.trim = 1000});

// No trim (keep all entries)
redis.addDouble("temp", 23.5, {.trim = 0});

// Specify exact timestamp (nanoseconds since epoch)
redis.addDouble("temp", 23.5, {.time = RAL_Time(1709500000000000000LL), .trim = 100});
```

## Get operations

Single-value get methods retrieve the most recent entry at or before `maxTime`. They return the timestamp of the entry found, or `RAL_Time(0)` if no entry exists.

```cpp
RAL_Time getString(subKey, string& dest, args = {});
RAL_Time getDouble(subKey, double& dest, args = {});
RAL_Time getInt(subKey, int64_t& dest, args = {});
RAL_Time getBlob(subKey, vector<uint8_t>& dest, args = {});
RAL_Time getAttrs(subKey, Attrs& dest, args = {});
```

### Examples

```cpp
double temp;
RAL_Time when = redis.getDouble("temperature", temp);
if (when.ok())
  printf("Temperature at %s: %f\n", when.id().c_str(), temp);

// Get value at or before a specific time
double old_temp;
redis.getDouble("temperature", old_temp, {.maxTime = some_past_time});

// Get from another adapter's key
int64_t other_count;
redis.getInt("count", other_count, {.baseKey = "OTHER_APP"});
```

## Range queries

Forward range methods return entries in chronological order (oldest first) between `minTime` and `maxTime`. Use `count` to limit results.

```cpp
TimeStringList getStrings(subKey, args = {});
TimeDoubleList getDoubles(subKey, args = {});
TimeIntList    getInts(subKey, args = {});
TimeBlobList   getBlobs(subKey, args = {});
TimeAttrsList  getAttrsRange(subKey, args = {});
```

### Examples

```cpp
// Get all entries
auto all = redis.getDoubles("temperature");
for (auto& [time, value] : all)
  printf("%s: %f\n", time.id().c_str(), value);

// Get entries in a time window
auto window = redis.getDoubles("temperature", {.minTime = t1, .maxTime = t2});

// Get first 10 entries after a time
auto recent = redis.getDoubles("temperature", {.minTime = t1, .count = 10});

// Get last 100 entries (no time bounds, just count from start)
auto latest = redis.getInts("counter", {.count = 100});
```

## Reverse range queries

Reverse range methods return entries before (and including) `maxTime`, limited by `count`. Results are returned in **chronological order** (oldest first), matching the forward range convention.

```cpp
TimeStringList getStringsBefore(subKey, args = {});
TimeDoubleList getDoublesBefore(subKey, args = {});
TimeIntList    getIntsBefore(subKey, args = {});
TimeBlobList   getBlobsBefore(subKey, args = {});
TimeAttrsList  getAttrsBefore(subKey, args = {});
```

### Examples

```cpp
// Get 5 most recent entries
auto last5 = redis.getDoublesBefore("temperature", {.maxTime = RAL_Time(), .count = 5});

// Get 10 entries at or before a specific time
auto before = redis.getIntsBefore("counter", {.maxTime = t1, .count = 10});
```

## Bulk add

Bulk add methods insert multiple timestamped entries in a single call and return the IDs of all successfully added entries.

```cpp
vector<RAL_Time> addStrings(subKey, TimeStringList data, trim = 1);
vector<RAL_Time> addDoubles(subKey, TimeDoubleList data, trim = 1);
vector<RAL_Time> addInts(subKey, TimeIntList data, trim = 1);
vector<RAL_Time> addBlobs(subKey, TimeBlobList data, trim = 1);
vector<RAL_Time> addAttrsBatch(subKey, TimeAttrsList data, trim = 1);
```

The `trim` parameter trims the stream to `max(trim, items_added)` after all entries are inserted.

### Examples

```cpp
// Add a batch of readings
TimeDoubleList readings;
readings.emplace_back(RAL_Time(), 23.5);  // RAL_Time() = auto-timestamp
readings.emplace_back(RAL_Time(), 23.6);
readings.emplace_back(RAL_Time(), 23.7);

auto ids = redis.addDoubles("temperature", readings, 10000);
printf("Added %zu entries\n", ids.size());
```

## Stream readers

Readers use `XREAD BLOCK` to listen for new stream entries in real-time. When data arrives, the registered callback fires on a worker thread.

```cpp
bool addReader(subKey, ReaderCallback func, baseKey = "");
bool removeReader(subKey, baseKey = "");
bool setDeferReaders(bool defer);
```

The callback receives raw `TimeAttrsList` — use the `ral_to_*` helpers to extract typed data.

### Examples

```cpp
redis.addReader("temperature",
  [](const std::string& base, const std::string& sub, const TimeAttrsList& data)
  {
    for (auto& [time, attrs] : data)
    {
      auto val = ral_to_double(attrs);
      if (val) printf("[%s] %s:%s = %f\n", time.id().c_str(), base.c_str(), sub.c_str(), *val);
    }
  }
);

// Later: stop listening
redis.removeReader("temperature");
```

### Deferring readers

When adding or removing multiple readers, use deferral to avoid restarting reader threads repeatedly:

```cpp
redis.setDeferReaders(true);    // pause all readers

redis.addReader("channel_a", callback_a);
redis.addReader("channel_b", callback_b);
redis.addReader("channel_c", callback_c);
redis.removeReader("old_channel");

redis.setDeferReaders(false);   // restart all readers at once
```

### Reader threading

Each reader thread owns a dedicated `redisContext*` for its blocking `XREAD`. Multiple keys can share a reader thread (distributed by hash). Callbacks are dispatched to the worker `ThreadPool` to avoid blocking the XREAD loop.

## Pub/Sub

Publish/subscribe messaging using Redis pub/sub channels. The channel name follows the same `baseKey:subKey` format.

```cpp
bool publish(subKey, message, baseKey = "");
bool subscribe(subKey, SubCallback func, baseKey = "");
bool unsubscribe(subKey, baseKey = "");
```

### Examples

```cpp
// Subscribe
redis.subscribe("alerts",
  [](const std::string& base, const std::string& sub, const std::string& msg)
  {
    printf("Alert from %s:%s: %s\n", base.c_str(), sub.c_str(), msg.c_str());
  }
);

// Publish (can be from same or different adapter instance)
redis.publish("alerts", "temperature exceeded threshold");

// Unsubscribe
redis.unsubscribe("alerts");
```

## Watchdog

The watchdog system uses Redis hash field expiration (`HEXPIRE`, requires Redis 7.4+) to track process liveness.

```cpp
bool addWatchdog(dogname, expiration_seconds);
bool petWatchdog(dogname, expiration_seconds);
vector<string> getWatchdogs();
```

### Auto-watchdog

Set `opts.dogname` to automatically register and pet a watchdog on construction:

```cpp
RAL_Options opts;
opts.dogname = "MY_PROCESS";
RedisAdapterLite redis("MYAPP", opts);
// Watchdog "MY_PROCESS" is now being petted every ~900ms with 1s expiration
```

### Manual watchdog

```cpp
redis.addWatchdog("SUBSYSTEM_A", 5);  // expires in 5 seconds

// Pet periodically from your main loop
redis.petWatchdog("SUBSYSTEM_A", 5);

// Check what's alive
auto dogs = redis.getWatchdogs();
for (auto& name : dogs) printf("  alive: %s\n", name.c_str());
```

## Key management

```cpp
bool del(subKey);
bool rename(srcSubKey, dstSubKey);
bool copy(srcSubKey, dstSubKey, baseKey = "");
bool connected();
```

`copy` supports cross-base-key copying. If the Redis `COPY` command fails (e.g. older Redis), it falls back to `XRANGE` + `XADD`.

```cpp
redis.del("old_data");
redis.rename("temp_data", "final_data");
redis.copy("source_key", "dest_key", "OTHER_BASE");

if (!redis.connected())
  printf("Lost connection to Redis\n");
```

## RedisCache

`RedisCache<T>` provides a double-buffered, lock-free read cache over a blob stream. It registers a reader callback that deserializes incoming blobs into a `vector<T>` and swaps buffers atomically.

```cpp
#include "RedisCache.hpp"

auto redis = std::make_shared<RedisAdapterLite>("MYAPP");
RedisCache<float> cache(redis, "waveform");

// Write a waveform
std::vector<float> data = {1.0f, 2.0f, 3.0f};
redis->addBlob("waveform", data.data(), data.size() * sizeof(float));

// Wait and read
cache.waitForNewValue();
std::vector<float> buffer;
cache.copyReadBuffer(buffer);
```

### Methods

```cpp
RAL_Time copyReadBuffer(vector<T>& dest);          // copy entire buffer
RAL_Time copyReadBuffer(T& dest, int index = 0, int* copied = nullptr);  // copy single element
void waitForNewValue();                              // block until new data arrives
void waitForNewValue(duration timeBetweenChecks);    // block with custom poll interval
bool newValueAvailable();                            // non-blocking check
void clearNewValueAvailable();                       // reset flag
```

## Threading model

| Thread | Purpose | Lifetime |
|--------|---------|----------|
| Main | All add/get/del/copy/rename operations | Application lifetime |
| Watchdog | Pets auto-watchdog every 900ms | Construction to destruction |
| Reader(s) | Blocking `XREAD` on assigned keys | `addReader` to `removeReader` / destruction |
| Subscriber | Blocking `redisGetReply` for pub/sub | `subscribe` to `unsubscribe` / destruction |
| Reconnect | One-shot reconnect on connection loss | Spawned on failure, joined before next |
| Worker pool | Dispatches reader/sub callbacks | Construction to destruction |

All shared state is protected:
- `_reader_mutex` guards the reader map
- `_sub_mutex` guards the subscriber map
- `_watchdog_mutex` + `_watchdog_cv` for watchdog sleep
- `_connecting` atomic flag for reconnect
- `_readers_defer` atomic flag for deferral
- `HiredisConnection._mutex` guards the shared `redisContext*`

Reader threads each own a **separate** `redisContext*` created via `create_context()`, so their blocking `XREAD` does not interfere with the main connection.

## Error handling

- Add methods return `RAL_NOT_CONNECTED` (value = -1) if disconnected. Check with `.ok()`.
- Get methods return `RAL_Time(0)` if no entry found or disconnected. Check with `.ok()`.
- Boolean methods return `false` on failure.
- On connection loss, the adapter spawns a reconnect thread that attempts to restore the connection and restart all readers.
- All errors are logged via `syslog`.

```cpp
RAL_Time result = redis.addDouble("temp", 23.5);
if (!result.ok())
{
  if (result.value == RAL_NOT_CONNECTED.value)
    printf("Redis disconnected\n");
  else
    printf("Add failed, error code: %u\n", result.err());
}
```

## Compatibility wrapper (RedisAdapter)

`RedisAdapter.hpp` is a header-only wrapper that provides the old template-based `RedisAdapter` API on top of `RedisAdapterLite`. It is intended for projects migrating from the original `RedisAdapter` that used redis-plus-plus and templates. The wrapper uses composition (it holds a `RedisAdapterLite` member) and delegates all operations to the lite backend.

### Include

```cpp
#include "RedisAdapter.hpp"
```

This includes `RedisAdapterLite.hpp` automatically.

### Type mappings

The wrapper defines its own type aliases that map to the lite equivalents:

| Wrapper type | Lite equivalent |
|---|---|
| `RA_Time` | `RAL_Time` |
| `RA_NOT_CONNECTED` | `RAL_NOT_CONNECTED` |
| `RA_ArgsGet` | `RAL_GetArgs` (note: `count` defaults to **1**, not 0) |
| `RA_ArgsAdd` | `RAL_AddArgs` |
| `RA_Options` | `RAL_Options` (with nested `cxn` struct, see below) |
| `RedisAdapter::TimeVal<T>` | `std::pair<RA_Time, T>` |
| `RedisAdapter::TimeValList<T>` | `std::vector<TimeVal<T>>` |
| `RedisAdapter::ReaderSubFn<T>` | Typed reader callback |
| `RedisAdapter::ListenSubFn` | `SubCallback` |

**Important difference:** `RA_ArgsGet::count` defaults to **1** (return only the most recent entry), whereas `RAL_GetArgs::count` defaults to **0** (unlimited). This matches the original `RedisAdapter` behavior.

### Options layout

The old `RA_Options` uses a nested `cxn` struct for connection settings:

```cpp
RA_Options opts;
opts.cxn.host     = "10.0.0.5";
opts.cxn.port     = 6380;
opts.cxn.password = "secret";
opts.cxn.path     = "/tmp/redis.sock";  // empty = use TCP
opts.cxn.user     = "default";
opts.cxn.timeout  = 500;                // ms
opts.cxn.size     = 5;                  // pool size — ignored by lite
opts.dogname      = "MY_PROCESS";
opts.workers      = 4;
opts.readers      = 1;
RedisAdapter redis("MYAPP", opts);
```

The `cxn.size` field (connection pool size) is accepted but ignored since `RedisAdapterLite` uses a single connection with mutex protection instead of a pool.

### Template type dispatch

All template methods use `if constexpr` to dispatch to the appropriate lite method at compile time:

| Template type `T` | Dispatches to |
|---|---|
| `std::string` | `addString` / `getString` / `getStrings` / etc. |
| `Attrs` | `addAttrs` / `getAttrs` / `getAttrsRange` / etc. |
| `double` | `addDoubles` / `getDoubles` / etc. (bulk only; see `addSingleDouble` for single) |
| `int64_t` | `addInt` / `getInt` / `getInts` / etc. |
| Other trivial `T` | `addBlob` / `getBlob` / `getBlobs` / etc. (raw `sizeof(T)` bytes) |

The `static_assert` ensures that non-trivial, non-string, non-Attrs types produce a compile error rather than a runtime failure.

### Single value operations

```cpp
// Write
redis.addSingleValue<int64_t>("count", 42);
redis.addSingleValue<std::string>("status", "running");
redis.addSingleDouble("temperature", 23.5);  // double has a dedicated method

// Read
int64_t count;
redis.getSingleValue<int64_t>("count", count);

std::string status;
redis.getSingleValue<std::string>("status", status);

// Trivial struct — goes through the blob path
struct Pose { float x, y, z; };
Pose pose = {1.0f, 2.0f, 3.0f};
redis.addSingleValue<Pose>("pose", pose);

Pose readback;
redis.getSingleValue<Pose>("pose", readback);
```

**Note:** `addSingleValue<double>` is disallowed at compile time. Use `addSingleDouble` instead (this matches the original API's distinction).

### List (array) operations

Lists serialize a `vector<T>` (or `array<T,N>` / `span<T>`) as a contiguous blob of `sizeof(T) * count` bytes:

```cpp
// Write a vector
std::vector<float> waveform = {1.0f, 2.0f, 3.0f};
redis.addSingleList<float>("waveform", waveform);

// Write a fixed-size array
std::array<double, 4> quaternion = {1.0, 0.0, 0.0, 0.0};
redis.addSingleList("orientation", quaternion);

// Read back
std::vector<float> readback;
redis.getSingleList<float>("waveform", readback);
```

### Range queries

```cpp
// Forward range — all entries between minTime and maxTime
auto history = redis.getValues<double>("temperature");
auto recent  = redis.getValuesAfter<double>("temperature", {.minTime = t1, .count = 100});

// Reverse range — entries before maxTime, limited by count
auto last10 = redis.getValuesBefore<double>("temperature", {.maxTime = t1, .count = 10});

// List ranges
auto waveforms = redis.getLists<float>("waveform");
auto before    = redis.getListsBefore<float>("waveform", {.count = 5});
auto after     = redis.getListsAfter<float>("waveform", {.minTime = t1});
```

### Bulk add

```cpp
// Bulk add typed values
RedisAdapter::TimeValList<double> readings;
readings.push_back({RA_Time(), 23.5});
readings.push_back({RA_Time(), 23.6});
auto ids = redis.addValues<double>("temperature", readings, 10000);

// Bulk add lists
RedisAdapter::TimeValList<std::vector<float>> frames;
frames.push_back({RA_Time(), {1.0f, 2.0f, 3.0f}});
frames.push_back({RA_Time(), {4.0f, 5.0f, 6.0f}});
auto list_ids = redis.addLists<float>("waveform", frames, 1000);
```

### Typed stream readers

The wrapper provides typed reader callbacks that deserialize data before calling your function:

```cpp
// Values reader — callback receives TimeValList<double>
redis.addValuesReader<double>("temperature",
  [](const std::string& base, const std::string& sub,
     const RedisAdapter::TimeValList<double>& data)
  {
    for (auto& [time, value] : data)
      printf("[%s] %s:%s = %f\n", time.id().c_str(), base.c_str(), sub.c_str(), value);
  }
);

// Lists reader — callback receives TimeValList<vector<float>>
redis.addListsReader<float>("waveform",
  [](const std::string& base, const std::string& sub,
     const RedisAdapter::TimeValList<std::vector<float>>& data)
  {
    for (auto& [time, vec] : data)
      printf("Received %zu floats\n", vec.size());
  }
);

// Generic reader — callback receives raw Attrs
redis.addGenericReader("config",
  [](const std::string& base, const std::string& sub,
     const RedisAdapter::TimeValList<Attrs>& data)
  {
    for (auto& [time, attrs] : data)
      for (auto& [k, v] : attrs) printf("  %s = %s\n", k.c_str(), v.c_str());
  }
);

redis.removeReader("temperature");
redis.removeGenericReader("config");
```

### Passthrough methods

The following methods delegate directly to `RedisAdapterLite` with identical signatures:

- `connected()`
- `addWatchdog(dogname, expiration)`, `petWatchdog(dogname, expiration)`, `getWatchdogs()`
- `del(subKey)`, `rename(src, dst)`, `copy(src, dst, baseKey)`
- `publish(subKey, message, baseKey)`, `subscribe(subKey, func, baseKey)`, `unsubscribe(subKey, baseKey)`
- `setDeferReaders(defer)`

### Limitations

- **`psubscribe` is not supported.** The lite backend does not implement pattern-based subscriptions. `psubscribe()` always returns `false`.
- **`cxn.size` is ignored.** The lite backend uses a single mutex-protected connection instead of a pool.
- **All `T` types for blob path must be trivially copyable.** A `static_assert` enforces this.

### Migration checklist

1. Replace `#include "RedisAdapter.h"` (or equivalent) with `#include "RedisAdapter.hpp"`.
2. Replace `RedisConnection::Options` with `RA_Options::Connection` (accessed via `opts.cxn`).
3. If you relied on `psubscribe`, replace with exact `subscribe` calls or implement pattern matching in your callback.
4. Remove any references to the connection pool size (`cxn.size`) -- it compiles but has no effect.
5. Link against `redis-adapter-lite` in CMake (the wrapper is header-only, no separate library target).
6. Run `ral-test-integration --gtest_filter="Wrapper.*"` to verify your build (`tests/integration/test_wrapper.cpp`).

