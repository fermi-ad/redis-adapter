# RedisAdapterLite

A C++17 Redis stream adapter built directly on [hiredis](https://github.com/redis/hiredis). Provides typed read/write access to Redis Streams with nanosecond timestamps, real-time reader callbacks, pub/sub, and a process watchdog.

Built directly on hiredis with no redis-plus-plus dependency, no cluster support, no templates. Uses `memcpy`-based serialization to avoid alignment UB, and standard modern CMake.

## Features

- Explicit typed API (no templates): string, double, int64, blob, attrs
- Nanosecond-precision timestamps via Redis Stream IDs
- Forward and reverse range queries with count limits
- Bulk add operations for multiple timestamped items
- Real-time stream reader callbacks with deferral support
- Pub/sub messaging
- Process watchdog via Redis hash field expiration (Redis 7.4+)
- Double-buffered `RedisCache` for lock-free reads
- `memcpy`-based serialization (no type-punning / alignment UB)
- Thread-safe: atomic flags, mutex-protected state, joinable threads

## Requirements

- C++17 compiler
- CMake 3.14+
- Redis server (6.0+ for streams, 7.4+ for watchdog expiration)
- hiredis (included as git submodule)
- Google Test and Google Benchmark are fetched automatically via CMake `FetchContent` when enabled

## Build

```bash
# Clone with submodules (hiredis)
git clone --recurse-submodules <repo-url>
cd redis-adapter

# Build library only
cmake -S . -B build
cmake --build build

# Build with tests
cmake -S . -B build -DRAL_BUILD_TESTS=ON
cmake --build build

# Build with benchmarks
cmake -S . -B build -DRAL_BUILD_BENCHMARKS=ON
cmake --build build
```

### CMake options

| Option | Default | Description |
|--------|---------|-------------|
| `RAL_BUILD_TESTS` | `OFF` | Build unit, integration, and regression test suites |
| `RAL_BUILD_BENCHMARKS` | `OFF` | Build the Google Benchmark suite |
| `RAL_COVERAGE` | `OFF` | Instrument with gcov (`--coverage`) for coverage reports |
| `RAL_PROFILE` | `OFF` | Instrument with gprof (`-pg`) for CPU profiling |

Options can be combined:

```bash
cmake -S . -B build -DRAL_BUILD_TESTS=ON -DRAL_COVERAGE=ON -DCMAKE_BUILD_TYPE=Debug
cmake -S . -B build -DRAL_BUILD_BENCHMARKS=ON -DRAL_PROFILE=ON -DCMAKE_BUILD_TYPE=Release
```

## Tests

Tests are split into three suites under `tests/`. A running Redis server is required for integration and regression tests.

```bash
# Build with tests enabled
cmake -S . -B build -DRAL_BUILD_TESTS=ON
cmake --build build

# Run all tests via CTest
cd build && ctest --output-on-failure

# Or run each suite directly
./build/ral-test-unit            # Pure logic tests (no Redis needed)
./build/ral-test-integration     # Requires Redis on 127.0.0.1:6379
./build/ral-test-regressions     # Requires Redis on 127.0.0.1:6379
```

### Test suites

| Target | Directory | Tests | Description |
|--------|-----------|-------|-------------|
| `ral-test-unit` | `tests/unit/` | RAL_Time, RAL_Helpers, ThreadPool | Pure logic, no Redis |
| `ral-test-integration` | `tests/integration/` | Add/get, range, bulk, pub/sub, readers, concurrency, reconnect | Full round-trip with Redis |
| `ral-test-regressions` | `tests/regressions/` | Auth injection, deadlocks, race conditions | Bug-specific regression tests |

## Coverage

Generate an HTML coverage report using lcov/gcov:

```bash
# Build with coverage instrumentation
cmake -S . -B build-coverage -DRAL_BUILD_TESTS=ON -DRAL_COVERAGE=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build build-coverage

# Run all test suites to generate .gcda files
./build-coverage/ral-test-unit
./build-coverage/ral-test-integration
./build-coverage/ral-test-regressions

# Capture and filter coverage data (src/ only)
lcov --capture --directory build-coverage --output-file build-coverage/coverage.info
lcov --extract build-coverage/coverage.info '*/src/*' --output-file build-coverage/coverage_src.info

# Generate HTML report
genhtml build-coverage/coverage_src.info --output-directory build-coverage/coverage-report

# Open the report
open build-coverage/coverage-report/index.html
```

## Benchmarks

The benchmark suite uses [Google Benchmark](https://github.com/google/benchmark) and requires a running Redis server.

```bash
# Build benchmarks
cmake -S . -B build -DRAL_BUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build

# Run all benchmarks
./build/ral-benchmark

# Run with JSON output (for reports)
./build/ral-benchmark --benchmark_format=json --benchmark_out=benchmark_results.json

# Run a subset of benchmarks
./build/ral-benchmark --benchmark_filter="BM_TCP_Add"
```

## Profiling

CPU profile using gprof:

```bash
# Build with profiling instrumentation
cmake -S . -B build-profile \
    -DRAL_BUILD_BENCHMARKS=ON \
    -DRAL_PROFILE=ON \
    -DCMAKE_BUILD_TYPE=Release
cmake --build build-profile

# Run benchmarks (generates gmon.out)
cd build-profile
./ral-benchmark --benchmark_format=json --benchmark_out=benchmark_results.json

# Generate gprof text report
gprof ral-benchmark gmon.out > gprof_report.txt

# Generate styled HTML report
cd ..
python3 scripts/generate_profile_report.py \
    --benchmark-json build-profile/benchmark_results.json \
    --gprof-txt      build-profile/gprof_report.txt \
    --output         build-profile/profile_report.html
```

The HTML report includes CPU hotspots with subsystem classification, benchmark results grouped by category with throughput metrics, and optimization recommendations.

## Integration

### As a subdirectory (recommended)

```cmake
add_subdirectory(redis-adapter)
target_link_libraries(your_target PRIVATE redis-adapter-lite)
```

All include paths and the hiredis dependency propagate automatically through the CMake target.

### After install

```cmake
find_package(redis-adapter-lite REQUIRED)
target_link_libraries(your_target PRIVATE redis-adapter-lite)
```

## Quick start

```cpp
#include "RedisAdapterLite.hpp"

int main()
{
  RedisAdapterLite redis("MYAPP");

  // Write
  redis.addDouble("temperature", 23.5);
  redis.addInt("count", 42);
  redis.addString("status", "running");

  // Read most recent
  double temp;
  redis.getDouble("temperature", temp);

  // Read range
  auto history = redis.getDoubles("temperature", {.count = 100});

  // Real-time reader
  redis.addReader("temperature",
    [](const std::string& base, const std::string& sub, const TimeAttrsList& data) {
      auto val = ral_to_double(data[0].second);
      if (val) printf("New temp: %f\n", *val);
    }
  );
}
```

## Compatibility wrapper (RedisAdapter)

If you are migrating from the old template-based `RedisAdapter`, a header-only compatibility wrapper is provided. It exposes the original template API on top of `RedisAdapterLite` via composition, so existing code can switch backends with minimal changes.

```cpp
#include "RedisAdapter.hpp"   // instead of the old RedisAdapter header

RA_Options opts;
opts.cxn.host = "10.0.0.5";
opts.cxn.port = 6380;
RedisAdapter redis("MYAPP", opts);

// Template-based add/get — same syntax as the original API
redis.addSingleValue<int64_t>("count", 42);
redis.addSingleDouble("temperature", 23.5);

int64_t count;
redis.getSingleValue<int64_t>("count", count);

// Typed range queries
auto history = redis.getValues<double>("temperature");

// Typed reader callbacks
redis.addValuesReader<double>("temperature",
  [](const std::string& base, const std::string& sub,
     const RedisAdapter::TimeValList<double>& data)
  {
    for (auto& [time, value] : data)
      printf("New temp: %f\n", value);
  }
);
```

Type dispatch is handled at compile time via `if constexpr`:

| Template type `T` | Dispatches to |
|---|---|
| `std::string` | String methods |
| `Attrs` | Attrs methods |
| `double` | Double methods |
| `int64_t` | Int methods |
| Other trivial `T` | Blob path (`sizeof(T)` raw bytes) |

The wrapper also re-creates the old argument structs (`RA_ArgsGet` with `count` defaulting to 1, `RA_ArgsAdd`) and the nested `RA_Options` / `RA_Options::Connection` layout. See [doc/MANUAL.md](doc/MANUAL.md) for full migration details and [doc/API.md](doc/API.md) for the complete wrapper API reference.

**Note:** `psubscribe` (pattern subscribe) is not supported by the lite backend and always returns `false`.

## Documentation

| Document | Description |
|----------|-------------|
| [doc/MANUAL.md](doc/MANUAL.md) | Complete usage manual — architecture, API, threading, error handling |
| [doc/API.md](doc/API.md) | Full API reference for all types, methods, and the compatibility wrapper |
| [doc/BENCHMARKS.md](doc/BENCHMARKS.md) | Performance data across Redis 7.0–8.6, TCP vs UDS |

## Project structure

```
redis-adapter/
  CMakeLists.txt              Main build
  src/                        Library source
    RedisAdapterLite.hpp/cpp    Public API and implementation
    RedisAdapter.hpp            Compatibility wrapper (header-only)
    RAL_Types.hpp               Type definitions
    RAL_Helpers.hpp             Serialization helpers
    RAL_Time.hpp/cpp            Nanosecond timestamps
    HiredisConnection.hpp/cpp   RAII hiredis wrapper
    HiredisReply.hpp/cpp        Reply parsing
    ThreadPool.hpp              Worker thread pool
    RedisCache.hpp              Double-buffered cache
  tests/
    unit/                     Pure logic tests (no Redis)
    integration/              Full round-trip tests (requires Redis)
    regressions/              Bug-specific regression tests
  test/
    benchmark_lite.cpp        Google Benchmark suite
  scripts/
    generate_profile_report.py  Profile report generator (gprof + benchmark JSON → HTML)
  doc/
    MANUAL.md                 Usage manual
    API.md                    API reference
    BENCHMARKS.md             Performance report
    benchmarks/               Raw benchmark JSON files
  hiredis/                    hiredis submodule
```

## License

BSD 3-Clause. See [LICENSE](LICENSE).
