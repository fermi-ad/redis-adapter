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

## Test

```bash
# Start Redis (if not already running)
./redis-start.sh

# Run tests
./build/ral-test
```

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

See [doc/MANUAL.md](doc/MANUAL.md) for the complete API reference and usage guide.

## Project structure

```
redis-adapter/
  CMakeLists.txt           Main build
  src/                     Library source
    RedisAdapterLite.hpp   Public API
    RedisAdapterLite.cpp   Implementation
    RAL_Types.hpp          Type definitions
    RAL_Helpers.hpp        Serialization helpers
    RAL_Time.hpp/cpp       Nanosecond timestamps
    HiredisConnection.hpp/cpp  RAII hiredis wrapper
    HiredisReply.hpp/cpp   Reply parsing
    ThreadPool.hpp         Worker thread pool
    RedisCache.hpp         Double-buffered cache
  test/
    test_lite.cpp          Google Test suite (67 tests)
  hiredis/                 hiredis submodule
```

## License

BSD 3-Clause. See [LICENSE](LICENSE).
