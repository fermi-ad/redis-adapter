# Building from Source

## Requirements

The core library requires:

- CMake 3.14 or newer.
- A compiler and standard library with C++17 support.
- Git with submodule support.
- POSIX threads and syslog support.
- The pinned hiredis and redis-plus-plus submodules.

Tests additionally require GoogleTest and a running Redis server. The complete
suite, including watchdog behavior, requires Redis 7.4 or newer because it uses
the `HEXPIRE` command. Benchmarks require Google Benchmark. Those dependencies
are pinned as submodules and are fetched by a recursive clone.

`RedisCache.hpp` uses C++20 library facilities. Consumers that include that
optional header should compile their target as C++20 even though the core
adapter API is C++17.

## Clone

Use HTTPS for an anonymous recursive clone:

```sh
git clone --recurse-submodules https://github.com/fermi-ad/redis-adapter.git
cd redis-adapter
```

If the repository was cloned without dependencies:

```sh
git submodule update --init --recursive
```

SSH clones also work for GitHub users with an SSH key. The submodules themselves
use public HTTPS URLs, so `--recurse-submodules` does not require SSH credentials.

## Configure and build

Build just the library dependencies and expose the integration variables:

```sh
cmake -S . -B build
cmake --build build --parallel
```

Build the test and benchmark executables:

```sh
cmake -S . -B build \
  -DREDIS_ADAPTER_TEST=ON \
  -DREDIS_ADAPTER_BENCHMARK=ON
cmake --build build --parallel
```

The available project options are:

| Option | Default | Effect |
| --- | --- | --- |
| `REDIS_ADAPTER_TEST` | `OFF` | Build `redis-adapter-test` and register its GoogleTest cases with CTest. |
| `REDIS_ADAPTER_BENCHMARK` | `OFF` | Build `redis-adapter-benchmark`. |

## Test Redis

For a locally installed Redis 7.4 or newer, the helper starts a daemon on
`127.0.0.1:6379` and creates `/tmp/redis.sock`:

```sh
./redis-start.sh
redis-cli ping
redis-cli -s /tmp/redis.sock ping
ctest --test-dir build --output-on-failure
```

The helper is for an isolated development machine. Check that port 6379 and
`/tmp/redis.sock` are unused before starting it. Stop it with:

```sh
redis-cli shutdown
```

CI uses [`docker-compose.test.yml`](../docker-compose.test.yml) with a pinned
official Redis 7.4 image instead of a binary stored in this repository. On a
Linux development host with Docker, the same environment can be started with:

```sh
docker compose -f docker-compose.test.yml up --detach --wait
ctest --test-dir build --output-on-failure
docker compose -f docker-compose.test.yml down --volumes
```

## Parent-project integration

RedisAdapter currently exposes source and dependency variables for a parent
CMake project. Add it as a subdirectory, compile the adapter source into your
target, include its directories, link its dependencies, and require its language
level:

```cmake
cmake_minimum_required(VERSION 3.14)
project(example LANGUAGES CXX)

add_subdirectory(redis-adapter)

add_executable(example
  main.cpp
  ${REDIS_ADAPTER_SOURCES}
)

target_include_directories(example PRIVATE
  ${REDIS_ADAPTER_INCLUDE_DIRS}
)

target_link_libraries(example PRIVATE
  ${REDIS_ADAPTER_LIBRARIES}
)

target_compile_features(example PRIVATE
  ${REDIS_ADAPTER_COMPILER_FEATURES}
)
```

The exported variables are:

| Variable | Meaning |
| --- | --- |
| `REDIS_ADAPTER_VERSION` | Library semantic version from `VERSION.txt`. |
| `REDIS_ADAPTER_SOURCES` | Adapter implementation sources to compile into the parent target. |
| `REDIS_ADAPTER_HEADERS` | Public and implementation headers. |
| `REDIS_ADAPTER_INCLUDE_DIRS` | RedisAdapter, hiredis, and redis-plus-plus include paths. |
| `REDIS_ADAPTER_LIBRARIES` | Static hiredis and redis-plus-plus targets. |
| `REDIS_ADAPTER_COMPILER_FEATURES` | Required core language feature (`cxx_std_17`). |

`RA_VERSION` remains the source Git revision embedded at configure time. It is
used for runtime provenance and watchdog values; it is not the semantic library
version.

## Updating submodules

Dependency revisions are deliberate release inputs. Update one submodule at a
time, run the full suite, review its license, and commit the new gitlink. Avoid
tracking a dependency branch or unpinned archive.
