# RedisAdapter

[![Build and test](https://github.com/fermi-ad/redis-adapter/actions/workflows/test.yml/badge.svg)](https://github.com/fermi-ad/redis-adapter/actions/workflows/test.yml)
[![Release](https://img.shields.io/github/v/release/fermi-ad/redis-adapter)](https://github.com/fermi-ad/redis-adapter/releases)
[![License](https://img.shields.io/github/license/fermi-ad/redis-adapter)](LICENSE)

RedisAdapter is a C++ library for exchanging typed instrumentation data over
Redis Streams. It provides a consistent key, timestamp, and payload model on top
of hiredis and redis-plus-plus.

## Features

- Typed scalar, string, attribute-map, and contiguous-array reads and writes.
- Time-range and latest-value queries with nanosecond `RA_Time` timestamps
  encoded directly in Redis Stream IDs.
- Background stream readers with typed callbacks, configurable reader sharding,
  and a worker pool for callback dispatch.
- Lazy reconnection after failed operations, including restoration of registered
  stream readers after Redis returns.
- Batched reader topology changes through `setDeferReaders()`, avoiding repeated
  thread teardown while a configuration is replaced.
- Redis Cluster-aware `{baseKey}:subKey` construction, which keeps a device's
  streams in one hash slot.
- Standalone Redis or Redis Cluster connections over TCP or a Unix-domain
  socket, with username/password authentication and connection pooling.
- Pub/sub helpers, generic readers for non-RedisAdapter streams, key lifecycle
  helpers, and field-TTL watchdogs.
- A versioned, implementation-independent
  [RedisAdapter wire protocol](docs/redis-adapter-implementation-spec.md).

The wire protocol is version 1.0. The C++ library is version 0.1.0; these are
separate compatibility promises.

## Quick start

RedisAdapter is normally embedded into another CMake project. Clone it with its
dependencies, then build the test target:

```sh
git clone --recurse-submodules https://github.com/fermi-ad/redis-adapter.git
cd redis-adapter
cmake -S . -B build -DREDIS_ADAPTER_TEST=ON
cmake --build build --parallel
```

Start Redis 7.4 or newer with both TCP and the test Unix socket enabled, then run
the suite:

```sh
./redis-start.sh
ctest --test-dir build --output-on-failure
```

A minimal writer and reader look like this:

```cpp
#include "RedisAdapter.hpp"

RedisAdapter redis("BPM01");

RA_Time written = redis.addSingleDouble("position", 1.25);
double position = 0.0;
RA_Time observed = redis.getSingleValue("position", position);

if (!written.ok() || !observed.ok()) {
  // Redis was unavailable or the operation failed.
}
```

The data is stored in the Redis Stream `{BPM01}:position`, under the binary-safe
`_` field. See [Building from source](docs/building.md) for dependencies, test
Redis options, and CMake integration.

## Documentation

- [Documentation index](docs/README.md)
- [Building from source and CMake integration](docs/building.md)
- [C++ API guide](docs/api.md)
- [RedisAdapter Protocol Specification v1.0](docs/redis-adapter-implementation-spec.md)
- [Release process](docs/releasing.md)
- [Changelog](CHANGELOG.md)

## Support and security

Use [GitHub issues](https://github.com/fermi-ad/redis-adapter/issues) for bugs and
feature requests. Report security issues privately as described in
[SECURITY.md](SECURITY.md).

## License

Project-authored code is available under the [BSD 3-Clause License](LICENSE).
The government-rights notice is in [NOTICE](NOTICE), and dependency licenses are
inventoried in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
