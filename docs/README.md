# RedisAdapter Documentation

## Library users

- [README](../README.md): features and a minimal first build.
- [C++ API guide](api.md): connection options, reads, writes, readers,
  reconnection, watchdogs, and return-value conventions.
- [Protocol Specification v1.0](redis-adapter-implementation-spec.md): the
  implementation-independent Redis wire contract.

## Integrators and contributors

- [Building from source](building.md): supported toolchain, dependencies, tests,
  benchmarks, and parent-project CMake integration.
- [Contributing](../CONTRIBUTING.md): public contribution workflow.
- [Security policy](../SECURITY.md): supported versions and private reporting.

## Maintainers

- [Release process](releasing.md): versioning, validation, tags, and artifacts.
- [Changelog](../CHANGELOG.md): user-visible changes by library release.
- [Third-party notices](../THIRD_PARTY_NOTICES.md): dependency license inventory.

## Versioning model

RedisAdapter has two independent version numbers:

- The C++ library version follows semantic versioning and is stored in
  [`VERSION.txt`](../VERSION.txt).
- The wire-protocol version describes interoperability with independent
  producers and consumers. Its normative text is the
  [Protocol Specification](redis-adapter-implementation-spec.md).

A protocol-compatible implementation does not need to use this C++ library. A
library release does not imply a protocol revision.
