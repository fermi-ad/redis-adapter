# Changelog

This project follows [Semantic Versioning](https://semver.org/). Library release
versions are independent of the RedisAdapter wire-protocol version.

## [0.1.0] - 2026-07-15

Initial public library release.

### Added

- RedisAdapter Protocol Specification v1.0.
- Versioned source releases containing all pinned git submodules.
- BSD 3-Clause licensing, government-rights notice, and third-party inventory.
- Public build/test CI, contribution guidance, security policy, and organized
  user, integrator, and maintainer documentation.

### Changed

- Switched every submodule remote to HTTPS for anonymous recursive clones.
- Made connection replacement safe for concurrent in-flight Redis operations
  and restored registered readers after lazy reconnection.
- Made the already non-movable `RedisConnection` contract explicit, removing a
  misleading defaulted-move compiler warning.
- Replaced the checked-in Redis test executable with a pinned official Redis
  7.4 container in CI.
- Stopped the optional benchmark target from building Google Benchmark's own
  upstream test suite.

[0.1.0]: https://github.com/fermi-ad/redis-adapter/releases/tag/v0.1.0
