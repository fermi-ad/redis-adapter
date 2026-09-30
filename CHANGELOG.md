# Changelog

This project follows [Semantic Versioning](https://semver.org/). Library release
versions are independent of the RedisAdapter wire-protocol version.

## [Unreleased]

### Added

- `RA_REJECTED` and structured stream write/trim results with error text and a
  topology-refresh decision, available to real and mock consumers.
- Owned subscription handles, exact stream snapshots and shared safe wire decoders.
- Configurable per-stream XREAD entry count and behavior-focused lifecycle tests.
- Independent connect timeout, dedicated blocking-reader pool, private Redis
  fixtures and automatic CI for stacked PRs.

### Changed

- Callers must use `!ok()` or inspect `err()`; checking only the historical
  `RA_NOT_CONNECTED` sentinel no longer covers all failures.
- Batches stop after an unavailable item or READONLY refusal and retain accepted
  timestamps. Exact batch trimming is selectable; final trim failure does not
  roll back accepted writes.
- Standalone stream writes are not replayed; RedisCluster's internal retry policy
  remains upstream behavior. Rejection can represent a transient server refusal.

### Fixed

- READONLY refreshes future connections while preserving its known-rejection status.
- Callback capture cleanup outside worker and reader locks, including exceptions
  and canceled jobs; peer registrations survive handle removal.
- Tail resolution retries, rejection-aware snapshots and future-only failure cursors.
- Exact-width legacy decoding: malformed single items return `RA_INVALID_PAYLOAD`,
  malformed typed callback entries are skipped, and empty arrays remain valid data.
- Finite shutdown cycles, startup/shutdown fencing and complete-batch sharing.
- Failed connection replacements preserve working clients and command availability.

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
