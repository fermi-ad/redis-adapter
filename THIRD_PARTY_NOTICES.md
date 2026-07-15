# Third-Party Notices

Project-authored RedisAdapter code is distributed under the BSD 3-Clause License
in [`LICENSE`](LICENSE). Third-party components retain their own copyright and
license terms. This file is an inventory; the referenced license files are
authoritative.

## Git submodules

| Component | Source | License notice in a recursive checkout |
| --- | --- | --- |
| hiredis | <https://github.com/redis/hiredis> | `hiredis/COPYING` |
| redis-plus-plus | <https://github.com/sewenew/redis-plus-plus> | `redis-plus-plus/LICENSE` |
| GoogleTest | <https://github.com/google/googletest> | `googletest/LICENSE` |
| Google Benchmark | <https://github.com/google/benchmark> | `benchmark/LICENSE` |

GoogleTest and Google Benchmark are used only by optional test and benchmark
targets. Release source bundles include the pinned submodules and their license
files so they can be built without resolving moving dependency revisions.

The test workflow uses the official Redis container image. Redis is not linked
into RedisAdapter or redistributed in the source bundle.
