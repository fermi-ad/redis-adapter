# C++ API Guide

This guide groups the public API by behavior. The declarations in
[`RedisAdapter.hpp`](../RedisAdapter.hpp) and
[`RedisConnection.hpp`](../RedisConnection.hpp) remain authoritative.

## Construction and connections

```cpp
RA_Options options;
options.cxn.host = "redis.example.org";
options.cxn.port = 6379;
options.cxn.user = "instrumentation";
options.cxn.password = "secret";

RedisAdapter redis("BPM01", options);
```

`RedisAdapter` first attempts a Redis Cluster connection and falls back to a
standalone connection. Setting `cxn.path` selects a Unix-domain socket and makes
`host` and `port` inapplicable.

| Option | Type | Default | Meaning |
| --- | --- | --- | --- |
| `cxn.path` | `std::string` | empty | Unix-domain socket path; when set, use a socket instead of TCP. |
| `cxn.host` | `std::string` | `127.0.0.1` | TCP host or address. |
| `cxn.port` | `uint16_t` | `6379` | TCP port. |
| `cxn.user` | `std::string` | `default` | Redis ACL username. |
| `cxn.password` | `std::string` | empty | Redis ACL password. |
| `cxn.timeout` | `uint32_t` | `500` | Socket and blocking-read timeout in milliseconds. |
| `cxn.size` | `uint16_t` | `5` | redis-plus-plus connection-pool size. |
| `dogname` | `std::string` | empty | If set, maintain a one-second field-TTL watchdog for this name. |
| `workers` | `uint16_t` | `1` | Worker threads used to dispatch reader callbacks. |
| `readers` | `uint16_t` | `1` | Reader threads across which stream keys are deterministically sharded. |

Credentials are passed directly to redis-plus-plus. Keep them out of source
control and populate `RA_Options` from the consuming application's secret or
configuration mechanism.

## Keys and timestamps

An adapter constructed with base key `BPM01` and called with sub-key `position`
uses `{BPM01}:position`. The braces are Redis Cluster hash tags, keeping streams
with the same base key in one slot.

`RA_Time` contains nanoseconds since the Unix epoch. Positive values are valid;
zero is uninitialized and negative values are errors. Use `ok()` before using a
returned timestamp and `err()` when an error code is needed. The protocol maps
nanoseconds to the Redis Stream ID `<milliseconds>-<nanoseconds-within-ms>`.

## Typed stream reads

| API family | Result |
| --- | --- |
| `getSingleValue<T>()` | Latest scalar, string, or `Attrs` at or before a time. |
| `getSingleList<T>()` | Latest contiguous typed list at or before a time. |
| `getValues<T>()`, `getLists<T>()` | Forward time range. |
| `getValuesBefore<T>()`, `getListsBefore<T>()` | Reverse query ending at a time, limited by `count`. |
| `getValuesAfter<T>()`, `getListsAfter<T>()` | Forward query beginning at a time, limited by `count`. |

`RA_ArgsGet` supplies an optional alternate `baseKey`, minimum and maximum
timestamps, and a count. Fields ignored by a particular query are noted in the
header. Empty range results and connection failures both produce an empty list;
applications that need to distinguish them should also check `connected()` and
their own freshness expectations.

## Typed stream writes

| API family | Behavior |
| --- | --- |
| `addSingleValue<T>()` | Write one scalar, string, or `Attrs`. |
| `addSingleDouble()` | Explicit double-valued convenience overload. |
| `addSingleList<T>()` | Write a vector, array, or compatible contiguous container. |
| `addValues<T>()`, `addLists<T>()` | Write timestamped batches. |

`RA_ArgsAdd.time` selects an explicit timestamp; zero asks the adapter to use
host time. `RA_ArgsAdd.trim` defaults to 1, making a stream a latest-value store.
Use a larger trim target when the application contract requires history.

The generic typed path stores its binary-safe payload under the `_` stream
field. Producer and consumer must agree on type and shape; the core protocol
does not embed a schema.

## Continuous readers

`addValuesReader<T>()` and `addListsReader<T>()` register typed callbacks for a
RedisAdapter stream. `addGenericReader()` registers an attribute-map callback
for a stream whose key does not use the RedisAdapter key schema. Readers begin
at the current stream tail (`$`), so registration does not replay history.

Reader callbacks run through the configured worker pool. Avoid blocking work in
a callback unless the pool is sized and the resulting backpressure is
intentional.

Use `removeReader()` or `removeGenericReader()` to remove registrations. When a
configuration changes several streams at once, bracket the changes with:

```cpp
redis.setDeferReaders(true);
// Add and remove all affected readers.
redis.setDeferReaders(false);
```

Deferral stops the reader threads once, applies the topology changes, then
restarts the final reader set. This is especially useful for hot-reloaded
applications.

## Reconnection behavior

Failed Redis operations trigger a throttled background connection attempt when
another attempt is not already active. After a successful reconnect, registered
stream readers are rebuilt and restarted. A failed call is not automatically
replayed; callers must decide whether retrying a write is safe for their data
model. Use `connected()` for an explicit health probe.

## Other helpers

- `publish()`, `subscribe()`, `psubscribe()`, and `unsubscribe()` wrap Redis
  pub/sub using the same base/sub-key naming convention.
- `copy()`, `rename()`, `del()`, and `exists()` manage RedisAdapter stream keys.
- `addWatchdog()`, `petWatchdog()`, and `getWatchdogs()` manage field-TTL
  watchdog entries. Watchdog expiration requires Redis 7.4 or newer.
- `RedisCache<T>` maintains a double-buffered view of a list stream. It requires
  C++20 and should be evaluated against the consuming application's concurrency
  needs before adoption.

## Error handling

The adapter catches redis-plus-plus errors, records them through syslog, and
returns status values rather than exposing Redis exceptions as its main API.
Check every returned `RA_Time`, boolean, list, or vector of timestamps. A Redis
command that loses its connection after transmission may have an unknown
outcome; applications should make retry behavior explicit.
