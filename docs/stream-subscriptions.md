# Owned stream subscriptions

`subscribeStream()` returns a movable, noncopyable `ReaderHandle` owning one
registration. Multiple registrations may share a key. Resetting a handle removes
only that registration; legacy `removeReader()` still removes all registrations
for the key. A handle may safely outlive its adapter.

Cancellation prevents queued callbacks from starting. An already executing
callback may finish, so consumers that replace generations must also fence their
own state changes. Callback exceptions are caught at the worker boundary.

Use an exact snapshot cursor to avoid a gap between snapshot and subscription:

```cpp
auto snapshot = redis.getStreamSnapshot("temperature");
// snapshot.connected reports command success; snapshot.present() reports data.
// The cursor is "0-0" for an empty stream.
auto handle = redis.subscribeStream("temperature", callback, snapshot.id);
```

Callbacks receive the exact Redis ID and raw field map for each new entry. Each
registration filters already delivered IDs independently, including when a new
registration rewinds a shared reader to an earlier cursor. The legacy
`RA_Time` timestamp interpretation is unchanged; stream ordering must use
`compareStreamIds()` rather than convert the ID to a timestamp and back.

The default `afterId="$"` starts after the current tail. Existing
`addValuesReader()` / `addListsReader()` calls retain their deferred-start
behavior: a reader added while `setDeferReaders(true)` is active starts at the
tail when readers are enabled. Pass an explicit snapshot cursor to the owned
API when updates arriving during deferral must be retained.

`decodeScalar()` and `decodeArray()` validate the default `_` field's byte
length before copying. They retain native legacy byte order, accept empty arrays,
reject missing/truncated payloads and invalid boolean representations, and leave
the destination unchanged on failure. Their optional `maxBytes` argument lets a
consumer enforce its payload budget before allocation. Raw callbacks allow the
consumer to report invalid input instead of silently replacing its last value.

Redis connection, socket, and connection-pool waits use the configured timeout.
Stream readers retry transient read failures; the existing health/reconnect path
still handles initially disconnected adapters and backend rediscovery.
