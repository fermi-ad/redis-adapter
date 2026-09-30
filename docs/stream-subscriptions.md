# Owned stream subscriptions

`subscribeStream()` returns a movable, noncopyable `ReaderHandle` owning one
registration. Multiple registrations may share a key. Resetting a handle removes
only that registration; legacy `removeReader()` still removes all registrations
for the key. A handle may safely outlive its adapter.

Cancellation fences callbacks that have not passed their active check. A callback
that already passed that check may still enter the user function. Consumers that
replace generations must also fence their own state changes. Callback exceptions are caught at the worker boundary.

Use an exact snapshot cursor to avoid a gap between snapshot and subscription:

```cpp
auto snapshot = redis.getStreamSnapshot("temperature");
// connected reports accepted snapshot; rejected reports a server-side rejection.
// Failure keeps "$" (future-only), while an accepted empty stream uses "0-0".
if (!snapshot.connected) {
    // Report snapshot.rejected or retry the snapshot after transport recovery.
    return;
}
RedisAdapter::SubscriptionOptions selection;
selection.afterId = snapshot.id;
auto handle = redis.subscribeStream("temperature", callback, selection);
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

A nonempty callback and a valid numeric cursor are required. `subscribeStream()`
throws `std::invalid_argument` for bad input and `std::runtime_error` during
shutdown. IDs without a sequence part compare as sequence zero. Retain the
`[[nodiscard]]` handle; discarding it immediately removes the subscription.

`RA_Options::readerBatchCount` defaults to 64 entries per stream per XREAD (zero
is normalized to one). This bounds entry count, not payload bytes or total worker
queue size. Consumers must enforce their own payload and backlog budgets.
Fresh complete batches share storage across registrations; rewound registrations
copy only the entries they have not delivered.

Tail resolution is retried after transient failure. Unresolved keys are excluded
from XREAD instead of repeatedly sending `$`; healthy keys can keep flowing.
A known missing or wrong-type key starts at `0-0` so a later replacement stream
can be read. If XREVRANGE is denied, Redis 7.4's XREAD `+` tail lookup is used.
If both lookups are denied, the key remains pending until access is restored.
An unavailable initial snapshot cannot guarantee retention from registration time;
use an accepted explicit snapshot cursor when that guarantee is required.

Blocking reads use the independent reader pool and finite physical cycles from
the connection policy. Errors retry with bounded backoff and a state-change log.
Initially disconnected adapters retain registrations through connection recovery.

Legacy single-item typed getters distinguish malformed data with
`RA_INVALID_PAYLOAD` (`err() == 3`), preserve the destination, and reserve zero
for an accepted empty result. Range and callback wrappers skip malformed entries;
they do not fabricate zero values or call back with a fully rejected empty batch.
