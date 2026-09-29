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

## Continuity and status

Owned readers periodically inspect stream boundaries with read-only `XINFO
STREAM`. `RA_Options.readerProbeMs` defaults to 1000 ms; zero disables inspection
and automatic reset recovery. Legacy unowned subscriptions do not enable probes
by themselves. A pass inspects at most 64 keys per bucket and stops on transport
failure; the interval is a minimum between checks, not a deadline for inspecting
every key in a large bucket. XINFO replies include the first/last entry payloads,
so include this extra traffic when qualifying large-frame workloads.

Deletion, replacement with a non-stream key, or a last-generated ID below an
owned registration's observed cursor starts a new stream epoch. Its delivered
and queued cursors are rewound, and queued callbacks from the previous epoch are
discarded. An already executing callback may finish; callbacks on that key stay
serialized. Recreated streams can then deliver IDs below the previous stream's
maximum. This also means an explicit future cursor is treated as a reset when
inspection finds a lower last-generated ID; disable probing for strict
wait-until-that-future-ID behavior. Ordinary connection outages preserve cursors.
Trimming a stream to empty preserves its last-generated ID and does not rewind.

An inspected wrong-type source is excluded from the shared XREAD until a later
inspection finds it repaired, allowing other streams in the bucket to continue.
No new Redis write or Lua permission is needed. If XINFO is denied, existing
permitted reads continue with `inspected=false` and an inspection-rejection
counter; automatic reset detection is unavailable for that source. Permit
`XINFO` on the same keys for complete continuity observation.

`ReaderHandle::status()` returns a thread-safe snapshot of registration counters
and cursors without I/O. As with the handle itself, do not race it against moving
or resetting that same handle. `active` means the registration is retained, not
that Redis is currently reachable. `connected` reflects the latest bucket read
or inspection transport observation. `stream` and `hasData` describe the last
successful boundary inspection and are meaningful only when `inspected=true`.
`cursor` is the callback deduplication cursor; `observedCursor` also includes
queued data. `lastReceived` is a process-local monotonic callback-delivery time.
`callbacks` and `entries` count attempted callback deliveries; `callbackErrors`
counts exceptions, which do not trigger replay.

Read rejections, transport failures, socket timeouts, inspection failures and
inspection rejections have separate counters. A socket timeout is not proof of
connection loss because Redis blocking-read and client socket deadlines can
overlap. `reconnects` counts observed unavailable-to-connected transitions.
`streamResets` and `disappearances` expose observed stream epochs and absence.
`retentionGaps` counts continuity checks whose saved cursor predates retained
history, initially or after a transport failure. It is a possible gap, not a
number of lost entries. Redis IDs do not encode an exact entry count or a stable
stream identity: delete/recreate cycles entirely between inspections may be
undetectable when the new stream has already passed the old ID. Consumer frame
IDs or an application epoch are still needed to establish exact continuity.

`RedisAdapter.Recovery` exercises reset, absence, wrong-type isolation, trim,
connection recovery, callback fencing and denied inspection permissions on
standalone Redis. Redis Cluster is outside the supported portfolio.
