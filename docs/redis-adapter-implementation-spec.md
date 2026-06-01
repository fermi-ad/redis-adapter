# RedisAdapter Protocol Specification v1.0

Version: 1.0
Audience: third-party hardware and firmware vendors, frontend IOC authors, and RedisAdapter consumers
Purpose: define the Redis on-wire protocol used by RedisAdapter-compatible producers and consumers.

This document specifies the Redis data model, naming conventions, timestamp encoding, stream entry
format, and baseline recovery behavior for RedisAdapter-compatible primary data streams.

RedisAdapter itself is the reference implementation. This document is the protocol description for
interoperation with that ecosystem.

## 1. Scope

This specification covers:

- Redis key naming and namespace rules
- Redis Stream usage and retention expectations
- Timestamp encoding into Redis Stream IDs
- Stream entry field conventions and binary payload encoding
- Baseline connection loss and recovery behavior
- Extension points for non-core protocol features

This specification does not cover:

- Redis deployment, clustering, replication, persistence, or security configuration
- ACNET, EPICS, or other control system integration details
- Visualization, archiving, or analysis tooling
- Performance tuning beyond the behavioral guarantees stated here
- The semantic agreement between a producer and consumer for a specific device
- Redis streams that are intentionally outside the RedisAdapter primary-data protocol

## 2. Protocol, Not Device Contract

This document defines the RedisAdapter wire protocol. It is not a complete contract between a
producer and a consumer.

For example, a producer is often a digitizer or firmware-backed software service, and a consumer is
often a frontend IOC or analysis process. Those parties still need a separate agreement for units,
channel meanings, update rates, control behavior, error states, calibration, and operational limits.

RedisAdapter-compatible systems may add fields, sub-keys, retention policies, metadata, and
device-specific conventions. They must preserve the core rules in this document if they expect
generic RedisAdapter consumers to read the primary data.

The practical compatibility rule is:

- If a generic RedisAdapter consumer reads a stream using only the key schema, Stream ID encoding,
  and `_` payload field defined here, it should get the intended primary value without
  producer-specific code.

## 3. Normative Language

The words `MUST`, `MUST NOT`, `SHOULD`, `SHOULD NOT`, and `MAY` are used in their usual RFC-style
sense:

- `MUST` and `MUST NOT` define core protocol requirements.
- `SHOULD` and `SHOULD NOT` define strongly recommended behavior where exceptions may be valid.
- `MAY` defines optional behavior.

## 4. Terminology

| Term | Definition |
| --- | --- |
| Producer | A device or software component that publishes data into Redis. |
| Consumer | A component that reads data from Redis, such as a frontend IOC or analysis tool. |
| Base Key | The logical namespace for a device or subsystem. |
| Sub-Key | A functional subdivision of a Base Key, such as settings, status, or data. |
| Stream Entry | A single Redis Stream element consisting of an ID and field map. |
| `RA_Time` | Nanoseconds since Unix epoch, encoded into Redis Stream IDs. |
| Primary Data | The main value a generic RedisAdapter consumer is expected to read from a stream entry. |

## 5. Redis Key Schema

### 5.1 Key Format

All Redis keys used for primary protocol data MUST follow this pattern:

```text
{<baseKey>}:<subKey>
```

Where:

- `<baseKey>` is a producer-defined device or subsystem namespace.
- `<subKey>` identifies a specific function or data class.

The curly braces are REQUIRED. They force Redis Cluster hash tagging so keys with the same base key
reside in the same cluster slot.

### 5.2 Allowed Characters

- `<baseKey>` and `<subKey>` SHOULD use printable ASCII characters.
- Whitespace is discouraged.
- `<baseKey>` MUST NOT contain `{` or `}`.
- `<subKey>` MUST NOT contain `{` or `}`.
- `:` MAY appear inside `<baseKey>` or `<subKey>` as a hierarchy separator. The first `:` after the
  closing `}` separates the base key from the sub-key; later `:` characters are part of the sub-key.

### 5.3 Examples

```text
{BPM01}:status
{BPM01}:settings
{BPM01}:waveform
{BOOSTER:DCCT:131.225.124.240}:DATA:E12
```

## 6. Redis Stream Usage Model

### 6.1 Mandatory Streams for Primary Data

All primary data published under this specification MUST be written using Redis Streams with `XADD`.

Other Redis data types, including strings, lists, and pub/sub channels, MUST NOT be required for
primary data transport when RedisAdapter protocol compatibility is expected.

Implementations MAY use other Redis data types or non-core Redis Streams for diagnostics, operator
messages, locks, or implementation-specific coordination, provided those features are not required
for a generic RedisAdapter consumer to read primary data.

### 6.2 Retention and Trimming

RedisAdapter commonly treats streams as latest-value stores. The reference implementation defaults
single-value writes to a trim target of 1.

Producer implementations:

- SHOULD trim status, configuration, and scalar latest-value streams to a target length of 1.
- MAY retain longer history for waveform, burst, diagnostic, or archive-oriented data.
- SHOULD document retention behavior per sub-key when retention differs from latest-value behavior.

Redis trimming may be approximate depending on the Redis command options used by the implementation.

Consumers:

- SHOULD tolerate streams that retain more than one entry.
- SHOULD use Stream IDs rather than list position when interpreting ordering or time.

## 7. Timestamp and Stream ID Encoding

### 7.1 Time Representation

Explicit protocol timestamps are represented as nanoseconds since Unix epoch. This value is referred
to as `RA_Time`.

### 7.2 Stream ID Encoding

Redis Stream IDs MUST encode `RA_Time` using this mapping:

```text
<milliseconds>-<nanoseconds_within_millisecond>
```

Where:

- `milliseconds = floor(nanoseconds / 1_000_000)`
- `nanoseconds_within_millisecond = nanoseconds % 1_000_000`

Example:

```text
1689876543210-123456
```

This represents:

```text
1689876543210 ms + 123456 ns
```

### 7.3 Server-Generated IDs

Redis server-generated IDs, such as `*`, MAY be used only when sub-millisecond timestamp precision is
not required.

Consumers MUST NOT assume that the suffix of a server-generated Redis Stream ID represents
nanoseconds. Redis uses the suffix as a sequence number when it generates the ID.

Producers that need nanosecond precision MUST provide explicit Stream IDs using the mapping in
Section 7.2.

Redis requires an explicit Stream ID to be greater than the current maximum ID for that stream.
Producers that publish explicit timestamps MUST handle duplicate or out-of-order timestamps, either
by rejecting the write, adjusting the ID according to a documented policy, or otherwise making the
gap visible to consumers.

The reference RedisAdapter uses explicit host-time IDs for data writes when no `RA_Time` is supplied
by the caller.

## 8. Stream Entry Field Conventions

### 8.1 Field Map Structure

Each Redis Stream entry consists of a map of string fields to string values.

RedisAdapter defines a single default payload field:

```text
_
```

The `_` field is mandatory for all data-bearing entries that are intended to be read by typed
RedisAdapter consumers. Consumers that intentionally read the full field map may process entries
without `_`, but those entries are outside the generic typed-value path.

### 8.2 Binary Payload Encoding

The `_` field contains a binary-safe string with the following rules:

- Fixed-size scalar types are stored as raw bytes.
- Arrays and lists are stored as contiguous raw bytes.
- Strings are stored as byte sequences. Human-readable strings SHOULD use UTF-8.

Numeric and array payloads intended for typed RedisAdapter consumers MUST NOT be JSON, base64, or
text-encoded in the `_` field. Producer-consumer pairs may intentionally define the primary data type
as a string.

Scalar payloads MUST contain exactly one value. Array and list payload lengths MUST be an integral
multiple of the element size.

The core protocol does not carry payload type, element size, units, or array shape. Producers and
consumers must agree on those semantics outside the core stream entry, unless they opt into a
self-description extension.

### 8.3 Endianness and Layout

- Multi-byte numeric types in v1.0 protocol streams MUST be little-endian.
- Floating point values MUST follow IEEE 754 representation.
- Struct packing and alignment MUST be consistent between producer and consumer and MUST be
  documented when structs are used as payloads.

The current C++ RedisAdapter reference implementation writes host-native bytes and assumes
little-endian targets. Producers running on systems that do not naturally store values in the
required layout MUST convert payload bytes before publishing.

### 8.4 Additional Fields

Additional fields MAY be present for metadata, attributes, diagnostics, schema information, or
producer-specific behavior.

Consumers MUST ignore unknown fields unless they have explicitly opted into an extension or
producer-specific contract.

Producers MUST NOT require generic consumers to interpret additional fields in order to read primary
data from `_`.

Known field extensions in current deployments include waveform interval metadata and event-offset
arrays, for example fields such as `in` and `02`.

## 9. Connection Loss and Recovery Behavior

RedisAdapter interoperability depends on producers and consumers behaving reasonably when Redis or
the network is unavailable. This protocol does not require one specific reconnection algorithm.

### 9.1 Producers

Producers SHOULD:

- Maintain a long-lived Redis connection where practical.
- Reconnect after transient connection failures.
- Avoid tight retry loops; use bounded retry cadence, backoff, throttling, or equivalent pressure
  control.
- Surface connection health through logs, diagnostics, watchdogs, or status streams.
- Document data continuity behavior during outages, including whether data is dropped, buffered, or
  acquisition is blocked.
- Use bounded buffering when buffering is part of the continuity policy.

Producers MUST NOT:

- Publish fabricated timestamps to hide an outage.
- Assume a write failed or succeeded if the connection drops mid-command unless Redis confirms the
  result.
- Replay buffered data in a way that violates the Stream ID ordering rules in Section 7.
- Spin aggressively enough to create Redis, network, CPU, or log pressure during an outage.

When write success is unknown, producers SHOULD prefer behavior that is safe for their data model.
For idempotent or latest-value data, rewriting the latest value after reconnect may be acceptable.
For historical or event data, producers should use explicit timestamps and a documented duplicate or
gap handling policy.

### 9.2 Consumers

Consumers SHOULD:

- Reconnect after transient connection failures.
- Resume stream reads from the last observed Stream ID when continuity matters.
- Treat duplicate entries, missing entries, and gaps as possible failure-mode outcomes.
- Distinguish "no data available" from "Redis unavailable" in status reporting.
- Avoid assuming a stream has only one entry unless the relevant sub-key documents latest-value
  retention.

RedisAdapter readers subscribe from the current tail by default. Consumers that need historical
continuity should explicitly read by Stream ID range.

Consumers MUST NOT require producer-specific metadata to read primary data from `_` unless they have
explicitly opted into an extension or device-specific contract.

### 9.3 Watchdogs and Health

Watchdog or health keys MAY be used to expose liveness. They are outside the core primary-data
protocol unless a project-specific contract makes them mandatory.

When watchdogs are used, their key names, expiration behavior, and failure interpretation MUST be
documented by the producer-consumer integration.

## 10. Compliance Requirements

A producer implementation is core-protocol compliant if it:

1. Publishes primary data using Redis Streams.
2. Uses the required `{<baseKey>}:<subKey>` key schema.
3. Encodes explicit timestamps as `RA_Time` Redis Stream IDs when nanosecond precision is required.
4. Stores primary payload data in the `_` field.
5. Preserves binary payload integrity.
6. Uses an intentional retention policy and documents any consumer-visible history expectations.
7. Handles Redis connection loss without tight retry loops, fabricated timestamps, or silent
   permanent failure.

A consumer implementation is core-protocol compliant if it:

1. Reads primary data from Redis Streams.
2. Interprets keys using the required `{<baseKey>}:<subKey>` schema.
3. Interprets explicit Stream IDs using the `RA_Time` mapping.
4. Reads primary payload data from the `_` field.
5. Ignores unknown fields unless it has opted into an extension.
6. Handles Redis connection loss and stream gaps as expected operational conditions.

Non-compliant behavior may result in data being unreadable by RedisAdapter-based consumers.

This checklist applies to RedisAdapter-compatible primary data streams. It does not make every Redis
stream in a deployment part of the core protocol.

## 11. Change Control and Issue Tracking

Changes to this protocol SHOULD be tracked explicitly against this document.

Issues and pull requests that affect RedisAdapter interoperability SHOULD identify:

- The spec section being changed or clarified.
- Whether the change affects core protocol compatibility or an optional extension.
- Whether existing producers or consumers need code changes.
- Whether compatibility tests should be added or updated.

Core protocol changes require more caution than extensions. A change is core if a generic
RedisAdapter consumer must understand it to read primary data. A change is an extension if consumers
can ignore it and still read the `_` payload correctly.

Breaking changes SHOULD include a migration plan or versioned compatibility story before acceptance.

## 12. Protocol Extensions

Extensions MAY define additional fields, sub-keys, conventions, and discovery mechanisms.

Extensions MUST:

- Preserve the core key schema for RedisAdapter-compatible primary data.
- Preserve the `_` field for primary data.
- Allow consumers that do not understand the extension to ignore extension fields.
- Clearly state whether they are producer-specific, project-specific, or intended for general
  RedisAdapter adoption.

Extensions SHOULD:

- Include a short name and version.
- Define compatibility expectations.
- Define failure behavior when extension metadata is absent, stale, or inconsistent.
- Include at least one producer example and one consumer example.

## 13. Candidate Extension: Self-Described Streams

Status: stub, not part of core compliance.

RedisAdapter users are considering an optional extension for self-described streams. The intent is to
let producers publish enough metadata for consumers to discover stream shape and interpretation
without a separate out-of-band document.

Current deployments often carry this information in application configuration, such as
`redis-pvxs-ioc` YAML or frontend XML. This extension would define how much of that description
belongs in Redis itself.

This extension could describe:

- Payload type, such as `float32`, `float64`, `int32`, `uint16`, `string`, or `struct`.
- Element count or array shape.
- Units.
- Engineering limits or display limits.
- Sample rate or nominal update rate.
- Byte order and packing for structured payloads.
- Human-readable signal name and description.
- Producer identity and firmware or software version.
- Schema version.

Open design questions:

1. Should descriptions live as additional fields on each data entry, in a dedicated descriptor stream,
   or in a separate discovery key?
2. Should descriptors be latest-value streams using the same `{<baseKey>}:<subKey>` key schema?
3. How should consumers detect stale metadata after producer restart or firmware update?
4. How much structure should be standardized before this becomes more expensive than useful?
5. Should this extension describe only the `_` payload, or also additional metadata fields?

Possible compatibility rule:

- A stream with self-description metadata must still be readable by a core RedisAdapter consumer that
  ignores all fields except `_`.

Until this extension is accepted, producer-consumer pairs that need self-description should document
their metadata fields and discovery behavior as a project-specific contract.

## 14. Reference Implementation

The Fermilab RedisAdapter C++ library serves as the reference implementation of this protocol.

Vendor implementations are not required to link against or include RedisAdapter source code, but they
MUST interoperate with the protocol described here to be considered RedisAdapter-compatible.
