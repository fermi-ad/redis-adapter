# RedisAdapterLite API Reference

C++17 | hiredis | Redis 6.0+ (7.4+ for watchdog)

---

## Headers

| Header | Include for |
|--------|-------------|
| `RedisAdapterLite.hpp` | Main adapter class (includes all below) |
| `RAL_Types.hpp` | Type aliases, option structs, callbacks |
| `RAL_Helpers.hpp` | Serialization helpers (`ral_to_*`, `ral_from_*`) |
| `RAL_Time.hpp` | Nanosecond timestamp type |
| `RedisCache.hpp` | Double-buffered lock-free cache template |
| `HiredisConnection.hpp` | Low-level hiredis wrapper (advanced use) |
| `HiredisReply.hpp` | Reply parsing utilities (advanced use) |
| `ThreadPool.hpp` | Worker thread pool (internal) |

---

## RAL_Time

Nanosecond-precision timestamp. Wraps a signed 64-bit integer.

**Header:** `RAL_Time.hpp`

### Constructors

```cpp
constexpr RAL_Time(int64_t nanos = 0);
RAL_Time(const std::string& id);
```

| Parameter | Description |
|-----------|-------------|
| `nanos` | Nanoseconds since epoch. `0` = uninitialized. |
| `id` | Redis Stream ID string `"milliseconds-nanosRemainder"`. |

### Members

| Member | Signature | Description |
|--------|-----------|-------------|
| `value` | `int64_t` | Raw nanosecond value. |
| `ok` | `bool ok() const` | `true` if `value > 0`. |
| `err` | `uint32_t err() const` | Error code when `!ok()`. Returns `0` if ok. |
| `id` | `std::string id() const` | Stream ID `"ms-nanos"`. |
| `id_or_now` | `std::string id_or_now() const` | `id()` if ok, else `"*"` (server time). |
| `id_or_min` | `std::string id_or_min() const` | `id()` if ok, else `"-"` (stream start). |
| `id_or_max` | `std::string id_or_max() const` | `id()` if ok, else `"+"` (stream end). |

### Conversions

```cpp
operator int64_t() const;   // value if ok, else 0
operator uint64_t() const;  // value if ok, else 0
```

### Constants

```cpp
inline constexpr RAL_Time RAL_NOT_CONNECTED(-1);
```

### Stream ID Encoding

```
value = milliseconds * 1,000,000 + nanosRemainder
ID    = "milliseconds-nanosRemainder"

Example: RAL_Time(1709500000123456789)  ->  "1709500000123-456789"
```

---

## Type Aliases

**Header:** `RAL_Types.hpp`

### Data Types

```cpp
using Attrs = std::unordered_map<std::string, std::string>;
```

### Time-Value Pairs

| Alias | Definition |
|-------|------------|
| `TimeString` | `std::pair<RAL_Time, std::string>` |
| `TimeDouble` | `std::pair<RAL_Time, double>` |
| `TimeInt` | `std::pair<RAL_Time, int64_t>` |
| `TimeBlob` | `std::pair<RAL_Time, std::vector<uint8_t>>` |
| `TimeAttrs` | `std::pair<RAL_Time, Attrs>` |

### List Types

| Alias | Definition |
|-------|------------|
| `TimeStringList` | `std::vector<TimeString>` |
| `TimeDoubleList` | `std::vector<TimeDouble>` |
| `TimeIntList` | `std::vector<TimeInt>` |
| `TimeBlobList` | `std::vector<TimeBlob>` |
| `TimeAttrsList` | `std::vector<TimeAttrs>` |

### Callback Types

```cpp
using ReaderCallback = std::function<void(
    const std::string& baseKey,
    const std::string& subKey,
    const TimeAttrsList& data)>;

using SubCallback = std::function<void(
    const std::string& baseKey,
    const std::string& subKey,
    const std::string& message)>;
```

---

## RAL_Options

**Header:** `RAL_Types.hpp`

Connection and adapter configuration.

```cpp
struct RAL_Options
{
  std::string path;                 // Unix socket path (empty = TCP)
  std::string host = "127.0.0.1";  // TCP host
  std::string user = "default";    // AUTH username
  std::string password;            // AUTH password (empty = no auth)
  uint32_t timeout = 500;          // Connection/command timeout (ms)
  uint16_t port = 6379;            // TCP port
  std::string dogname;             // Auto-watchdog name (empty = disabled)
  uint16_t workers = 1;            // Worker thread pool size
  uint16_t readers = 1;            // Reader thread count
};
```

| Field | Default | Description |
|-------|---------|-------------|
| `path` | `""` | Unix domain socket path. When set, `host`/`port` are ignored. |
| `host` | `"127.0.0.1"` | Redis server hostname or IP (TCP mode). |
| `user` | `"default"` | Redis ACL username. |
| `password` | `""` | Redis password. Empty disables AUTH. |
| `timeout` | `500` | Timeout in milliseconds for connect and commands. |
| `port` | `6379` | Redis server port (TCP mode). |
| `dogname` | `""` | When non-empty, auto-registers and pets a watchdog on construction. |
| `workers` | `1` | Number of worker threads for reader/subscriber callbacks. |
| `readers` | `1` | Number of reader threads. Keys are distributed across readers by hash. |

---

## RAL_AddArgs

**Header:** `RAL_Types.hpp`

Options for add (write) operations.

```cpp
struct RAL_AddArgs
{
  RAL_Time time;       // Timestamp. 0 = current host time.
  uint32_t trim = 1;   // Trim stream to N entries. 0 = no trim.
};
```

| Field | Default | Description |
|-------|---------|-------------|
| `time` | `0` | Timestamp for the entry. `0` uses `*` (server-assigned time). |
| `trim` | `1` | MAXLEN trim after write. Set to `0` to disable trimming. |

---

## RAL_GetArgs

**Header:** `RAL_Types.hpp`

Options for get (read) operations.

```cpp
struct RAL_GetArgs
{
  std::string baseKey;   // Override base key (empty = adapter default)
  RAL_Time minTime;      // Lower bound (0 = stream start)
  RAL_Time maxTime;      // Upper bound (0 = stream end)
  uint32_t count = 0;    // Max entries (0 = unlimited)
};
```

| Field | Default | Description |
|-------|---------|-------------|
| `baseKey` | `""` | Override the adapter's base key for cross-adapter reads. |
| `minTime` | `0` | Minimum timestamp (inclusive). `0` = `"-"` (stream start). |
| `maxTime` | `0` | Maximum timestamp (inclusive). `0` = `"+"` (stream end). |
| `count` | `0` | Maximum entries to return. `0` = unlimited. |

---

## RedisAdapterLite

**Header:** `RedisAdapterLite.hpp`

Main adapter class. Not copyable, not movable.

**Alias:** `using RedisAdapter = RedisAdapterLite;`

### Constructor / Destructor

```cpp
RedisAdapterLite(const std::string& baseKey, const RAL_Options& options = {});
~RedisAdapterLite();
```

| Parameter | Description |
|-----------|-------------|
| `baseKey` | Prefix for all Redis keys. Keys are formed as `baseKey:subKey`. |
| `options` | Connection and behavior options. See [RAL_Options](#ral_options). |

The constructor connects to Redis immediately. If `options.dogname` is set, a background thread begins auto-petting the watchdog.

The destructor joins all background threads (watchdog, readers, subscriber, reconnect).

---

### Connection

#### `connected`

```cpp
bool connected();
```

Sends a `PING` to Redis. Returns `true` if the server responds with `PONG`. Triggers automatic reconnection on failure.

---

### Add Operations

All add methods write a single entry to the Redis Stream at key `baseKey:subKey`. Returns the `RAL_Time` of the new entry, or `RAL_NOT_CONNECTED` on failure.

#### `addString`

```cpp
RAL_Time addString(const std::string& subKey,
                   const std::string& data,
                   const RAL_AddArgs& args = {});
```

Writes a string value. Stored as-is in the stream field `"_"`.

#### `addDouble`

```cpp
RAL_Time addDouble(const std::string& subKey,
                   double data,
                   const RAL_AddArgs& args = {});
```

Writes a double value. Serialized as 8 bytes via `memcpy` into field `"_"`.

#### `addInt`

```cpp
RAL_Time addInt(const std::string& subKey,
                int64_t data,
                const RAL_AddArgs& args = {});
```

Writes a 64-bit integer. Serialized as 8 bytes via `memcpy` into field `"_"`.

#### `addBlob`

```cpp
RAL_Time addBlob(const std::string& subKey,
                 const void* data,
                 size_t size,
                 const RAL_AddArgs& args = {});
```

Writes raw binary data of arbitrary size into field `"_"`. Zero-copy path to hiredis -- the pointer is passed directly to `redisCommandArgv` without intermediate copies.

| Parameter | Description |
|-----------|-------------|
| `data` | Pointer to raw data. `nullptr` writes an empty value. |
| `size` | Size in bytes. |

#### `addAttrs`

```cpp
RAL_Time addAttrs(const std::string& subKey,
                  const Attrs& data,
                  const RAL_AddArgs& args = {});
```

Writes a multi-field key-value map. Each entry in the `Attrs` map becomes a separate field-value pair in the Redis Stream entry.

---

### Get Operations (Single Value)

All single-value get methods retrieve the most recent stream entry at or before `maxTime`. Returns the `RAL_Time` of the entry found, or `RAL_Time(0)` if no entry exists.

#### `getString`

```cpp
RAL_Time getString(const std::string& subKey,
                   std::string& dest,
                   const RAL_GetArgs& args = {});
```

#### `getDouble`

```cpp
RAL_Time getDouble(const std::string& subKey,
                   double& dest,
                   const RAL_GetArgs& args = {});
```

#### `getInt`

```cpp
RAL_Time getInt(const std::string& subKey,
                int64_t& dest,
                const RAL_GetArgs& args = {});
```

#### `getBlob`

```cpp
RAL_Time getBlob(const std::string& subKey,
                 std::vector<uint8_t>& dest,
                 const RAL_GetArgs& args = {});
```

#### `getAttrs`

```cpp
RAL_Time getAttrs(const std::string& subKey,
                  Attrs& dest,
                  const RAL_GetArgs& args = {});
```

---

### Range Queries (Forward)

Return entries in chronological order (oldest first) between `minTime` and `maxTime`. Use `count` to limit results.

#### `getStrings`

```cpp
TimeStringList getStrings(const std::string& subKey,
                          const RAL_GetArgs& args = {});
```

#### `getDoubles`

```cpp
TimeDoubleList getDoubles(const std::string& subKey,
                          const RAL_GetArgs& args = {});
```

#### `getInts`

```cpp
TimeIntList getInts(const std::string& subKey,
                    const RAL_GetArgs& args = {});
```

#### `getBlobs`

```cpp
TimeBlobList getBlobs(const std::string& subKey,
                      const RAL_GetArgs& args = {});
```

#### `getAttrsRange`

```cpp
TimeAttrsList getAttrsRange(const std::string& subKey,
                            const RAL_GetArgs& args = {});
```

---

### Range Queries (Reverse)

Return entries before (and including) `maxTime`, limited by `count`. Results are returned in **chronological order** (oldest first), not reverse order.

#### `getStringsBefore`

```cpp
TimeStringList getStringsBefore(const std::string& subKey,
                                const RAL_GetArgs& args = {});
```

#### `getDoublesBefore`

```cpp
TimeDoubleList getDoublesBefore(const std::string& subKey,
                                const RAL_GetArgs& args = {});
```

#### `getIntsBefore`

```cpp
TimeIntList getIntsBefore(const std::string& subKey,
                          const RAL_GetArgs& args = {});
```

#### `getBlobsBefore`

```cpp
TimeBlobList getBlobsBefore(const std::string& subKey,
                            const RAL_GetArgs& args = {});
```

#### `getAttrsBefore`

```cpp
TimeAttrsList getAttrsBefore(const std::string& subKey,
                             const RAL_GetArgs& args = {});
```

---

### Bulk Add

Insert multiple timestamped entries in a single call. Returns a vector of `RAL_Time` for each successfully added entry. The stream is trimmed to `max(trim, items_added)` after all entries are inserted.

#### `addStrings`

```cpp
std::vector<RAL_Time> addStrings(const std::string& subKey,
                                 const TimeStringList& data,
                                 uint32_t trim = 1);
```

#### `addDoubles`

```cpp
std::vector<RAL_Time> addDoubles(const std::string& subKey,
                                 const TimeDoubleList& data,
                                 uint32_t trim = 1);
```

#### `addInts`

```cpp
std::vector<RAL_Time> addInts(const std::string& subKey,
                              const TimeIntList& data,
                              uint32_t trim = 1);
```

#### `addBlobs`

```cpp
std::vector<RAL_Time> addBlobs(const std::string& subKey,
                               const TimeBlobList& data,
                               uint32_t trim = 1);
```

#### `addAttrsBatch`

```cpp
std::vector<RAL_Time> addAttrsBatch(const std::string& subKey,
                                    const TimeAttrsList& data,
                                    uint32_t trim = 1);
```

---

### Stream Readers

Real-time stream monitoring using `XREAD BLOCK`. Callbacks fire on worker threads.

#### `addReader`

```cpp
bool addReader(const std::string& subKey,
               ReaderCallback func,
               const std::string& baseKey = "");
```

Registers a callback for new entries on `baseKey:subKey`. The callback receives raw `TimeAttrsList` -- use `ral_to_*` helpers to extract typed data. Returns `false` if not connected.

Multiple callbacks can be registered on the same key. Each callback receives its own copy of the data.

#### `removeReader`

```cpp
bool removeReader(const std::string& subKey,
                  const std::string& baseKey = "");
```

Removes all callbacks for the given key and stops the reader thread if no keys remain.

#### `setDeferReaders`

```cpp
bool setDeferReaders(bool defer);
```

When `defer = true`, stops all reader threads. When `defer = false`, restarts them. Use this to batch multiple `addReader`/`removeReader` calls without restarting threads each time.

---

### Pub/Sub

Publish/subscribe messaging. Channel names follow `baseKey:subKey` format.

#### `publish`

```cpp
bool publish(const std::string& subKey,
             const std::string& message,
             const std::string& baseKey = "");
```

Publishes a message to the channel. Returns `false` on connection failure.

#### `subscribe`

```cpp
bool subscribe(const std::string& subKey,
               SubCallback func,
               const std::string& baseKey = "");
```

Subscribes to a channel. The callback fires on a worker thread when messages arrive.

#### `unsubscribe`

```cpp
bool unsubscribe(const std::string& subKey,
                 const std::string& baseKey = "");
```

Unsubscribes from a channel. If no subscriptions remain, stops the subscriber thread.

---

### Watchdog

Process liveness monitoring via Redis hash field expiration. Requires Redis 7.4+ for `HEXPIRE`.

All watchdog data is stored in the hash key `baseKey:watchdog`.

#### `addWatchdog`

```cpp
bool addWatchdog(const std::string& dogname,
                 uint32_t expiration);
```

Registers a new watchdog. Sets the hash field `dogname` to the library's git commit hash and applies `HEXPIRE` with the given expiration in seconds.

#### `petWatchdog`

```cpp
bool petWatchdog(const std::string& dogname,
                 uint32_t expiration);
```

Refreshes the expiration timer for a watchdog. Call periodically to keep the watchdog alive.

#### `getWatchdogs`

```cpp
std::vector<std::string> getWatchdogs();
```

Returns the names of all currently alive (non-expired) watchdog fields.

---

### Key Management

#### `del`

```cpp
bool del(const std::string& subKey);
```

Deletes the Redis key `baseKey:subKey`.

#### `rename`

```cpp
bool rename(const std::string& srcSubKey,
            const std::string& dstSubKey);
```

Renames `baseKey:srcSubKey` to `baseKey:dstSubKey`.

#### `copy`

```cpp
bool copy(const std::string& srcSubKey,
          const std::string& dstSubKey,
          const std::string& baseKey = "");
```

Copies `baseKey:srcSubKey` (or `overrideBaseKey:srcSubKey`) to `baseKey:dstSubKey`. Falls back to manual `XRANGE` + `XADD` if the Redis `COPY` command fails.

---

## Serialization Helpers

**Header:** `RAL_Helpers.hpp`

Convert between typed values and `Attrs` maps. All single-value types use the field name `"_"`. Numeric types use `memcpy`-based serialization (no type-punning or alignment UB).

### Decode (Attrs -> typed value)

| Function | Signature | Returns |
|----------|-----------|---------|
| `ral_to_string` | `std::optional<std::string> ral_to_string(const Attrs&)` | String value or `nullopt`. |
| `ral_to_double` | `std::optional<double> ral_to_double(const Attrs&)` | Double or `nullopt` (requires exactly 8 bytes). |
| `ral_to_int` | `std::optional<int64_t> ral_to_int(const Attrs&)` | Int64 or `nullopt` (requires exactly 8 bytes). |
| `ral_to_blob` | `std::optional<std::vector<uint8_t>> ral_to_blob(const Attrs&)` | Byte vector or `nullopt`. |

### Encode (typed value -> Attrs)

| Function | Signature | Description |
|----------|-----------|-------------|
| `ral_from_string` | `Attrs ral_from_string(const std::string&)` | Wraps string in `{"_": value}`. |
| `ral_from_double` | `Attrs ral_from_double(double)` | 8-byte memcpy into `{"_": bytes}`. |
| `ral_from_int` | `Attrs ral_from_int(int64_t)` | 8-byte memcpy into `{"_": bytes}`. |
| `ral_from_blob` | `Attrs ral_from_blob(const void*, size_t)` | Raw bytes into `{"_": bytes}`. |

### Constants

```cpp
const std::string DEFAULT_FIELD = "_";
```

---

## RedisCache\<T\>

**Header:** `RedisCache.hpp`

Double-buffered, lock-free read cache over a blob stream. Registers a stream reader that deserializes incoming blobs as arrays of `T` and swaps buffers atomically.

### Constructor

```cpp
RedisCache(std::shared_ptr<RedisAdapterLite> ra, std::string subkey);
```

| Parameter | Description |
|-----------|-------------|
| `ra` | Shared pointer to the adapter (must outlive the cache). |
| `subkey` | Stream sub-key to monitor. |

Automatically calls `addReader` on construction.

### Methods

#### `copyReadBuffer` (vector)

```cpp
RAL_Time copyReadBuffer(std::vector<T>& destBuffer);
```

Copies the entire read buffer into `destBuffer`. On the first call (before any reader data arrives), performs a synchronous `getBlob` to seed the buffer. Returns the timestamp of the data, or `RAL_Time(0)` if no data exists.

#### `copyReadBuffer` (single element)

```cpp
RAL_Time copyReadBuffer(T& destValue,
                        int firstIndexToCopy = 0,
                        int* pElementsCopied = nullptr);
```

Copies a single element at `firstIndexToCopy` from the read buffer. Sets `*pElementsCopied` to 1 on success, 0 if the index is out of range.

#### `waitForNewValue`

```cpp
void waitForNewValue();

template<typename Rep, typename Period>
void waitForNewValue(std::chrono::duration<Rep, Period> timeBetweenChecks);
```

Blocks until new data arrives from the stream reader. Default poll interval is 1ms.

#### `newValueAvailable`

```cpp
bool newValueAvailable();
```

Non-blocking check. Returns `true` if the reader has received data since the last `waitForNewValue` or `clearNewValueAvailable`.

#### `clearNewValueAvailable`

```cpp
void clearNewValueAvailable();
```

Resets the new-value flag without reading data.

---

## HiredisConnection

**Header:** `HiredisConnection.hpp`

Low-level RAII wrapper around `redisContext*`. Thread-safe (mutex-protected). Typically used through `RedisAdapterLite`, but available for direct use.

Not copyable.

### Constructor / Destructor

```cpp
explicit HiredisConnection(const RAL_Options& opts);
~HiredisConnection();
```

Connects immediately on construction. The destructor frees the `redisContext`.

### Connection Management

| Method | Signature | Description |
|--------|-----------|-------------|
| `connect` | `bool connect(const RAL_Options& opts)` | Reconnect with new options. |
| `reconnect` | `bool reconnect()` | Reconnect with existing options. |
| `ping` | `bool ping()` | Send PING, return true on PONG. |
| `create_context` | `redisContext* create_context()` | Create a new independent context (for reader threads). Caller owns the returned pointer. |

### Stream Commands

| Method | Signature | Description |
|--------|-----------|-------------|
| `xadd` | `std::string xadd(key, id, fields)` | XADD with multiple fields. Returns stream ID or empty. |
| `xadd_trim` | `std::string xadd_trim(key, id, fields, maxlen)` | XADD with MAXLEN ~ trim. |
| `xadd_single` | `std::string xadd_single(key, id, field, field_len, value, value_len)` | Fast-path single-field XADD using stack arrays. |
| `xadd_trim_single` | `std::string xadd_trim_single(key, id, field, field_len, value, value_len, maxlen)` | Fast-path single-field XADD with MAXLEN ~ trim. |
| `xtrim` | `int64_t xtrim(key, maxlen)` | XTRIM MAXLEN ~. Returns entries removed. |
| `xrange` | `TimeAttrsList xrange(key, min, max)` | XRANGE forward query. |
| `xrange` | `TimeAttrsList xrange(key, min, max, count)` | XRANGE with COUNT. |
| `xrevrange` | `TimeAttrsList xrevrange(key, max, min)` | XREVRANGE reverse query. |
| `xrevrange` | `TimeAttrsList xrevrange(key, max, min, count)` | XREVRANGE with COUNT. |
| `xread_block` | `map<string, TimeAttrsList> xread_block(keys_ids, timeout_ms)` | Blocking XREAD on multiple streams. |

### Key Commands

| Method | Signature | Description |
|--------|-----------|-------------|
| `del` | `int64_t del(key)` | DEL key. Returns keys deleted. |
| `rename` | `bool rename(src, dst)` | RENAME src dst. |
| `copy` | `int64_t copy(src, dst)` | COPY src dst. Returns 1 on success. |
| `exists` | `int64_t exists(key)` | EXISTS key. Returns 1 if exists. |

### Hash Commands

| Method | Signature | Description |
|--------|-----------|-------------|
| `hset` | `bool hset(key, field, value)` | HSET key field value. |
| `hexpire` | `int32_t hexpire(key, field, seconds)` | HEXPIRE (Redis 7.4+). Returns -3 if unsupported. |
| `hkeys` | `std::vector<std::string> hkeys(key)` | HKEYS key. |

### Pub/Sub

| Method | Signature | Description |
|--------|-----------|-------------|
| `publish` | `int64_t publish(channel, message)` | PUBLISH. Returns subscriber count. |

---

## HiredisReply

**Header:** `HiredisReply.hpp`

### Types

```cpp
struct ReplyDeleter {
  void operator()(redisReply* r) const { if (r) freeReplyObject(r); }
};
using ReplyPtr = std::unique_ptr<redisReply, ReplyDeleter>;
```

### Parse Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `parse_entry_fields` | `Attrs parse_entry_fields(redisReply*)` | Parse `[field, value, ...]` array into `Attrs`. |
| `parse_stream_reply` | `TimeAttrsList parse_stream_reply(redisReply*)` | Parse XRANGE/XREVRANGE reply. |
| `parse_xread_reply` | `map<string, TimeAttrsList> parse_xread_reply(redisReply*)` | Parse XREAD reply. |

---

## ThreadPool

**Header:** `ThreadPool.hpp`

Deterministic worker pool. Jobs with the same name hash always run on the same worker thread, preserving ordering per-key.

### Constructor / Destructor

```cpp
ThreadPool(unsigned short num);
~ThreadPool();
```

### Methods

```cpp
void job(const std::string& name, std::function<void(void)> func);
```

Dispatches `func` to the worker selected by `hash(name) % num_workers`. Jobs with the same `name` are serialized.

---

## Error Handling Summary

| Method type | Success | Not found | Disconnected |
|-------------|---------|-----------|--------------|
| `add*` | `RAL_Time` with `ok() == true` | N/A | `RAL_NOT_CONNECTED` (value = -1) |
| `get*` (single) | `RAL_Time` with `ok() == true` | `RAL_Time(0)` | `RAL_Time(0)` |
| `get*` (range) | Non-empty list | Empty list | Empty list |
| `bool` methods | `true` | N/A | `false` |

All errors are logged via `syslog(LOG_ERR, ...)`.

On connection loss, a background reconnect thread is spawned automatically. It restores the connection and restarts all active readers.

---

## Key Naming Convention

All Redis keys follow the format:

```
baseKey:subKey
```

| Example | Base key | Sub key | Redis key |
|---------|----------|---------|-----------|
| `redis("MOTOR1").addDouble("position", 3.14)` | `MOTOR1` | `position` | `MOTOR1:position` |
| `redis("DAQ").getInt("sample_count", val)` | `DAQ` | `sample_count` | `DAQ:sample_count` |
| `redis("APP").getWatchdogs()` | `APP` | `watchdog` | `APP:watchdog` |

Methods accepting a `baseKey` parameter override the adapter's default for cross-adapter reads:

```cpp
// Read from OTHER:position using adapter with base key MYAPP
redis.getDouble("position", val, {.baseKey = "OTHER"});
```

---

## CMake Integration

### As a subdirectory

```cmake
add_subdirectory(redis-adapter)
target_link_libraries(your_target PRIVATE redis-adapter-lite)
```

### After install

```cmake
find_package(redis-adapter-lite REQUIRED)
target_link_libraries(your_target PRIVATE redis-adapter-lite)
```

### Build options

| Option | Default | Description |
|--------|---------|-------------|
| `RAL_BUILD_TESTS` | `OFF` | Build `ral-test` (Google Test). |
| `RAL_BUILD_BENCHMARKS` | `OFF` | Build `ral-benchmark` (Google Benchmark). |

---

## Thread Safety

`RedisAdapterLite` is thread-safe for concurrent calls from multiple threads:

- `HiredisConnection._mutex` serializes all Redis commands on the shared context
- Reader/subscriber threads each own a dedicated `redisContext*`
- `_reader_mutex` and `_sub_mutex` protect reader/subscriber state
- `ThreadPool` dispatches callbacks to worker threads with per-key ordering
- `RedisCache` uses `shared_mutex` for concurrent reads with exclusive buffer swaps
