//
//  HiredisConnection.hpp
//
//  RAII wrapper around redisContext* for single-server Redis
//  No cluster support, no connection pooling
//

#pragma once

#include <hiredis/hiredis.h>
#include "HiredisReply.hpp"
#include "RAL_Types.hpp"
#include <string>
#include <vector>
#include <mutex>
#include <cstdarg>

class HiredisConnection
{
public:
  explicit HiredisConnection(const RAL_Options& opts);
  ~HiredisConnection();

  HiredisConnection(const HiredisConnection&) = delete;
  HiredisConnection& operator=(const HiredisConnection&) = delete;

  //--- Connection management ---
  bool connect(const RAL_Options& opts);
  bool reconnect();
  bool ping();

  //--- Stream commands ---
  std::string xadd(const std::string& key, const std::string& id,
                    const Attrs& fields);
  std::string xadd_trim(const std::string& key, const std::string& id,
                         const Attrs& fields, uint32_t maxlen);

  // Fast-path single-field variants (stack arrays, no heap allocs)
  std::string xadd_single(const std::string& key, const std::string& id,
                           const char* field, size_t field_len,
                           const char* value, size_t value_len);
  std::string xadd_trim_single(const std::string& key, const std::string& id,
                                const char* field, size_t field_len,
                                const char* value, size_t value_len,
                                uint32_t maxlen);

  int64_t xtrim(const std::string& key, uint32_t maxlen);

  TimeAttrsList xrange(const std::string& key, const std::string& min,
                        const std::string& max);
  TimeAttrsList xrange(const std::string& key, const std::string& min,
                        const std::string& max, uint32_t count);
  TimeAttrsList xrevrange(const std::string& key, const std::string& max,
                           const std::string& min);
  TimeAttrsList xrevrange(const std::string& key, const std::string& max,
                           const std::string& min, uint32_t count);

  // Blocking XREAD on multiple streams. Returns empty map on timeout.
  std::unordered_map<std::string, TimeAttrsList>
  xread_block(const std::vector<std::pair<std::string, std::string>>& keys_ids,
              uint32_t timeout_ms);

  //--- Key commands ---
  int64_t del(const std::string& key);
  bool rename(const std::string& src, const std::string& dst);
  int64_t copy(const std::string& src, const std::string& dst);
  int64_t exists(const std::string& key);

  //--- Hash commands (for watchdog) ---
  bool hset(const std::string& key, const std::string& field, const std::string& value);
  int32_t hexpire(const std::string& key, const std::string& field, uint32_t seconds);
  std::vector<std::string> hkeys(const std::string& key);

  //--- Pub/sub ---
  int64_t publish(const std::string& channel, const std::string& message);

  //--- Create a separate context for subscriber/reader threads ---
  redisContext* create_context();

private:
  ReplyPtr cmd(const char* fmt, ...);

  redisContext* _ctx = nullptr;
  RAL_Options _opts;
  std::mutex _mutex;   // protects _ctx
};
