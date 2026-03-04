//
//  RAL_Types.hpp
//
//  Shared types for RedisAdapterLite
//

#pragma once

#include "RAL_Time.hpp"
#include <string>
#include <vector>
#include <cstdint>
#include <unordered_map>
#include <functional>

// Stream field map
using Attrs = std::unordered_map<std::string, std::string>;

// Time-value pair types (no templates)
using TimeString = std::pair<RAL_Time, std::string>;
using TimeDouble = std::pair<RAL_Time, double>;
using TimeInt    = std::pair<RAL_Time, int64_t>;
using TimeBlob   = std::pair<RAL_Time, std::vector<uint8_t>>;
using TimeAttrs  = std::pair<RAL_Time, Attrs>;

using TimeStringList = std::vector<TimeString>;
using TimeDoubleList = std::vector<TimeDouble>;
using TimeIntList    = std::vector<TimeInt>;
using TimeBlobList   = std::vector<TimeBlob>;
using TimeAttrsList  = std::vector<TimeAttrs>;

// Reader callback: raw Attrs, user extracts typed data via RAL_Helpers
using ReaderCallback = std::function<void(const std::string& baseKey,
                                          const std::string& subKey,
                                          const TimeAttrsList& data)>;

// Pub/sub callback
using SubCallback = std::function<void(const std::string& baseKey,
                                       const std::string& subKey,
                                       const std::string& message)>;

// Options for get operations
struct RAL_GetArgs
{
  std::string baseKey;   // override base key (empty = adapter's default)
  RAL_Time minTime;
  RAL_Time maxTime;
  uint32_t count = 0;
};

// Options for add operations
struct RAL_AddArgs
{
  RAL_Time time;         // 0 = current host time
  uint32_t trim = 1;     // trim stream to this many entries
};

// Connection/adapter options
struct RAL_Options
{
  std::string path;                 // unix socket (empty = TCP)
  std::string host = "127.0.0.1";
  std::string user = "default";
  std::string password;
  uint32_t timeout = 500;          // milliseconds
  uint16_t port = 6379;
  std::string dogname;             // auto-watchdog name (empty = disabled)
  uint16_t workers = 1;            // worker pool size
  uint16_t readers = 1;            // reader thread count
};
