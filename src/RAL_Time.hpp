//
//  RAL_Time.hpp
//
//  Nanosecond-precision timestamp for RedisAdapterLite
//  Stream IDs are stored as "milliseconds-nanosRemainder"
//

#pragma once

#include <cstdint>
#include <string>

struct RAL_Time
{
  constexpr RAL_Time(int64_t nanos = 0) : value(nanos) {}
  RAL_Time(const std::string& id);

  bool ok() const { return value > 0; }

  operator int64_t()  const { return ok() ? value : 0; }
  operator uint64_t() const { return ok() ? value : 0; }

  uint32_t err() const { return ok() ? 0 : static_cast<uint32_t>(-value); }

  std::string id() const;
  std::string id_or_now() const;

  std::string id_or_min() const { return ok() ? id() : "-"; }
  std::string id_or_max() const { return ok() ? id() : "+"; }

  int64_t value;
};

inline constexpr RAL_Time RAL_NOT_CONNECTED(-1);
