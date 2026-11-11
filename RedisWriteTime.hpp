#pragma once

#include "RedisTime.hpp"
#include <algorithm>
#include <stdexcept>

namespace redis_adapter_detail {

// Host clocks may repeat or move backwards while a pipeline is queued. Keep
// generated IDs ahead of every earlier queued ID for this key. Explicit IDs
// stay unchanged, including stale IDs which Redis will reject.
inline RA_Time queuedWriteTime(RA_Time requested, RA_Time now, int64_t& latest) {
  if (requested.ok()) {
    latest = std::max(latest, requested.value);
    return requested;
  }
  auto value = std::max<int64_t>(now.value, 1);
  if (value <= latest) {
    if (latest == std::numeric_limits<int64_t>::max())
      throw std::overflow_error("pipeline host timestamp exceeds RA_Time range");
    value = latest + 1;
  }
  latest = value;
  return RA_Time(value);
}

} // namespace redis_adapter_detail
