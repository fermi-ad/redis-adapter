#pragma once
#include <charconv>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string>

// Nanosecond legacy timestamps and status values, also usable in mock builds.
struct RA_Time {
  RA_Time(int64_t nanos = 0) : value(nanos) {}
  RA_Time(const std::string& id) : value(0) {
    const auto dash = id.find('-');
    uint64_t millis = 0, remainder = 0;
    const auto end = id.data() + (dash == std::string::npos ? id.size() : dash);
    const auto first = std::from_chars(id.data(), end, millis);
    if (first.ec != std::errc{} || first.ptr != end) return;
    if (dash != std::string::npos) {
      const auto second = std::from_chars(id.data() + dash + 1, id.data() + id.size(), remainder);
      if (second.ec != std::errc{} || second.ptr != id.data() + id.size()) return;
    }
    const uint64_t maximum = std::numeric_limits<int64_t>::max();
    if (remainder > maximum || millis > (maximum - remainder) / 1000000u) return;
    value = static_cast<int64_t>(millis * 1000000u + remainder);
  }
  bool ok() const { return value > 0; }
  operator int64_t() const { return ok() ? value : 0; }
  operator uint64_t() const { return ok() ? value : 0; }
  uint32_t err() const { return ok() ? 0 : -value; }
  std::string id() const {
    return ok() ? std::to_string(value / 1000000) + "-" + std::to_string(value % 1000000) : "0-0";
  }
  std::string id_or_now() const {
    if (ok()) return id();
    const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return RA_Time(now).id();
  }
  std::string id_or_min() const { return ok() ? id() : "-"; }
  std::string id_or_max() const { return ok() ? id() : "+"; }
  friend bool operator==(RA_Time lhs, RA_Time rhs) { return lhs.value == rhs.value; }
  friend bool operator!=(RA_Time lhs, RA_Time rhs) { return !(lhs == rhs); }
  int64_t value;
};

inline const RA_Time RA_NOT_CONNECTED(-1);
inline const RA_Time RA_REJECTED(-2);
inline const RA_Time RA_INVALID_PAYLOAD(-3);
