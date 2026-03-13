//
//  RAL_Time.cpp
//
//  Nanosecond-precision timestamp implementation
//

#include "RAL_Time.hpp"
#include <chrono>
#include <syslog.h>

static const uint32_t NANOS_PER_MILLI = 1'000'000;

static uint64_t nanoseconds_since_epoch()
{
  using namespace std::chrono;
  return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

RAL_Time::RAL_Time(const std::string& id)
{
  try
  {
    int64_t ms = std::stoll(id);
    // Guard against overflow: max safe ms is INT64_MAX / NANOS_PER_MILLI (~year 2262)
    static constexpr int64_t MAX_MS = INT64_MAX / NANOS_PER_MILLI;
    if (ms > MAX_MS)
    {
      syslog(LOG_WARNING, "RAL_Time: timestamp overflow for '%s'", id.c_str());
      value = 0;
      return;
    }
    value = ms * NANOS_PER_MILLI;
    size_t pos = id.find('-');
    if (pos != std::string::npos) { value += std::stoll(id.substr(pos + 1)); }
  }
  catch (const std::exception& e)
  {
    syslog(LOG_WARNING, "RAL_Time: failed to parse '%s': %s", id.c_str(), e.what());
    value = 0;
  }
  catch (...)
  {
    syslog(LOG_WARNING, "RAL_Time: failed to parse '%s'", id.c_str());
    value = 0;
  }
}

std::string RAL_Time::id() const
{
  return ok() ? std::to_string(value / NANOS_PER_MILLI) + "-" + std::to_string(value % NANOS_PER_MILLI)
              : "0-0";
}

std::string RAL_Time::id_or_now() const
{
  return ok() ? id() : RAL_Time(nanoseconds_since_epoch()).id();
}
