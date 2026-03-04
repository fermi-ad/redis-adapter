//
//  RAL_Time.cpp
//
//  Nanosecond-precision timestamp implementation
//

#include "RAL_Time.hpp"
#include <chrono>

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
    value = std::stoll(id) * NANOS_PER_MILLI;
    size_t pos = id.find('-');
    if (pos != std::string::npos) { value += std::stoll(id.substr(pos + 1)); }
  }
  catch (...) { value = 0; }
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
