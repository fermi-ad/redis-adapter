//
//  RAL_Helpers.hpp
//
//  Safe serialization helpers using memcpy (no type-punning / alignment UB)
//

#pragma once

#include "RAL_Types.hpp"
#include <cstring>
#include <optional>

inline const std::string DEFAULT_FIELD = "_";

//--- Extract typed data from Attrs ---

inline std::optional<std::string> ral_to_string(const Attrs& attrs)
{
  auto it = attrs.find(DEFAULT_FIELD);
  if (it == attrs.end()) return std::nullopt;
  return it->second;
}

inline std::optional<double> ral_to_double(const Attrs& attrs)
{
  auto it = attrs.find(DEFAULT_FIELD);
  if (it == attrs.end() || it->second.size() != sizeof(double)) return std::nullopt;
  double val;
  std::memcpy(&val, it->second.data(), sizeof(double));
  return val;
}

inline std::optional<int64_t> ral_to_int(const Attrs& attrs)
{
  auto it = attrs.find(DEFAULT_FIELD);
  if (it == attrs.end() || it->second.size() != sizeof(int64_t)) return std::nullopt;
  int64_t val;
  std::memcpy(&val, it->second.data(), sizeof(int64_t));
  return val;
}

inline std::optional<std::vector<uint8_t>> ral_to_blob(const Attrs& attrs)
{
  auto it = attrs.find(DEFAULT_FIELD);
  if (it == attrs.end()) return std::nullopt;
  auto& s = it->second;
  return std::vector<uint8_t>(s.begin(), s.end());
}

//--- Build Attrs from typed data ---

inline Attrs ral_from_string(const std::string& val)
{
  return {{ DEFAULT_FIELD, val }};
}

inline Attrs ral_from_double(double val)
{
  std::string buf(sizeof(double), '\0');
  std::memcpy(buf.data(), &val, sizeof(double));
  return {{ DEFAULT_FIELD, std::move(buf) }};
}

inline Attrs ral_from_int(int64_t val)
{
  std::string buf(sizeof(int64_t), '\0');
  std::memcpy(buf.data(), &val, sizeof(int64_t));
  return {{ DEFAULT_FIELD, std::move(buf) }};
}

inline Attrs ral_from_blob(const void* data, size_t size)
{
  return {{ DEFAULT_FIELD, data ? std::string(static_cast<const char*>(data), size) : std::string() }};
}
