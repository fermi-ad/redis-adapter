#pragma once
//
//  Shared waveform utilities for secondary adapters
//
//  - DataType enum + parsing
//  - Type-dispatched deserialization (blob → vector<double>)
//  - Type-dispatched serialization (vector<double> → blob via addBlob)
//  - Common signal handling and idle loop
//

#include "RedisAdapterLite.hpp"
#include "RAL_Helpers.hpp"

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

// ---- DataType ----

enum class DataType { Float32, Float64, Int32, Uint16 };

inline DataType parseDataType(const std::string& s)
{
    if (s == "float64") return DataType::Float64;
    if (s == "int32")   return DataType::Int32;
    if (s == "uint16")  return DataType::Uint16;
    return DataType::Float32;
}

inline const char* dataTypeName(DataType dt)
{
    switch (dt)
    {
    case DataType::Float64: return "float64";
    case DataType::Int32:   return "int32";
    case DataType::Uint16:  return "uint16";
    default:                return "float32";
    }
}

// ---- Deserialization: blob → vector<double> ----

inline std::vector<double> deserializeWaveform(
    const std::vector<uint8_t>& blob, DataType dt)
{
    std::vector<double> out;

    switch (dt)
    {
    case DataType::Float32:
    {
        size_t n = blob.size() / sizeof(float);
        out.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            float v;
            std::memcpy(&v, blob.data() + i * sizeof(float), sizeof(float));
            out[i] = static_cast<double>(v);
        }
        break;
    }
    case DataType::Float64:
    {
        size_t n = blob.size() / sizeof(double);
        out.resize(n);
        if (n > 0)
            std::memcpy(out.data(), blob.data(), n * sizeof(double));
        break;
    }
    case DataType::Int32:
    {
        size_t n = blob.size() / sizeof(int32_t);
        out.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            int32_t v;
            std::memcpy(&v, blob.data() + i * sizeof(int32_t), sizeof(int32_t));
            out[i] = static_cast<double>(v);
        }
        break;
    }
    case DataType::Uint16:
    {
        size_t n = blob.size() / sizeof(uint16_t);
        out.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            uint16_t v;
            std::memcpy(&v, blob.data() + i * sizeof(uint16_t), sizeof(uint16_t));
            out[i] = static_cast<double>(v);
        }
        break;
    }
    }

    return out;
}

// ---- WriteBatch: accumulate serialized writes, flush via pipeline ----
//
//  Reuses a shared float buffer for float32 serialization and pipelines
//  all XADD commands in a single Redis round-trip.

class WriteBatch
{
public:
  void add(const std::string& subKey, const std::vector<double>& data,
           DataType dt, RAL_Time sourceTime = RAL_Time())
  {
    Entry e;
    e.subKey = subKey;
    e.args.time = sourceTime;

    switch (dt)
    {
    case DataType::Float32:
    {
      size_t n = data.size();
      if (_floatBuf.size() < n) _floatBuf.resize(n);
      for (size_t i = 0; i < n; ++i)
        _floatBuf[i] = static_cast<float>(data[i]);
      e.serialized.resize(n * sizeof(float));
      std::memcpy(e.serialized.data(), _floatBuf.data(), e.serialized.size());
      break;
    }
    case DataType::Float64:
      e.serialized.resize(data.size() * sizeof(double));
      std::memcpy(e.serialized.data(), data.data(), e.serialized.size());
      break;
    case DataType::Int32:
    {
      size_t n = data.size();
      e.serialized.resize(n * sizeof(int32_t));
      auto* dst = reinterpret_cast<int32_t*>(e.serialized.data());
      for (size_t i = 0; i < n; ++i)
        dst[i] = static_cast<int32_t>(std::round(data[i]));
      break;
    }
    case DataType::Uint16:
    {
      size_t n = data.size();
      e.serialized.resize(n * sizeof(uint16_t));
      auto* dst = reinterpret_cast<uint16_t*>(e.serialized.data());
      for (size_t i = 0; i < n; ++i)
      {
        double v = std::round(data[i]);
        if (v < 0.0)     v = 0.0;
        if (v > 65535.0)  v = 65535.0;
        dst[i] = static_cast<uint16_t>(v);
      }
      break;
    }
    }

    _entries.push_back(std::move(e));
  }

  void addDouble(const std::string& subKey, double value,
                 RAL_Time sourceTime = RAL_Time())
  {
    Entry e;
    e.subKey = subKey;
    e.args.time = sourceTime;
    e.serialized.resize(sizeof(double));
    std::memcpy(e.serialized.data(), &value, sizeof(double));
    _entries.push_back(std::move(e));
  }

  std::vector<RAL_Time> flush(RedisAdapterLite& redis)
  {
    if (_entries.empty()) return {};

    std::vector<RedisAdapterLite::BlobEntry> batch;
    batch.reserve(_entries.size());
    for (auto& e : _entries)
    {
      batch.push_back({
        e.subKey,
        e.serialized.data(),
        e.serialized.size(),
        e.args
      });
    }
    auto result = redis.addBlobBatch(batch);
    _entries.clear();
    return result;
  }

  void clear() { _entries.clear(); }
  size_t size() const { return _entries.size(); }

private:
  struct Entry {
    std::string subKey;
    std::vector<char> serialized;
    RAL_AddArgs args;
  };
  std::vector<Entry> _entries;
  std::vector<float> _floatBuf;  // shared serialization buffer for float32
};

// ---- Serialization: vector<double> → addBlob ----
//
//  Both overloads write with the same stream field ("_").
//  The timestamped overload preserves the source trigger's timestamp
//  so all derived data correlates back to the original acquisition.

inline void serializeAndWrite(
    RedisAdapterLite& redis,
    const std::string& key,
    const std::vector<double>& data,
    DataType dt,
    RAL_Time sourceTime = RAL_Time())
{
    RAL_AddArgs args;
    args.time = sourceTime;

    switch (dt)
    {
    case DataType::Float32:
    {
        std::vector<float> buf(data.size());
        for (size_t i = 0; i < data.size(); ++i)
            buf[i] = static_cast<float>(data[i]);
        redis.addBlob(key, buf.data(), buf.size() * sizeof(float), args);
        break;
    }
    case DataType::Float64:
    {
        redis.addBlob(key, data.data(), data.size() * sizeof(double), args);
        break;
    }
    case DataType::Int32:
    {
        std::vector<int32_t> buf(data.size());
        for (size_t i = 0; i < data.size(); ++i)
            buf[i] = static_cast<int32_t>(std::round(data[i]));
        redis.addBlob(key, buf.data(), buf.size() * sizeof(int32_t), args);
        break;
    }
    case DataType::Uint16:
    {
        std::vector<uint16_t> buf(data.size());
        for (size_t i = 0; i < data.size(); ++i)
        {
            double v = std::round(data[i]);
            if (v < 0.0)        v = 0.0;
            if (v > 65535.0)    v = 65535.0;
            buf[i] = static_cast<uint16_t>(v);
        }
        redis.addBlob(key, buf.data(), buf.size() * sizeof(uint16_t), args);
        break;
    }
    }
}
