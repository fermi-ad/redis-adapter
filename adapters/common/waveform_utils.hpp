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
