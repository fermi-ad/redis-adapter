#pragma once
#include "sw/redis++/redis++.h" // maybe pull this out at some point idk.
#include <memory>
#include <chrono>

inline uint64_t getTimeNanosTest()
{
    auto now = std::chrono::steady_clock::now();
    auto nanos = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
    return nanos.time_since_epoch().count();
}

struct RA_Time
{
  RA_Time(int64_t nanos = 0) : value(nanos) {}
  RA_Time(const std::string& id);

  bool ok() const { return value > 0; }

  operator int64_t()  const { return ok() ? value : 0; }
  operator uint64_t() const { return ok() ? value : 0; }

  uint32_t err() const { return ok() ? 0 : -value; }

  std::string id() const;
  std::string id_or_now() const;

  std::string id_or_min() const { return ok() ? id() : "-"; }
  std::string id_or_max() const { return ok() ? id() : "+"; }

  int64_t value;
};

struct RA_ArgsAdd
  { RA_Time time; uint32_t trim = 1; };

class MockRedisAdapter // Mock
{
public:
  using Attrs = std::unordered_map<std::string, std::string>;

  template<typename T> using TimeVal = std::pair<RA_Time, T>;         //  analagous to Item
  template<typename T> using TimeValList = std::vector<TimeVal<T>>;   //  analagous to ItemStream

  template<typename T>
  using ReaderSubFn = std::function<void(const std::string& baseKey, const std::string& subKey, const TimeValList<T>& data)>;

  MockRedisAdapter() { }

    //  overload for array and span
    int addSingleList_numCalls_array_and_span = 0;
    struct addSingleList_array_and_span_args {
      std::string subKey;
      std::shared_ptr<void> data; // copy both into a raw array and let the tester deal with the type themself
      int dataSize; // Size of data in bytes
      RA_ArgsAdd args;
    };
    std::vector<addSingleList_array_and_span_args> addSingleList_array_and_span_arguments;
    template<template<typename T, size_t S> class C, typename T, size_t S> RA_Time
    addSingleList(const std::string& subKey, const C<T, S>& data, const RA_ArgsAdd& args = {})
    {
      int dataSize = data.size_bytes();
      std::shared_ptr<void> ptr(
        new T[dataSize],                   // allocate
        [](void* p) { delete[] static_cast<T*>(p); } // deleter
      );
      T* raw = static_cast<T*>(ptr.get());
      std::copy(data.begin(), data.end(), raw);

      addSingleList_array_and_span_args arguments {
        .subKey = subKey,
        .data = ptr,
        .dataSize = dataSize,
        .args = args
      };
      addSingleList_array_and_span_arguments.push_back(arguments);

      addSingleList_numCalls_array_and_span++;
      return RA_Time(getTimeNanosTest());
    }
    //  overload for vector
    int addSingleList_numCalls_vector = 0;
    struct addSingleList_vector_args {
      std::string subKey;
      std::shared_ptr<void> data; // copy both into a raw array and let the tester deal with the type themself
      int dataSize; // Size of data in bytes
      RA_ArgsAdd args;
    };
    std::vector<addSingleList_vector_args> addSingleList_vector_arguments;
    template<typename T> RA_Time
    addSingleList(const std::string& subKey, const std::vector<T>& data, const RA_ArgsAdd& args = {})
    {
      int dataSize = data.size_bytes();
      std::shared_ptr<void> ptr(
        new T[dataSize],                   // allocate
        [](void* p) { delete[] static_cast<T*>(p); } // deleter
      );
      T* raw = static_cast<T*>(ptr.get());
      std::copy(data.begin(), data.end(), raw);

      addSingleList_vector_args arguments {
        .subKey = subKey,
        .data = ptr,
        .dataSize = dataSize,
        .args = args
      };
      addSingleList_vector_arguments.push_back(arguments);

      addSingleList_numCalls_vector++;
      return RA_Time(getTimeNanosTest());
    }

    int addSingleValue_numCalls = 0;
    struct addSingleValue_args {
      std::string subKey;
      std::shared_ptr<void> data; // copy both into a raw array and let the tester deal with the type themself
      RA_ArgsAdd args;
    };
    std::vector<addSingleValue_args> addSingleValue_arguments;
    template<typename T> RA_Time
    addSingleValue(const std::string& subKey, const T& data, const RA_ArgsAdd& args = {}) {
      std::shared_ptr<void> ptr(
        new T,                   // allocate
        [](void* p) { delete[] static_cast<T*>(p); } // deleter
      );
      T* raw = static_cast<T*>(ptr.get());
      std::copy(&data, &data+sizeof(data), raw);
      addSingleValue_args arguments {
        .subKey = subKey,
        .data = ptr,
        .args = args
      };
      addSingleValue_arguments.push_back(arguments);

      addSingleList_numCalls_vector++;
      return RA_Time(getTimeNanosTest());
    }
};