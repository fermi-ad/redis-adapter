#pragma once
#include "../RedisStreamData.hpp"
#include "../RedisTime.hpp"
#include "../RedisReaderMode.hpp"
#include <algorithm>
#include <memory>
#include <chrono>
#include <deque>
#include <string>
#include <vector>

inline uint64_t getTimeNanosTest()
{
    auto now = std::chrono::steady_clock::now();
    auto nanos = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
    return nanos.time_since_epoch().count();
}

struct RA_ArgsAdd
  { RA_Time time; uint32_t trim = 1; bool approximateTrim = true; };

class MockRedisAdapter : public RedisStreamData // Mock
{
public:
  using ReaderFallbackReason = RA_ReaderFallbackReason;
  using ReaderBackendStatus = RA_ReaderBackendStatus;
  [[nodiscard]] ReaderBackendStatus readerBackendStatus() const { return {}; }
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
      size_t dataSize; // Size of data in bytes
      RA_ArgsAdd args;
    };
    std::vector<addSingleList_array_and_span_args> addSingleList_array_and_span_arguments;
    template<template<typename T, size_t S> class C, typename T, size_t S> RA_Time
    addSingleList(const std::string& subKey, const C<T, S>& data, const RA_ArgsAdd& args = {})
    {
      size_t dataSize = data.size() * sizeof(T);
      std::shared_ptr<void> ptr(
        new T[data.size()],                   // allocate
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
      size_t dataSize; // Size of data in bytes
      RA_ArgsAdd args;
    };
    std::vector<addSingleList_vector_args> addSingleList_vector_arguments;
    template<typename T> RA_Time
    addSingleList(const std::string& subKey, const std::vector<T>& data, const RA_ArgsAdd& args = {})
    {
      size_t dataSize = data.size() * sizeof(T);
      std::shared_ptr<void> ptr(
        new T[data.size()],                   // allocate
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
    addSingleValue(const std::string& subKey, const T& data, const RA_ArgsAdd& args = {})
    {
      std::shared_ptr<void> ptr(
        new T(data),                       // allocate
        [](void* p) { delete static_cast<T*>(p); } // deleter
      );

      addSingleValue_args arguments {
        .subKey = subKey,
        .data = ptr,
        .args = args
      };
      addSingleValue_arguments.push_back(arguments);

      addSingleValue_numCalls++;
      return RA_Time(getTimeNanosTest());
    }

    //  Pipeline mock: records every queued write in queue order and returns scripted results
    struct PipelineWrite {
      std::string subKey;
      std::string data;     // payload bytes (empty for Attrs writes)
      Attrs attrs;          // Attrs writes only
      RA_ArgsAdd args;
      bool view = false;    // queued with addSingleListView
      size_t exec = 0;      // which exec() sent it, counting from 0
    };
    std::vector<PipelineWrite> pipelineWrites;  // every write queued, in queue order
    std::vector<size_t> pipelineExecSizes;      // number of writes each exec() sent
    //  Results for the next writes, consumed in send order (for example RA_REJECTED, or
    //  RA_NOT_CONNECTED for writes after a lost reply). Once empty, writes are accepted and
    //  report args.time when it is set, otherwise a generated time.
    std::deque<RA_Time> pipelineResults;

    class Pipeline
    {
    public:
      //  overloads for array and span, vector, and pointer and element count
      template<template<typename T, size_t S> class C, typename T, size_t S> size_t
      addSingleListView(const std::string& subKey, const C<T, S>& data, const RA_ArgsAdd& args = {})
        { return queue(subKey, data.data(), data.size() * sizeof(T), args, true); }
      template<typename T> size_t
      addSingleListView(const std::string& subKey, const std::vector<T>& data, const RA_ArgsAdd& args = {})
        { return queue(subKey, data.data(), data.size() * sizeof(T), args, true); }
      template<typename T> size_t
      addSingleListView(const std::string& subKey, const T* data, size_t size, const RA_ArgsAdd& args = {})
        { return queue(subKey, data, size * sizeof(T), args, true); }

      template<template<typename T, size_t S> class C, typename T, size_t S> size_t
      addSingleList(const std::string& subKey, const C<T, S>& data, const RA_ArgsAdd& args = {})
        { return queue(subKey, data.data(), data.size() * sizeof(T), args, false); }
      template<typename T> size_t
      addSingleList(const std::string& subKey, const std::vector<T>& data, const RA_ArgsAdd& args = {})
        { return queue(subKey, data.data(), data.size() * sizeof(T), args, false); }

      template<typename T> size_t
      addSingleValue(const std::string& subKey, const T& data, const RA_ArgsAdd& args = {})
        { return queue(subKey, &data, sizeof(T), args, false); }
      size_t addSingleValue(const std::string& subKey, const Attrs& data, const RA_ArgsAdd& args = {})
      {
        const size_t index = queue(subKey, nullptr, 0, args, false);
        _mock.pipelineWrites.back().attrs = data;
        return index;
      }

      std::vector<std::vector<RA_Time>> exec()
      {
        std::vector<std::vector<RA_Time>> results;
        const size_t exec = _mock.pipelineExecSizes.size();
        for (const size_t write : _queued)
        {
          auto& queued = _mock.pipelineWrites[write];
          queued.exec = exec;
          RA_Time result = queued.args.time.ok() ? queued.args.time : RA_Time(getTimeNanosTest());
          if ( ! _mock.pipelineResults.empty())
          {
            result = _mock.pipelineResults.front();
            _mock.pipelineResults.pop_front();
          }
          results.push_back({result});
        }
        _mock.pipelineExecSizes.push_back(_queued.size());
        _queued.clear();
        return results;
      }

    private:
      friend class MockRedisAdapter;
      explicit Pipeline(MockRedisAdapter& mock) : _mock(mock) {}

      size_t queue(const std::string& subKey, const void* data, size_t bytes, const RA_ArgsAdd& args, bool view)
      {
        PipelineWrite write;
        write.subKey = subKey;
        if (bytes) write.data.assign(static_cast<const char*>(data), bytes);
        write.args = args;
        write.view = view;
        _queued.push_back(_mock.pipelineWrites.size());
        _mock.pipelineWrites.push_back(std::move(write));
        return _queued.size() - 1;
      }

      MockRedisAdapter& _mock;
      std::vector<size_t> _queued;
    };

    Pipeline pipeline() { return Pipeline(*this); }
};
