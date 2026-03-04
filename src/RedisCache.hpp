//
//  RedisCache.hpp
//
//  Double-buffered cache for RedisAdapterLite stream data.
//  Keeps a template parameter for the element type since this is
//  purely client-side convenience (no Redis-layer templates).
//

#pragma once

#include "RedisAdapterLite.hpp"
#include <shared_mutex>
#include <array>
#include <atomic>
#include <chrono>
#include <thread>
#include <cstring>

template<typename Type>
class RedisCache
{
public:
  RedisCache(std::shared_ptr<RedisAdapterLite> ra, std::string subkey)
    : _ra(ra), _subkey(subkey)
  {
    registerCacheReader();
  }

  ~RedisCache() = default;

  RedisCache(const RedisCache&) = delete;
  RedisCache& operator=(const RedisCache&) = delete;
  RedisCache(RedisCache&&) = delete;
  RedisCache& operator=(RedisCache&&) = delete;

  RAL_Time copyReadBuffer(std::vector<Type>& destBuffer)
  {
    std::shared_lock<std::shared_mutex> lk(_swapMutex);

    if (!_lastWrite.ok())
    {
      std::vector<uint8_t> blob;
      _lastWrite = _ra->getBlob(_subkey, blob);
      if (blob.size() >= sizeof(Type))
      {
        size_t count = blob.size() / sizeof(Type);
        _buffers.at(_readIndex).resize(count);
        std::memcpy(_buffers.at(_readIndex).data(), blob.data(), count * sizeof(Type));
      }
    }

    destBuffer = _buffers.at(_readIndex);
    return _lastWrite;
  }

  RAL_Time copyReadBuffer(Type& destValue, int firstIndexToCopy = 0, int* pElementsCopied = nullptr)
  {
    std::shared_lock<std::shared_mutex> lk(_swapMutex);

    if (!_lastWrite.ok())
    {
      std::vector<uint8_t> blob;
      _lastWrite = _ra->getBlob(_subkey, blob);
      if (blob.size() >= sizeof(Type))
      {
        size_t count = blob.size() / sizeof(Type);
        _buffers.at(_readIndex).resize(count);
        std::memcpy(_buffers.at(_readIndex).data(), blob.data(), count * sizeof(Type));
      }
    }

    auto& src = _buffers.at(_readIndex);
    if (firstIndexToCopy >= static_cast<int>(src.size()))
    {
      if (pElementsCopied) *pElementsCopied = 0;
      return RAL_Time();
    }

    destValue = src[firstIndexToCopy];
    if (pElementsCopied) *pElementsCopied = 1;
    return _lastWrite;
  }

  void waitForNewValue()
  {
    waitForNewValue(std::chrono::milliseconds(1));
  }

  template <typename Rep, typename Period>
  void waitForNewValue(std::chrono::duration<Rep, Period> timeBetweenChecks)
  {
    while (!_newValueAvailable.load())
      std::this_thread::sleep_for(timeBetweenChecks);
    _newValueAvailable.store(false);
  }

  bool newValueAvailable() { return _newValueAvailable.load(); }
  void clearNewValueAvailable() { _newValueAvailable.store(false); }

private:
  void registerCacheReader()
  {
    _ra->addReader(_subkey,
      [this](const std::string&, const std::string&, const TimeAttrsList& entries)
      {
        if (entries.empty()) return;

        auto blob = ral_to_blob(entries.front().second);
        if (!blob || blob->size() < sizeof(Type)) return;

        size_t count = blob->size() / sizeof(Type);
        int writeIndex = (_readIndex + 1) % 2;

        _buffers.at(writeIndex).resize(count);
        std::memcpy(_buffers.at(writeIndex).data(), blob->data(), count * sizeof(Type));

        {
          std::lock_guard<std::shared_mutex> swapLock(_swapMutex);
          _readIndex = writeIndex;
          _lastWrite = entries.front().first;
        }
        _newValueAvailable.store(true);
      }
    );
  }

  std::shared_ptr<RedisAdapterLite> _ra;
  std::string _subkey;

  std::atomic<bool> _newValueAvailable{false};
  mutable std::shared_mutex _swapMutex;
  int _readIndex = 0;
  std::array<std::vector<Type>, 2> _buffers;
  RAL_Time _lastWrite = 0;
};
