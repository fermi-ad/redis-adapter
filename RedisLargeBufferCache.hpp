//
//  RedisLargeBufferCache.hpp
//
//  Reuses hiredis's large buffers instead of returning them to the system after every write.
//
//  Every stream add makes hiredis build a payload-sized formatted command and append it to its
//  output buffer, which it frees after each complete write. Above glibc's mmap threshold each of
//  those is a fresh mapping that the kernel zero-fills a page at a time on first touch, so a
//  1 MB add faulted in about 4 MB per call. On 32-bit glibc the threshold can't be raised past
//  512 KB, so allocator tuning can't fix it there. Keeping a few large blocks for reuse can.
//
//  The cache is installed as hiredis's allocator (hiredisSetAllocators). It keeps no header in
//  front of blocks, so pointers hiredis allocated before installation are still freed correctly:
//  small requests go straight to malloc, and only blocks the cache itself handed out are tracked.
//  Don't combine it with other custom hiredis allocators.
//

#pragma once

#include <hiredis/alloc.h>
#include <malloc.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>
#include <unordered_map>

namespace redis_adapter_detail {

class LargeBufferCache
{
public:
  //  Requests below this go straight to malloc. Blocks handed out at or above it are tracked.
  static constexpr size_t MIN_LARGE = 64 * 1024;

  //  maxCached : the most bytes of free blocks kept for reuse; anything beyond is freed
  explicit LargeBufferCache(size_t maxCached = 16u << 20) : _max_cached(maxCached) {}

  ~LargeBufferCache()
  {
    for (auto& item : _cached) std::free(item.second);
  }

  LargeBufferCache(const LargeBufferCache&) = delete;
  LargeBufferCache& operator=(const LargeBufferCache&) = delete;

  void* allocate(size_t size)
  {
    if (size < MIN_LARGE) return std::malloc(size);
    std::lock_guard<std::mutex> lk(_mtx);
    auto it = _cached.lower_bound(size);                //  smallest cached block that fits
    if (it != _cached.end())
    {
      void* block = it->second;
      _cached_bytes -= it->first;
      _live.emplace(block, it->first);
      _cached.erase(it);
      return block;
    }
    const size_t capacity = capacityFor(size);
    if (capacity == 0) return nullptr;
    void* block = std::malloc(capacity);
    if (block) _live.emplace(block, capacity);
    return block;
  }

  void* allocateZeroed(size_t count, size_t size)
  {
    if (size && count > std::numeric_limits<size_t>::max() / size) return nullptr;
    void* block = allocate(count * size);
    if (block) std::memset(block, 0, count * size);
    return block;
  }

  void* reallocate(void* ptr, size_t size)
  {
    if ( ! ptr) return allocate(size);
    size_t capacity = 0;
    if (mayBeTracked(ptr))
    {
      std::lock_guard<std::mutex> lk(_mtx);
      auto it = _live.find(ptr);
      if (it != _live.end()) capacity = it->second;
    }
    if (capacity)
    {
      if (size <= capacity) return ptr;                 //  still fits: same block, nothing copied
      void* block = allocate(size);
      if ( ! block) return nullptr;                     //  realloc semantics: ptr is unchanged
      std::memcpy(block, ptr, capacity);
      release(ptr);
      return block;
    }
    //  ptr came from malloc (a small request, or allocated before the cache was installed)
    if (size < MIN_LARGE) return std::realloc(ptr, size);
    void* block = allocate(size);
    if ( ! block) return nullptr;
    std::memcpy(block, ptr, std::min(malloc_usable_size(ptr), size));
    std::free(ptr);
    return block;
  }

  void release(void* ptr)
  {
    if ( ! ptr) return;
    if (mayBeTracked(ptr))
    {
      std::lock_guard<std::mutex> lk(_mtx);
      auto it = _live.find(ptr);
      if (it != _live.end())
      {
        const size_t capacity = it->second;
        _live.erase(it);
        if (_cached_bytes + capacity <= _max_cached)
        {
          _cached.emplace(capacity, ptr);
          _cached_bytes += capacity;
          return;
        }
      }
    }
    std::free(ptr);
  }

  char* duplicate(const char* str)
  {
    const size_t size = std::strlen(str) + 1;
    char* copy = static_cast<char*>(allocate(size));
    if (copy) std::memcpy(copy, str, size);
    return copy;
  }

  //  for tests
  size_t cachedBytes() const { std::lock_guard<std::mutex> lk(_mtx); return _cached_bytes; }
  size_t cachedBlocks() const { std::lock_guard<std::mutex> lk(_mtx); return _cached.size(); }
  size_t liveBlocks() const { std::lock_guard<std::mutex> lk(_mtx); return _live.size(); }

  //  capacity handed out for a request: the next power of two, at least MIN_LARGE (0 on overflow)
  static size_t capacityFor(size_t size)
  {
    size_t capacity = MIN_LARGE;
    while (capacity < size)
    {
      if (capacity > std::numeric_limits<size_t>::max() / 2) return 0;
      capacity *= 2;
    }
    return capacity;
  }

private:
  //  Every tracked block has a usable size of at least MIN_LARGE, so small frees skip the lock.
  static bool mayBeTracked(void* ptr) { return malloc_usable_size(ptr) >= MIN_LARGE; }

  mutable std::mutex _mtx;
  std::unordered_map<void*, size_t> _live;              //  blocks handed out -> capacity
  std::multimap<size_t, void*> _cached;                 //  free blocks by capacity
  size_t _cached_bytes = 0;
  const size_t _max_cached;
};

//  The process-wide cache hiredis uses. Never destroyed, so hiredis can still free through it
//  while other static objects are being torn down at exit.
inline LargeBufferCache& hiredisBufferCache()
{
  static LargeBufferCache* cache = new LargeBufferCache();
  return *cache;
}

//  Make hiredis allocate through hiredisBufferCache(). Safe to call more than once.
inline void installHiredisBufferCache()
{
  static std::once_flag once;
  std::call_once(once, []
  {
    hiredisAllocFuncs funcs{};
    funcs.mallocFn = [](size_t size) { return hiredisBufferCache().allocate(size); };
    funcs.callocFn = [](size_t count, size_t size) { return hiredisBufferCache().allocateZeroed(count, size); };
    funcs.reallocFn = [](void* ptr, size_t size) { return hiredisBufferCache().reallocate(ptr, size); };
    funcs.strdupFn = [](const char* str) { return hiredisBufferCache().duplicate(str); };
    funcs.freeFn = [](void* ptr) { hiredisBufferCache().release(ptr); };
    hiredisSetAllocators(&funcs);
  });
}

} // namespace redis_adapter_detail
