#pragma once

#include <mutex>
#include <map>
#include <unordered_map>
#include <variant>
#include <string>
#include <vector>
#include "RedisAdapter.hpp"

template<typename Type>
class RedisCache {
private:
    std::shared_ptr<RedisAdapter> _ra;
    std::mutex swapMutex; // Mutex That prevents swapping which buffer is for reading and writing.

    int readIndex = 0;
    std::vector<Type> buffers[2];
    uint64_t lastWrite = 0;
    std::string _subkey;

public:
    // Returns time that the last buffer was written or zero if one has never been written.
    uint64_t copyReadBuffer(std::vector<Type>& destBuffer) {
        std::lock_guard<std::mutex> swapLock(swapMutex);

        // If the cache has no data read in whatever's there to initilize the cache.
        if (lastWrite == 0) 
        {
            lastWrite = _ra->getStreamListSingle(_subkey, buffers[readIndex]);
        }

        std::vector<Type>& sourceBuffer = buffers[readIndex];
        // Copy internel buffer to user
        destBuffer = sourceBuffer;

        return lastWrite;
    }
    void writeBuffer(const RedisAdapter::TimeValList<std::vector<Type>>& entry)
    {
        const std::vector<Type>& data = entry.front().second;
        int writeIndex = (readIndex + 1 ) % 2;

        buffers[writeIndex] = data;

        {
            std::lock_guard<std::mutex> swapLock(swapMutex);
            readIndex = (readIndex + 1 ) % 2;
            lastWrite = entry.front().first;
        }
    }

    RedisCache(std::shared_ptr<RedisAdapter> ra, std::string subkey) { _ra = ra; _subkey = subkey; registerCacheReader();}

   void registerCacheReader() {
        //Setup redis setting readers
        _ra->addStreamListReader<Type>(_subkey, [this](const std::string& base, const std::string& sub, const RedisAdapter::TimeValList<std::vector<Type>>& entry) {
            writeBuffer(entry);
        });
    }

    ~RedisCache() = default;

    // Disabling copy and move operations
    RedisCache(const RedisCache&) = delete;
    RedisCache& operator=(const RedisCache&) = delete;
    RedisCache(RedisCache&&) = delete;
    RedisCache& operator=(RedisCache&&) = delete;
};