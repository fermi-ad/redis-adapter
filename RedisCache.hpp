#pragma once

#include <shared_mutex>
#include <map>
#include <unordered_map>
#include <variant>
#include <string>
#include <vector>
#include <array>
#include <span>
#include "RedisAdapter.hpp"

template<typename Type>
class RedisCache {
private:
    std::shared_ptr<RedisAdapter> _ra;
    // The implementation of this cache has a potential flaw where if we have multiple readers contantly reading, then we could potentally stop new data from ever being
    // written and as a side effect lock up the stream reader. If we ever actually use that we should think through implementing that sanely. Boost has an implementaion of queued
    // locking that would prevent this, but then we'd have to pull in boost or part of it, with redis adapter.
    mutable std::shared_mutex swapMutex; // Mutex that prevents swapping which buffer is for reading and writing.
                                         // This mutex allows for simultanious reads, but prevents reading while writing.

    int readIndex = 0;
    std::array<std::vector<Type>, 2> buffers;
    RA_Time lastWrite = 0;
    std::string _subkey;

    void writeBuffer(const RedisAdapter::TimeValList<std::vector<Type>>& entry)
    {
        const std::vector<Type>& data = entry.front().second;
        int writeIndex = (readIndex + 1 ) % 2;

        buffers.at(writeIndex) = data;

        {
            // take an exclusive lock, so a reader can't read, while the buffers are being swapped
            std::lock_guard<std::shared_mutex> swapLock(swapMutex);
            readIndex = (readIndex + 1 ) % 2;
            lastWrite = entry.front().first;
        }
    }
    void registerCacheReader() {
        //Setup redis setting readers
        _ra->addListsReader<Type>(_subkey, [this](const std::string& base, const std::string& sub, const RedisAdapter::TimeValList<std::vector<Type>>& entry) {
            writeBuffer(entry);
        });
    }

public:
    // Returns time that the last buffer was written

    //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    //  copyReadBuffer : Copies the data in the cache to destBuffer
    //
    //    destBuffer : Vector to copy the cached data to
    //    return     : time of the last data written to the redis stream, or 0 if there is data at that key
    //
    RA_Time copyReadBuffer(std::vector<Type>& destBuffer) {
        // Take a shared lock, so other readers can still read, but the writer cannot swap the buffers
        std::shared_lock<std::shared_mutex> swapLock(swapMutex);

        // If the cache has no data read in whatever's there to initilize the cache.
        if (!lastWrite.ok())
        {
            lastWrite = _ra->getSingleList(_subkey, buffers.at(readIndex));
        }

        std::vector<Type>& sourceBuffer = buffers.at(readIndex);
        // Copy internel buffer to user
        destBuffer = sourceBuffer;

        return lastWrite;
    }

    //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    //  copyReadBuffer : Copies the data in the cache to destValue or destBuffer
    //    destValue        : Scalar to copy the cached data to
    //    destBuffer       : Span, which can wrap many types, to copy the cached data to
    //    firstIndexToCopy : Fist index of the buffer to copy
    //    pElementsCopied  : Pointer to buffer where the number of elements copied into destBuffer is written.
    //                       This can be less than desired if the buffer is smaller than expected
    //    return           : time of the last data written to the redis stream, or 0 if there is data at that key
    RA_Time copyReadBuffer(Type& destValue, int firstIndexToCopy = 0, int* pElementsCopied = nullptr)
    {
        std::span<Type> destBuffer(&destValue, 1);
        return copyReadBuffer(destBuffer, firstIndexToCopy, pElementsCopied);
    }
    RA_Time copyReadBuffer(std::span<Type> destBuffer, int firstIndexToCopy = 0 , int* pElementsCopied = nullptr)
    {
        // Take a shared lock, so other readers can still read, but the writer cannot swap the buffers
        std::shared_lock<std::shared_mutex> swapLock(swapMutex);

        // If the cache has no data read in whatever's there to initilize the cache.
        if (!lastWrite.ok())
        {
            lastWrite = _ra->getSingleList(_subkey, buffers.at(readIndex));
        }

        std::vector<Type>& sourceBuffer = buffers.at(readIndex);

        typename std::vector<Type>::iterator copySourceStart = sourceBuffer.begin() + firstIndexToCopy;
        typename std::vector<Type>::iterator copySourceEnd   = copySourceStart + destBuffer.size();

        if (copySourceEnd > sourceBuffer.end())
            { copySourceEnd = sourceBuffer.end(); }

        auto elementAfterLastCopied = std::copy(copySourceStart, copySourceEnd, destBuffer.begin());
        if (pElementsCopied != nullptr)
            { *pElementsCopied = elementAfterLastCopied - destBuffer.begin(); }

        return lastWrite;
    }

    RedisCache(std::shared_ptr<RedisAdapter> ra, std::string subkey) { _ra = ra; _subkey = subkey; registerCacheReader(); }

    ~RedisCache() = default;

    // Disabling copy and move operations
    RedisCache(const RedisCache&) = delete;
    RedisCache& operator=(const RedisCache&) = delete;
    RedisCache(RedisCache&&) = delete;
    RedisCache& operator=(RedisCache&&) = delete;
};
