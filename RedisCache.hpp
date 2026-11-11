#pragma once

#include <shared_mutex>
#include <map>
#include <unordered_map>
#include <variant>
#include <string>
#include <vector>
#include <array>
#include <chrono>
#include "spanCpp14.hpp"
#include <semaphore>
#include "RedisAdapter.hpp"

template<typename Type>
class RedisCache {
private:
    struct State {
        std::atomic<bool> newValueAvailable{false};
        // Concurrent readers can delay the exclusive buffer swap.
        mutable std::shared_mutex swapMutex;
        int readIndex = 0;
        std::array<std::vector<Type>, 2> buffers;
        RA_Time lastWrite = 0;
    };

    std::shared_ptr<RedisAdapter> _ra;
    std::string _subkey;
    std::shared_ptr<State> _state = std::make_shared<State>();
    // Retire this registration before releasing the cache's state or adapter.
    // A callback already executing retains State independently of this object.
    RedisAdapter::ReaderHandle _reader;

    static void writeBuffer(const std::shared_ptr<State>& state,
                            const RedisAdapter::TimeValList<std::vector<Type>>& entry)
    {
        if (entry.empty()) return;
        // A reader callback can contain several retained updates. This cache
        // represents the latest value, including the corresponding timestamp.
        const auto& latest = entry.back();
        const std::vector<Type>& data = latest.second;
        int writeIndex = (state->readIndex + 1 ) % 2;

        state->buffers.at(writeIndex) = data;

        {
            // take an exclusive lock, so a reader can't read, while the buffers are being swapped
            std::lock_guard<std::shared_mutex> swapLock(state->swapMutex);
            state->readIndex = (state->readIndex + 1 ) % 2;
            state->lastWrite = latest.first;
        }
        // Effectivly does nothing if the code using this class doesn't ever try to read this.
        // Set the atomic flag to indicate that new data is available
        state->newValueAvailable.store(true);
    }
    void registerCacheReader() {
        _reader = _ra->subscribeListsReader<Type>(_subkey,
            [state = _state](const std::string&, const std::string&,
                             const RedisAdapter::TimeValList<std::vector<Type>>& entry) {
                writeBuffer(state, entry);
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
        std::shared_lock<std::shared_mutex> swapLock(_state->swapMutex);

        // If the cache has no data read in whatever's there to initilize the cache.
        if (!_state->lastWrite.ok())
        {
            _state->lastWrite = _ra->getSingleList(_subkey, _state->buffers.at(_state->readIndex));
        }

        std::vector<Type>& sourceBuffer = _state->buffers.at(_state->readIndex);
        // Copy internel buffer to user
        destBuffer = sourceBuffer;

        return _state->lastWrite;
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
        std::shared_lock<std::shared_mutex> swapLock(_state->swapMutex);

        // If the cache has no data read in whatever's there to initilize the cache.
        if (!_state->lastWrite.ok())
        {
            _state->lastWrite = _ra->getSingleList(_subkey, _state->buffers.at(_state->readIndex));
        }

        std::vector<Type>& sourceBuffer = _state->buffers.at(_state->readIndex);

        typename std::vector<Type>::iterator copySourceStart = sourceBuffer.begin() + firstIndexToCopy;
        typename std::vector<Type>::iterator copySourceEnd   = copySourceStart + destBuffer.size();

        if (copySourceEnd > sourceBuffer.end())
            { copySourceEnd = sourceBuffer.end(); }

        if (copySourceStart >= sourceBuffer.end()) {
           *pElementsCopied = 0;
           return RA_Time(); // Return an invalid time, and don't copy anything
        }

        auto elementAfterLastCopied = std::copy(copySourceStart, copySourceEnd, destBuffer.begin());
        if (pElementsCopied != nullptr)
            { *pElementsCopied = elementAfterLastCopied - destBuffer.begin(); }

        return _state->lastWrite;
    }
    void waitForNewValueMilis(int millisecondsBeweenChecks) {
        waitForNewValue(std::chrono::milliseconds(millisecondsBeweenChecks));
    }

    // Take care of the template for the default case
    void waitForNewValue() {
        waitForNewValue(std::chrono::milliseconds(1)); // Call template with default argument
    }
    template <typename Rep, typename Period>
    void waitForNewValue(std::chrono::duration<Rep, Period> timeBetweenChecks) {
        // Sleep until the new-value flag is raised
        while (!_state->newValueAvailable.load()) {
            std::this_thread::sleep_for(timeBetweenChecks); // Sleep for a short duration
        }
        // After waking up, lower the flag
        _state->newValueAvailable.store(false);
    }
    bool newValueAvaliable() {
        return _state->newValueAvailable.load();
    }
    void clearNewValueAvaliable() {
        _state->newValueAvailable.store(false);
    }

    RedisCache(std::shared_ptr<RedisAdapter> ra, std::string subkey) { _ra = ra; _subkey = subkey; registerCacheReader(); }

    ~RedisCache() = default;

    // Disabling copy and move operations
    RedisCache(const RedisCache&) = delete;
    RedisCache& operator=(const RedisCache&) = delete;
    RedisCache(RedisCache&&) = delete;
    RedisCache& operator=(RedisCache&&) = delete;
};
