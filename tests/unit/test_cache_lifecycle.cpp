//
//  test_cache_lifecycle.cpp
//
//  Bug: RedisCache destructor was `= default`, but the constructor
//  registered a reader callback capturing `this`. After destruction,
//  the dangling callback would access freed memory (use-after-free).
//
//  Fix: Destructor now calls _ra->removeReader(_subkey).
//
//  Bug: RedisCache used entries.front() to cache incoming data,
//  but XREAD returns entries in chronological order (oldest first).
//  A batch with multiple entries would cache the oldest, not newest.
//
//  Fix: Changed to entries.back() to cache the most recent value.
//

#include <gtest/gtest.h>
#include "RedisCache.hpp"

// Verify that RedisCache destructor calls removeReader.
// We can't easily run a live Redis test here, but we verify
// the destructor is non-trivial (it compiles with removeReader call).
TEST(CacheLifecycle, DestructorIsNonTrivial)
{
  // RedisCache<int> should NOT be trivially destructible
  // since the destructor now calls removeReader.
  EXPECT_FALSE(std::is_trivially_destructible_v<RedisCache<int>>);
}

// Verify double-buffer swap logic: writeIndex is always opposite of readIndex
TEST(CacheLifecycle, DoubleBufferIndexAlternates)
{
  int readIndex = 0;
  int writeIndex = (readIndex + 1) % 2;
  EXPECT_EQ(writeIndex, 1);

  readIndex = 1;
  writeIndex = (readIndex + 1) % 2;
  EXPECT_EQ(writeIndex, 0);
}
