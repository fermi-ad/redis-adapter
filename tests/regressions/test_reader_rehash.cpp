//
//  test_reader_rehash.cpp
//
//  Bug: Reader threads captured a reference to ReaderInfo obtained via
//  _readers.at(token). If a new addReader() call inserted a different
//  token into the _readers map, std::unordered_map could rehash,
//  invalidating the running thread's reference (use-after-free).
//
//  Fix: Changed _readers to store unique_ptr<ReaderInfo> so the
//  heap-allocated ReaderInfo objects are stable across rehashes.
//  Reader threads now capture a raw pointer to the stable object.
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <atomic>
#include <thread>
#include <chrono>
#include <string>

using namespace std;
using namespace chrono;

// Add readers on multiple distinct streams with readers > 1
// so different streams hash to different tokens, forcing map growth.
// If the old code (map of ReaderInfo by value) was used, a rehash
// during insertion could invalidate a running reader's reference.
TEST(ReaderRehash, MultipleTokensNoCrash)
{
  RAL_Options opts;
  opts.readers = 4;  // Multiple reader threads = multiple map entries
  opts.timeout = 200;

  RedisAdapterLite redis("TEST", opts);
  if (!redis.connected()) GTEST_SKIP() << "Redis not available";

  atomic<int> callbacks{0};

  // Add readers on enough different keys to likely span multiple tokens
  for (int i = 0; i < 8; i++)
  {
    string key = "t_rehash_" + to_string(i);
    redis.addReader(key,
      [&](const string&, const string&, const TimeAttrsList&) {
        callbacks++;
      });
  }

  // Let readers settle
  this_thread::sleep_for(milliseconds(100));

  // Write data to trigger callbacks
  for (int i = 0; i < 8; i++)
  {
    string key = "t_rehash_" + to_string(i);
    redis.addString(key, "data");
  }

  // Wait for some callbacks
  for (int i = 0; i < 50 && callbacks < 4; i++)
    this_thread::sleep_for(milliseconds(10));

  EXPECT_GE(callbacks.load(), 1);

  // Remove all readers and clean up
  for (int i = 0; i < 8; i++)
  {
    string key = "t_rehash_" + to_string(i);
    redis.removeReader(key);
    redis.del(key);
  }
}

// Add and remove readers in rapid succession with multiple tokens
TEST(ReaderRehash, RapidAddRemoveMultipleTokens)
{
  RAL_Options opts;
  opts.readers = 4;
  opts.timeout = 200;

  RedisAdapterLite redis("TEST", opts);
  if (!redis.connected()) GTEST_SKIP() << "Redis not available";

  for (int round = 0; round < 3; round++)
  {
    for (int i = 0; i < 6; i++)
    {
      string key = "t_rehash_rapid_" + to_string(i);
      redis.addReader(key,
        [](const string&, const string&, const TimeAttrsList&) {});
    }
    this_thread::sleep_for(milliseconds(20));
    for (int i = 0; i < 6; i++)
    {
      string key = "t_rehash_rapid_" + to_string(i);
      redis.removeReader(key);
    }
  }

  // No crash = success
  SUCCEED();
}
