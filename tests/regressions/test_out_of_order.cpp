//
//  test_out_of_order.cpp
//
//  Regression: Pushing entries with out-of-order (decreasing) timestamps
//  to a Redis stream causes XADD to return an error. The adapter must
//  handle this gracefully — no crash, no false reconnection, no tcache
//  corruption from use-after-free in reconnect/reader restart paths.
//
//  Bug: add_entry, add_entry_single, and pipeline_add all called
//  check_reconnect(0) when XADD failed, interpreting the empty result
//  as a connection failure. This triggered unnecessary reconnection
//  threads that stopped/restarted all readers — potentially causing
//  use-after-free and tcache corruption under load.
//
//  Fix: Changed to check_reconnect(_redis.is_connected() ? 1 : 0)
//  so only actual connection failures trigger reconnection.
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <chrono>
#include <thread>
#include <atomic>

using namespace std;
using namespace chrono;

static RedisAdapterLite& redis()
{
  static RedisAdapterLite r("TEST");
  return r;
}

// =========================================================================
//  Single add with stale timestamp
// =========================================================================

TEST(OutOfOrder, SingleAddStaleTimestamp)
{
  redis().del("t_ooo_single");

  // Add first entry — gets a recent timestamp
  auto t1 = redis().addDouble("t_ooo_single", 1.0, {.trim = 0});
  ASSERT_TRUE(t1.ok());

  // Add second entry with a timestamp far in the past
  RAL_Time old_time(1000000LL);  // ~1ms after epoch
  auto t2 = redis().addDouble("t_ooo_single", 2.0, {.time = old_time, .trim = 0});

  // Should fail gracefully — not ok, not RAL_NOT_CONNECTED
  EXPECT_FALSE(t2.ok());
  EXPECT_NE(t2.value, RAL_NOT_CONNECTED.value);

  // Connection should still be alive — no false reconnect
  EXPECT_TRUE(redis().connected());

  // Original data should be intact
  double val;
  auto t3 = redis().getDouble("t_ooo_single", val);
  EXPECT_TRUE(t3.ok());
  EXPECT_DOUBLE_EQ(val, 1.0);

  redis().del("t_ooo_single");
}

TEST(OutOfOrder, SingleAddStringStaleTimestamp)
{
  redis().del("t_ooo_str");

  auto t1 = redis().addString("t_ooo_str", "first", {.trim = 0});
  ASSERT_TRUE(t1.ok());

  RAL_Time old_time(1000000LL);
  auto t2 = redis().addString("t_ooo_str", "second", {.time = old_time, .trim = 0});
  EXPECT_FALSE(t2.ok());
  EXPECT_TRUE(redis().connected());

  redis().del("t_ooo_str");
}

TEST(OutOfOrder, SingleAddIntStaleTimestamp)
{
  redis().del("t_ooo_int");

  auto t1 = redis().addInt("t_ooo_int", 100, {.trim = 0});
  ASSERT_TRUE(t1.ok());

  RAL_Time old_time(1000000LL);
  auto t2 = redis().addInt("t_ooo_int", 200, {.time = old_time, .trim = 0});
  EXPECT_FALSE(t2.ok());
  EXPECT_TRUE(redis().connected());

  redis().del("t_ooo_int");
}

TEST(OutOfOrder, SingleAddBlobStaleTimestamp)
{
  redis().del("t_ooo_blob");

  uint8_t data[] = {1, 2, 3};
  auto t1 = redis().addBlob("t_ooo_blob", data, sizeof(data), {.trim = 0});
  ASSERT_TRUE(t1.ok());

  RAL_Time old_time(1000000LL);
  auto t2 = redis().addBlob("t_ooo_blob", data, sizeof(data), {.time = old_time, .trim = 0});
  EXPECT_FALSE(t2.ok());
  EXPECT_TRUE(redis().connected());

  redis().del("t_ooo_blob");
}

TEST(OutOfOrder, SingleAddAttrsStaleTimestamp)
{
  redis().del("t_ooo_attr");

  auto t1 = redis().addAttrs("t_ooo_attr", {{"k", "v1"}}, {.trim = 0});
  ASSERT_TRUE(t1.ok());

  RAL_Time old_time(1000000LL);
  auto t2 = redis().addAttrs("t_ooo_attr", {{"k", "v2"}}, {.time = old_time, .trim = 0});
  EXPECT_FALSE(t2.ok());
  EXPECT_TRUE(redis().connected());

  redis().del("t_ooo_attr");
}

// =========================================================================
//  Bulk pipeline with mixed valid/out-of-order entries
// =========================================================================

TEST(OutOfOrder, BulkPipelineMixedOrder)
{
  redis().del("t_ooo_bulk");

  // First, add an entry to establish a recent stream ID
  auto seed = redis().addDouble("t_ooo_bulk", 0.0, {.trim = 0});
  ASSERT_TRUE(seed.ok());

  // Build a batch where some entries have old timestamps (will fail)
  // and some use auto-timestamp (will succeed)
  TimeDoubleList data;
  RAL_Time old_time(1000000LL);

  data.push_back({RAL_Time(), 1.0});       // auto = succeeds
  data.push_back({old_time,   2.0});       // old  = fails
  data.push_back({RAL_Time(), 3.0});       // auto = succeeds
  data.push_back({old_time,   4.0});       // old  = fails
  data.push_back({RAL_Time(), 5.0});       // auto = succeeds

  auto ids = redis().addDoubles("t_ooo_bulk", data, 0);

  // Only the auto-timestamp entries should succeed
  EXPECT_EQ(ids.size(), 3u);
  for (auto& id : ids) EXPECT_TRUE(id.ok());

  // Connection must still be alive
  EXPECT_TRUE(redis().connected());

  // Stream should have 4 entries (seed + 3 successful)
  auto all = redis().getDoubles("t_ooo_bulk");
  EXPECT_EQ(all.size(), 4u);

  redis().del("t_ooo_bulk");
}

TEST(OutOfOrder, BulkPipelineAllFail)
{
  redis().del("t_ooo_allfail");

  // Seed with a recent entry
  auto seed = redis().addDouble("t_ooo_allfail", 0.0, {.trim = 0});
  ASSERT_TRUE(seed.ok());

  // All entries have old timestamps — all will fail
  RAL_Time old_time(1000000LL);
  TimeDoubleList data;
  for (int i = 0; i < 10; ++i)
    data.push_back({old_time, static_cast<double>(i)});

  auto ids = redis().addDoubles("t_ooo_allfail", data, 0);

  // All should fail
  EXPECT_TRUE(ids.empty());

  // Connection must still be alive — no false reconnect
  EXPECT_TRUE(redis().connected());

  // Only the seed entry should be in the stream
  auto all = redis().getDoubles("t_ooo_allfail");
  EXPECT_EQ(all.size(), 1u);

  redis().del("t_ooo_allfail");
}

// =========================================================================
//  WriteBatch with out-of-order entries to same key
// =========================================================================

TEST(OutOfOrder, WriteBatchSameKeyOutOfOrder)
{
  redis().del("t_ooo_wb");

  // Seed
  auto seed = redis().addDouble("t_ooo_wb", 0.0, {.trim = 0});
  ASSERT_TRUE(seed.ok());

  RAL_Time old_time(1000000LL);

  auto batch = redis().createBatch();
  batch.addDouble("t_ooo_wb", 1.0);                          // auto = succeeds
  batch.addDouble("t_ooo_wb", 2.0, {.time = old_time});      // old  = fails
  batch.addDouble("t_ooo_wb", 3.0);                          // auto = succeeds

  auto results = batch.execute();
  ASSERT_EQ(results.size(), 3u);

  // First and third should succeed, second should fail
  EXPECT_TRUE(results[0].ok());
  EXPECT_FALSE(results[1].ok());
  EXPECT_TRUE(results[2].ok());

  // Connection alive
  EXPECT_TRUE(redis().connected());

  redis().del("t_ooo_wb");
}

TEST(OutOfOrder, WriteBatchMultiKeyMixedOrder)
{
  redis().del("t_ooo_wbA");
  redis().del("t_ooo_wbB");

  // Seed both keys
  redis().addDouble("t_ooo_wbA", 0.0, {.trim = 0});
  redis().addDouble("t_ooo_wbB", 0.0, {.trim = 0});

  RAL_Time old_time(1000000LL);

  auto batch = redis().createBatch();
  batch.addDouble("t_ooo_wbA", 1.0);                          // succeeds
  batch.addDouble("t_ooo_wbB", 2.0, {.time = old_time});      // fails
  batch.addDouble("t_ooo_wbA", 3.0);                          // succeeds
  batch.addDouble("t_ooo_wbB", 4.0);                          // succeeds (auto)

  auto results = batch.execute();
  ASSERT_EQ(results.size(), 4u);
  EXPECT_TRUE(results[0].ok());
  EXPECT_FALSE(results[1].ok());
  EXPECT_TRUE(results[2].ok());
  EXPECT_TRUE(results[3].ok());

  EXPECT_TRUE(redis().connected());

  redis().del("t_ooo_wbA");
  redis().del("t_ooo_wbB");
}

// =========================================================================
//  Out-of-order under reader load — the tcache crash scenario
// =========================================================================

TEST(OutOfOrder, NoReconnectUnderReaderLoad)
{
  redis().del("t_ooo_reader");

  atomic<int> callbacks{0};
  redis().addReader("t_ooo_reader",
    [&](const string&, const string&, const TimeAttrsList&) {
      callbacks++;
    });
  this_thread::sleep_for(milliseconds(50));

  // Add a valid entry so the reader gets data
  redis().addDouble("t_ooo_reader", 1.0, {.trim = 0});
  this_thread::sleep_for(milliseconds(100));
  int before = callbacks.load();

  // Hammer with out-of-order adds — should NOT trigger reconnect,
  // which would stop/restart the reader (the old tcache crash path)
  RAL_Time old_time(1000000LL);
  for (int i = 0; i < 50; ++i)
    redis().addDouble("t_ooo_reader", 99.0, {.time = old_time, .trim = 0});

  // Connection should be alive, reader should still be running
  EXPECT_TRUE(redis().connected());

  // Add a valid entry — reader should still receive it
  redis().addDouble("t_ooo_reader", 2.0, {.trim = 0});
  this_thread::sleep_for(milliseconds(200));

  EXPECT_GT(callbacks.load(), before);

  redis().removeReader("t_ooo_reader");
  redis().del("t_ooo_reader");
}

// =========================================================================
//  Rapid out-of-order stress (crash/corruption detector)
// =========================================================================

TEST(OutOfOrder, RapidStressNoCrash)
{
  redis().del("t_ooo_stress");

  RAL_Time old_time(1000000LL);

  for (int round = 0; round < 5; ++round)
  {
    // Valid add
    redis().addDouble("t_ooo_stress", static_cast<double>(round), {.trim = 0});

    // Rapid out-of-order adds
    for (int i = 0; i < 100; ++i)
      redis().addDouble("t_ooo_stress", 0.0, {.time = old_time, .trim = 0});

    // Pipeline with mixed
    TimeDoubleList data;
    for (int i = 0; i < 50; ++i)
    {
      if (i % 3 == 0)
        data.push_back({old_time, static_cast<double>(i)});
      else
        data.push_back({RAL_Time(), static_cast<double>(i)});
    }
    redis().addDoubles("t_ooo_stress", data, 0);

    // WriteBatch mixed
    auto batch = redis().createBatch();
    for (int i = 0; i < 20; ++i)
    {
      if (i % 2 == 0)
        batch.addDouble("t_ooo_stress", static_cast<double>(i), {.time = old_time});
      else
        batch.addDouble("t_ooo_stress", static_cast<double>(i));
    }
    batch.execute();
  }

  EXPECT_TRUE(redis().connected());
  redis().del("t_ooo_stress");
}
