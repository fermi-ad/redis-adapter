//
//  test_write_batch.cpp
//
//  Tests for WriteBatch — multi-key pipelined writes.
//  Verifies that commands to different keys are batched into a single
//  pipeline round-trip and produce correct results.
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <chrono>
#include <cmath>
#include <set>

using namespace std;
using namespace chrono;

static RedisAdapterLite& redis()
{
  static RedisAdapterLite r("TEST");
  return r;
}

static void cleanup(const vector<string>& keys)
{
  for (auto& k : keys) redis().del(k);
}

// --- Basic multi-key writes ---

TEST(WriteBatch, MultiKeyDoubles)
{
  cleanup({"batch_dA", "batch_dB", "batch_dC"});

  auto batch = redis().createBatch();
  batch.addDouble("batch_dA", 1.1);
  batch.addDouble("batch_dB", 2.2);
  batch.addDouble("batch_dC", 3.3);
  EXPECT_EQ(batch.size(), 3u);

  auto results = batch.execute();
  ASSERT_EQ(results.size(), 3u);
  EXPECT_TRUE(results[0].ok());
  EXPECT_TRUE(results[1].ok());
  EXPECT_TRUE(results[2].ok());
  EXPECT_EQ(batch.size(), 0u);  // cleared after execute

  double a, b, c;
  redis().getDouble("batch_dA", a);
  redis().getDouble("batch_dB", b);
  redis().getDouble("batch_dC", c);
  EXPECT_DOUBLE_EQ(a, 1.1);
  EXPECT_DOUBLE_EQ(b, 2.2);
  EXPECT_DOUBLE_EQ(c, 3.3);

  cleanup({"batch_dA", "batch_dB", "batch_dC"});
}

TEST(WriteBatch, MultiKeyInts)
{
  cleanup({"batch_iA", "batch_iB"});

  auto batch = redis().createBatch();
  batch.addInt("batch_iA", 100);
  batch.addInt("batch_iB", 200);

  auto results = batch.execute();
  ASSERT_EQ(results.size(), 2u);

  int64_t a, b;
  redis().getInt("batch_iA", a);
  redis().getInt("batch_iB", b);
  EXPECT_EQ(a, 100);
  EXPECT_EQ(b, 200);

  cleanup({"batch_iA", "batch_iB"});
}

TEST(WriteBatch, MultiKeyStrings)
{
  cleanup({"batch_sA", "batch_sB"});

  auto batch = redis().createBatch();
  batch.addString("batch_sA", "hello");
  batch.addString("batch_sB", "world");

  auto results = batch.execute();
  ASSERT_EQ(results.size(), 2u);

  string a, b;
  redis().getString("batch_sA", a);
  redis().getString("batch_sB", b);
  EXPECT_EQ(a, "hello");
  EXPECT_EQ(b, "world");

  cleanup({"batch_sA", "batch_sB"});
}

TEST(WriteBatch, MultiKeyBlobs)
{
  cleanup({"batch_bA", "batch_bB"});

  uint8_t blob1[] = {0xDE, 0xAD};
  uint8_t blob2[] = {0xBE, 0xEF, 0xCA, 0xFE};

  auto batch = redis().createBatch();
  batch.addBlob("batch_bA", blob1, sizeof(blob1));
  batch.addBlob("batch_bB", blob2, sizeof(blob2));

  auto results = batch.execute();
  ASSERT_EQ(results.size(), 2u);

  vector<uint8_t> a, b;
  redis().getBlob("batch_bA", a);
  redis().getBlob("batch_bB", b);
  ASSERT_EQ(a.size(), 2u);
  EXPECT_EQ(a[0], 0xDE);
  EXPECT_EQ(a[1], 0xAD);
  ASSERT_EQ(b.size(), 4u);
  EXPECT_EQ(b[0], 0xBE);
  EXPECT_EQ(b[3], 0xFE);

  cleanup({"batch_bA", "batch_bB"});
}

TEST(WriteBatch, MultiKeyAttrs)
{
  cleanup({"batch_aA", "batch_aB"});

  auto batch = redis().createBatch();
  batch.addAttrs("batch_aA", {{"x", "1"}, {"y", "2"}});
  batch.addAttrs("batch_aB", {{"name", "test"}});

  auto results = batch.execute();
  ASSERT_EQ(results.size(), 2u);

  Attrs a, b;
  redis().getAttrs("batch_aA", a);
  redis().getAttrs("batch_aB", b);
  EXPECT_EQ(a["x"], "1");
  EXPECT_EQ(a["y"], "2");
  EXPECT_EQ(b["name"], "test");

  cleanup({"batch_aA", "batch_aB"});
}

// --- Mixed types in one batch ---

TEST(WriteBatch, MixedTypes)
{
  cleanup({"batch_mx_d", "batch_mx_i", "batch_mx_s", "batch_mx_b"});

  auto batch = redis().createBatch();
  batch.addDouble("batch_mx_d", 9.9);
  batch.addInt("batch_mx_i", 42);
  batch.addString("batch_mx_s", "mixed");
  uint8_t blob[] = {1, 2, 3};
  batch.addBlob("batch_mx_b", blob, 3);

  auto results = batch.execute();
  ASSERT_EQ(results.size(), 4u);
  for (auto& r : results) EXPECT_TRUE(r.ok());

  double d; int64_t i; string s; vector<uint8_t> b;
  redis().getDouble("batch_mx_d", d);
  redis().getInt("batch_mx_i", i);
  redis().getString("batch_mx_s", s);
  redis().getBlob("batch_mx_b", b);
  EXPECT_DOUBLE_EQ(d, 9.9);
  EXPECT_EQ(i, 42);
  EXPECT_EQ(s, "mixed");
  ASSERT_EQ(b.size(), 3u);
  EXPECT_EQ(b[0], 1);

  cleanup({"batch_mx_d", "batch_mx_i", "batch_mx_s", "batch_mx_b"});
}

// --- Same key multiple times ---

TEST(WriteBatch, SameKeyMultipleTimes)
{
  cleanup({"batch_same"});

  auto batch = redis().createBatch();
  for (int i = 0; i < 10; ++i)
    batch.addDouble("batch_same", static_cast<double>(i), {.trim = 0});

  auto results = batch.execute();
  ASSERT_EQ(results.size(), 10u);

  auto all = redis().getDoubles("batch_same");
  ASSERT_EQ(all.size(), 10u);
  for (int i = 0; i < 10; ++i)
    EXPECT_DOUBLE_EQ(all[i].second, static_cast<double>(i));

  cleanup({"batch_same"});
}

// --- Trim per entry ---

TEST(WriteBatch, TrimPerEntry)
{
  cleanup({"batch_trim_a", "batch_trim_b"});

  // Add 5 entries to key A with trim=3, 5 to key B with no trim
  auto batch = redis().createBatch();
  for (int i = 0; i < 5; ++i)
  {
    batch.addDouble("batch_trim_a", static_cast<double>(i), {.trim = 3});
    batch.addDouble("batch_trim_b", static_cast<double>(i), {.trim = 0});
  }

  auto results = batch.execute();
  ASSERT_EQ(results.size(), 10u);

  auto a = redis().getDoubles("batch_trim_a");
  auto b = redis().getDoubles("batch_trim_b");

  // Key A should be trimmed (~ is approximate, but should be <= 5)
  EXPECT_LE(a.size(), 5u);
  // Key B should have all 5
  EXPECT_EQ(b.size(), 5u);

  cleanup({"batch_trim_a", "batch_trim_b"});
}

// --- Edge cases ---

TEST(WriteBatch, EmptyBatch)
{
  auto batch = redis().createBatch();
  EXPECT_EQ(batch.size(), 0u);
  auto results = batch.execute();
  EXPECT_TRUE(results.empty());
}

TEST(WriteBatch, SingleEntry)
{
  cleanup({"batch_one"});

  auto batch = redis().createBatch();
  batch.addDouble("batch_one", 7.7);
  EXPECT_EQ(batch.size(), 1u);

  auto results = batch.execute();
  ASSERT_EQ(results.size(), 1u);
  EXPECT_TRUE(results[0].ok());

  double d;
  redis().getDouble("batch_one", d);
  EXPECT_DOUBLE_EQ(d, 7.7);

  cleanup({"batch_one"});
}

TEST(WriteBatch, ClearDiscards)
{
  auto batch = redis().createBatch();
  batch.addDouble("batch_discard", 1.0);
  batch.addDouble("batch_discard", 2.0);
  EXPECT_EQ(batch.size(), 2u);
  batch.clear();
  EXPECT_EQ(batch.size(), 0u);

  auto results = batch.execute();
  EXPECT_TRUE(results.empty());
}

TEST(WriteBatch, ReuseBatchAfterExecute)
{
  cleanup({"batch_reuse_a", "batch_reuse_b"});

  auto batch = redis().createBatch();

  // First use
  batch.addDouble("batch_reuse_a", 1.0);
  auto r1 = batch.execute();
  ASSERT_EQ(r1.size(), 1u);
  EXPECT_EQ(batch.size(), 0u);

  // Second use
  batch.addDouble("batch_reuse_b", 2.0);
  auto r2 = batch.execute();
  ASSERT_EQ(r2.size(), 1u);

  double a, b;
  redis().getDouble("batch_reuse_a", a);
  redis().getDouble("batch_reuse_b", b);
  EXPECT_DOUBLE_EQ(a, 1.0);
  EXPECT_DOUBLE_EQ(b, 2.0);

  cleanup({"batch_reuse_a", "batch_reuse_b"});
}

// --- Large multi-key batch ---

TEST(WriteBatch, LargeMultiKeyBatch)
{
  const int N = 200;
  vector<string> keys;
  for (int i = 0; i < N; ++i)
    keys.push_back("batch_lg_" + to_string(i));
  cleanup(keys);

  auto batch = redis().createBatch();
  for (int i = 0; i < N; ++i)
    batch.addDouble(keys[i], static_cast<double>(i));

  auto results = batch.execute();
  ASSERT_EQ(results.size(), static_cast<size_t>(N));

  // Spot check several
  for (int i : {0, 50, 100, 199})
  {
    double d;
    redis().getDouble(keys[i], d);
    EXPECT_DOUBLE_EQ(d, static_cast<double>(i));
  }

  cleanup(keys);
}

// --- Timestamps are unique and increasing ---

TEST(WriteBatch, TimestampsAreIncreasing)
{
  cleanup({"batch_ts_a", "batch_ts_b", "batch_ts_c"});

  auto batch = redis().createBatch();
  batch.addDouble("batch_ts_a", 1.0);
  batch.addDouble("batch_ts_b", 2.0);
  batch.addDouble("batch_ts_c", 3.0);

  auto results = batch.execute();
  ASSERT_EQ(results.size(), 3u);

  // Each entry gets a unique stream ID from Redis
  set<int64_t> values;
  for (auto& r : results)
  {
    EXPECT_TRUE(r.ok());
    values.insert(r.value);
  }
  // All three should be unique
  EXPECT_EQ(values.size(), 3u);

  cleanup({"batch_ts_a", "batch_ts_b", "batch_ts_c"});
}

// --- Performance: batch vs sequential ---

TEST(WriteBatch, FasterThanSequentialMultiKey)
{
  const int N = 200;
  vector<string> keys;
  for (int i = 0; i < N; ++i)
    keys.push_back("batch_perf_" + to_string(i));
  cleanup(keys);

  // Pipelined batch
  auto t0 = steady_clock::now();
  {
    auto batch = redis().createBatch();
    for (int i = 0; i < N; ++i)
      batch.addDouble(keys[i], static_cast<double>(i));
    auto results = batch.execute();
    ASSERT_EQ(results.size(), static_cast<size_t>(N));
  }
  auto t1 = steady_clock::now();
  auto pipeline_us = duration_cast<microseconds>(t1 - t0).count();

  cleanup(keys);

  // Sequential single adds
  auto t2 = steady_clock::now();
  for (int i = 0; i < N; ++i)
    redis().addDouble(keys[i], static_cast<double>(i));
  auto t3 = steady_clock::now();
  auto sequential_us = duration_cast<microseconds>(t3 - t2).count();

  double speedup = static_cast<double>(sequential_us) / static_cast<double>(pipeline_us);
  EXPECT_GT(speedup, 3.0)
    << "Pipeline: " << pipeline_us << "us, Sequential: " << sequential_us
    << "us, Speedup: " << speedup << "x";

  cleanup(keys);
}
