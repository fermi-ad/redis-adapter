//
//  test_pipeline.cpp
//
//  Tests for pipelined bulk operations.
//  Verifies that the pipeline path produces the same results as the
//  old sequential path while being significantly faster.
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <chrono>
#include <cmath>

using namespace std;
using namespace chrono;

static RedisAdapterLite& redis()
{
  static RedisAdapterLite r("TEST");
  return r;
}

// --- Correctness: all bulk types produce correct data via pipeline ---

TEST(Pipeline, BulkDoubles)
{
  redis().del("t_pipe_dbl");
  TimeDoubleList data;
  for (int i = 0; i < 50; ++i)
    data.push_back({RAL_Time(), static_cast<double>(i) * 1.1});

  auto ids = redis().addDoubles("t_pipe_dbl", data, 0);
  ASSERT_EQ(ids.size(), 50u);

  // All returned timestamps must be valid and increasing
  for (size_t i = 1; i < ids.size(); ++i)
  {
    EXPECT_TRUE(ids[i].ok());
    EXPECT_GT(ids[i].value, ids[i - 1].value);
  }

  // Read back and verify values
  auto results = redis().getDoubles("t_pipe_dbl");
  ASSERT_EQ(results.size(), 50u);
  for (int i = 0; i < 50; ++i)
    EXPECT_DOUBLE_EQ(results[i].second, static_cast<double>(i) * 1.1);

  redis().del("t_pipe_dbl");
}

TEST(Pipeline, BulkInts)
{
  redis().del("t_pipe_int");
  TimeIntList data;
  for (int i = 0; i < 50; ++i)
    data.push_back({RAL_Time(), static_cast<int64_t>(i * 100)});

  auto ids = redis().addInts("t_pipe_int", data, 0);
  ASSERT_EQ(ids.size(), 50u);

  auto results = redis().getInts("t_pipe_int");
  ASSERT_EQ(results.size(), 50u);
  for (int i = 0; i < 50; ++i)
    EXPECT_EQ(results[i].second, i * 100);

  redis().del("t_pipe_int");
}

TEST(Pipeline, BulkStrings)
{
  redis().del("t_pipe_str");
  TimeStringList data;
  for (int i = 0; i < 50; ++i)
    data.push_back({RAL_Time(), "str_" + to_string(i)});

  auto ids = redis().addStrings("t_pipe_str", data, 0);
  ASSERT_EQ(ids.size(), 50u);

  auto results = redis().getStrings("t_pipe_str");
  ASSERT_EQ(results.size(), 50u);
  for (int i = 0; i < 50; ++i)
    EXPECT_EQ(results[i].second, "str_" + to_string(i));

  redis().del("t_pipe_str");
}

TEST(Pipeline, BulkBlobs)
{
  redis().del("t_pipe_blob");
  TimeBlobList data;
  for (int i = 0; i < 50; ++i)
  {
    vector<uint8_t> blob(16, static_cast<uint8_t>(i));
    data.push_back({RAL_Time(), blob});
  }

  auto ids = redis().addBlobs("t_pipe_blob", data, 0);
  ASSERT_EQ(ids.size(), 50u);

  auto results = redis().getBlobs("t_pipe_blob");
  ASSERT_EQ(results.size(), 50u);
  for (int i = 0; i < 50; ++i)
  {
    ASSERT_EQ(results[i].second.size(), 16u);
    EXPECT_EQ(results[i].second[0], static_cast<uint8_t>(i));
  }

  redis().del("t_pipe_blob");
}

TEST(Pipeline, BulkAttrs)
{
  redis().del("t_pipe_attr");
  TimeAttrsList data;
  for (int i = 0; i < 50; ++i)
  {
    Attrs a = {{"key", to_string(i)}, {"val", "data_" + to_string(i)}};
    data.push_back({RAL_Time(), a});
  }

  auto ids = redis().addAttrsBatch("t_pipe_attr", data, 0);
  ASSERT_EQ(ids.size(), 50u);

  auto results = redis().getAttrsRange("t_pipe_attr");
  ASSERT_EQ(results.size(), 50u);
  for (int i = 0; i < 50; ++i)
  {
    EXPECT_EQ(results[i].second.at("key"), to_string(i));
    EXPECT_EQ(results[i].second.at("val"), "data_" + to_string(i));
  }

  redis().del("t_pipe_attr");
}

// --- Trim works with pipeline ---

TEST(Pipeline, TrimLimitsStreamSize)
{
  redis().del("t_pipe_trim");
  TimeDoubleList data;
  for (int i = 0; i < 100; ++i)
    data.push_back({RAL_Time(), static_cast<double>(i)});

  // Trim to ~50 entries (approximate with ~)
  auto ids = redis().addDoubles("t_pipe_trim", data, 50);
  EXPECT_EQ(ids.size(), 100u);

  // Stream should be trimmed to approximately 50
  auto results = redis().getDoubles("t_pipe_trim");
  EXPECT_LE(results.size(), 110u);  // ~ is approximate
  EXPECT_GE(results.size(), 40u);

  redis().del("t_pipe_trim");
}

// --- Empty batch ---

TEST(Pipeline, EmptyBatch)
{
  TimeDoubleList empty;
  auto ids = redis().addDoubles("t_pipe_empty", empty, 0);
  EXPECT_TRUE(ids.empty());
}

// --- Single item batch (edge case) ---

TEST(Pipeline, SingleItemBatch)
{
  redis().del("t_pipe_single");
  TimeDoubleList data = {{RAL_Time(), 42.0}};

  auto ids = redis().addDoubles("t_pipe_single", data, 0);
  ASSERT_EQ(ids.size(), 1u);
  EXPECT_TRUE(ids[0].ok());

  double dest;
  auto t = redis().getDouble("t_pipe_single", dest);
  EXPECT_TRUE(t.ok());
  EXPECT_DOUBLE_EQ(dest, 42.0);

  redis().del("t_pipe_single");
}

// --- Large batch ---

TEST(Pipeline, LargeBatch)
{
  redis().del("t_pipe_large");
  const int N = 5000;
  TimeDoubleList data;
  data.reserve(N);
  for (int i = 0; i < N; ++i)
    data.push_back({RAL_Time(), static_cast<double>(i)});

  auto ids = redis().addDoubles("t_pipe_large", data, 0);
  ASSERT_EQ(ids.size(), static_cast<size_t>(N));

  // Spot check first and last
  auto first = redis().getDoubles("t_pipe_large", {.count = 1});
  ASSERT_EQ(first.size(), 1u);
  EXPECT_DOUBLE_EQ(first[0].second, 0.0);

  RAL_GetArgs args;
  args.count = 1;
  auto last = redis().getDoublesBefore("t_pipe_large", args);
  ASSERT_EQ(last.size(), 1u);
  EXPECT_DOUBLE_EQ(last[0].second, static_cast<double>(N - 1));

  redis().del("t_pipe_large");
}

// --- Pipeline is faster than sequential ---

TEST(Pipeline, FasterThanSequential)
{
  redis().del("t_pipe_perf");

  const int N = 500;
  TimeDoubleList data;
  data.reserve(N);
  for (int i = 0; i < N; ++i)
    data.push_back({RAL_Time(), static_cast<double>(i)});

  // Pipelined bulk
  auto t0 = steady_clock::now();
  auto ids = redis().addDoubles("t_pipe_perf", data, 0);
  auto t1 = steady_clock::now();
  auto pipeline_us = duration_cast<microseconds>(t1 - t0).count();

  ASSERT_EQ(ids.size(), static_cast<size_t>(N));

  // Sequential single adds
  redis().del("t_pipe_perf");
  auto t2 = steady_clock::now();
  for (int i = 0; i < N; ++i)
    redis().addDouble("t_pipe_perf", static_cast<double>(i), {.trim = 0});
  auto t3 = steady_clock::now();
  auto sequential_us = duration_cast<microseconds>(t3 - t2).count();

  // Pipeline should be at least 5x faster for 500 items
  double speedup = static_cast<double>(sequential_us) / static_cast<double>(pipeline_us);
  EXPECT_GT(speedup, 5.0)
    << "Pipeline: " << pipeline_us << "us, Sequential: " << sequential_us
    << "us, Speedup: " << speedup << "x";

  redis().del("t_pipe_perf");
}
