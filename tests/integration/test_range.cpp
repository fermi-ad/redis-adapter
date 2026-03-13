//
//  test_range.cpp
//
//  Integration tests for forward/reverse range queries
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"

using namespace std;

// ===================================================================
//  Forward range (XRANGE)
// ===================================================================

TEST(Range, Ints)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_i");

  RAL_Time id1 = redis.addInt("t_rng_i", 10, { .trim = 0 });
  RAL_Time id2 = redis.addInt("t_rng_i", 20, { .trim = 0 });
  RAL_Time id3 = redis.addInt("t_rng_i", 30, { .trim = 0 });

  auto results = redis.getInts("t_rng_i", { .minTime = id1, .maxTime = id3 });
  EXPECT_EQ(results.size(), 3u);
  EXPECT_EQ(results[0].second, 10);
  EXPECT_EQ(results[1].second, 20);
  EXPECT_EQ(results[2].second, 30);
}

TEST(Range, Strings)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_s");

  RAL_Time id1 = redis.addString("t_rng_s", "aaa", { .trim = 0 });
  RAL_Time id2 = redis.addString("t_rng_s", "bbb", { .trim = 0 });
  RAL_Time id3 = redis.addString("t_rng_s", "ccc", { .trim = 0 });

  auto results = redis.getStrings("t_rng_s", { .minTime = id1, .maxTime = id3 });
  EXPECT_EQ(results.size(), 3u);
  EXPECT_EQ(results[0].second, "aaa");
  EXPECT_EQ(results[1].second, "bbb");
  EXPECT_EQ(results[2].second, "ccc");
}

TEST(Range, Doubles)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_d");

  RAL_Time id1 = redis.addDouble("t_rng_d", 1.1, { .trim = 0 });
  RAL_Time id2 = redis.addDouble("t_rng_d", 2.2, { .trim = 0 });
  RAL_Time id3 = redis.addDouble("t_rng_d", 3.3, { .trim = 0 });

  auto results = redis.getDoubles("t_rng_d", { .minTime = id1, .maxTime = id3 });
  EXPECT_EQ(results.size(), 3u);
  EXPECT_DOUBLE_EQ(results[0].second, 1.1);
  EXPECT_DOUBLE_EQ(results[1].second, 2.2);
  EXPECT_DOUBLE_EQ(results[2].second, 3.3);
}

TEST(Range, Blobs)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_b");

  uint8_t a[] = { 1, 2 };
  uint8_t b[] = { 3, 4 };
  RAL_Time id1 = redis.addBlob("t_rng_b", a, sizeof(a), { .trim = 0 });
  RAL_Time id2 = redis.addBlob("t_rng_b", b, sizeof(b), { .trim = 0 });

  auto results = redis.getBlobs("t_rng_b", { .minTime = id1, .maxTime = id2 });
  EXPECT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].second[0], 1);
  EXPECT_EQ(results[0].second[1], 2);
  EXPECT_EQ(results[1].second[0], 3);
  EXPECT_EQ(results[1].second[1], 4);
}

TEST(Range, Attrs)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_a");

  Attrs a1 = {{ "k", "v1" }};
  Attrs a2 = {{ "k", "v2" }};
  RAL_Time id1 = redis.addAttrs("t_rng_a", a1, { .trim = 0 });
  RAL_Time id2 = redis.addAttrs("t_rng_a", a2, { .trim = 0 });

  auto results = redis.getAttrsRange("t_rng_a", { .minTime = id1, .maxTime = id2 });
  EXPECT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].second.at("k"), "v1");
  EXPECT_EQ(results[1].second.at("k"), "v2");
}

TEST(Range, WithCount)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_cnt");

  for (int i = 0; i < 5; i++)
    redis.addInt("t_rng_cnt", i * 10, { .trim = 0 });

  auto all = redis.getInts("t_rng_cnt");
  EXPECT_EQ(all.size(), 5u);

  auto limited = redis.getInts("t_rng_cnt", { .count = 2 });
  EXPECT_EQ(limited.size(), 2u);
  EXPECT_EQ(limited[0].second, 0);
  EXPECT_EQ(limited[1].second, 10);
}

TEST(Range, AfterMinTime)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_after");

  RAL_Time id1 = redis.addInt("t_rng_after", 10, { .trim = 0 });
  RAL_Time id2 = redis.addInt("t_rng_after", 20, { .trim = 0 });
  RAL_Time id3 = redis.addInt("t_rng_after", 30, { .trim = 0 });

  auto results = redis.getInts("t_rng_after", { .minTime = id1, .count = 2 });
  EXPECT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].second, 10);
  EXPECT_EQ(results[1].second, 20);
}

TEST(Range, Empty)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_empty");

  auto results = redis.getInts("t_rng_empty");
  EXPECT_TRUE(results.empty());
}

// ===================================================================
//  Reverse range (XREVRANGE / getBefore)
// ===================================================================

TEST(RangeBefore, Ints)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bfr_i");

  RAL_Time id1 = redis.addInt("t_bfr_i", 10, { .trim = 0 });
  RAL_Time id2 = redis.addInt("t_bfr_i", 20, { .trim = 0 });
  RAL_Time id3 = redis.addInt("t_bfr_i", 30, { .trim = 0 });
  RAL_Time id4 = redis.addInt("t_bfr_i", 40, { .trim = 0 });

  auto results = redis.getIntsBefore("t_bfr_i", { .maxTime = id3, .count = 2 });
  EXPECT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].second, 20);
  EXPECT_EQ(results[1].second, 30);
}

TEST(RangeBefore, Strings)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bfr_s");

  RAL_Time id1 = redis.addString("t_bfr_s", "aaa", { .trim = 0 });
  RAL_Time id2 = redis.addString("t_bfr_s", "bbb", { .trim = 0 });
  RAL_Time id3 = redis.addString("t_bfr_s", "ccc", { .trim = 0 });

  auto results = redis.getStringsBefore("t_bfr_s", { .maxTime = id3, .count = 2 });
  EXPECT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].second, "bbb");
  EXPECT_EQ(results[1].second, "ccc");
}

TEST(RangeBefore, Doubles)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bfr_d");

  RAL_Time id1 = redis.addDouble("t_bfr_d", 1.1, { .trim = 0 });
  RAL_Time id2 = redis.addDouble("t_bfr_d", 2.2, { .trim = 0 });
  RAL_Time id3 = redis.addDouble("t_bfr_d", 3.3, { .trim = 0 });

  auto results = redis.getDoublesBefore("t_bfr_d", { .maxTime = id3, .count = 2 });
  EXPECT_EQ(results.size(), 2u);
  EXPECT_DOUBLE_EQ(results[0].second, 2.2);
  EXPECT_DOUBLE_EQ(results[1].second, 3.3);
}

TEST(RangeBefore, Blobs)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bfr_b");

  uint8_t a[] = { 1 };
  uint8_t b[] = { 2 };
  uint8_t c[] = { 3 };
  RAL_Time id1 = redis.addBlob("t_bfr_b", a, 1, { .trim = 0 });
  RAL_Time id2 = redis.addBlob("t_bfr_b", b, 1, { .trim = 0 });
  RAL_Time id3 = redis.addBlob("t_bfr_b", c, 1, { .trim = 0 });

  auto results = redis.getBlobsBefore("t_bfr_b", { .maxTime = id3, .count = 2 });
  EXPECT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].second[0], 2);
  EXPECT_EQ(results[1].second[0], 3);
}

TEST(RangeBefore, Attrs)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bfr_a");

  RAL_Time id1 = redis.addAttrs("t_bfr_a", {{ "k", "v1" }}, { .trim = 0 });
  RAL_Time id2 = redis.addAttrs("t_bfr_a", {{ "k", "v2" }}, { .trim = 0 });
  RAL_Time id3 = redis.addAttrs("t_bfr_a", {{ "k", "v3" }}, { .trim = 0 });

  auto results = redis.getAttrsBefore("t_bfr_a", { .maxTime = id3, .count = 2 });
  EXPECT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].second.at("k"), "v2");
  EXPECT_EQ(results[1].second.at("k"), "v3");
}

TEST(RangeBefore, AllItems)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bfr_all");

  RAL_Time id1 = redis.addInt("t_bfr_all", 10, { .trim = 0 });
  RAL_Time id2 = redis.addInt("t_bfr_all", 20, { .trim = 0 });
  RAL_Time id3 = redis.addInt("t_bfr_all", 30, { .trim = 0 });

  auto results = redis.getIntsBefore("t_bfr_all", { .maxTime = id3 });
  EXPECT_EQ(results.size(), 3u);
  EXPECT_EQ(results[0].second, 10);
  EXPECT_EQ(results[1].second, 20);
  EXPECT_EQ(results[2].second, 30);
}

TEST(RangeBefore, ChronologicalOrder)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bfr_ord");

  RAL_Time id1 = redis.addInt("t_bfr_ord", 100, { .trim = 0 });
  RAL_Time id2 = redis.addInt("t_bfr_ord", 200, { .trim = 0 });
  RAL_Time id3 = redis.addInt("t_bfr_ord", 300, { .trim = 0 });
  RAL_Time id4 = redis.addInt("t_bfr_ord", 400, { .trim = 0 });
  RAL_Time id5 = redis.addInt("t_bfr_ord", 500, { .trim = 0 });

  auto results = redis.getIntsBefore("t_bfr_ord", { .maxTime = id5, .count = 3 });
  EXPECT_EQ(results.size(), 3u);
  EXPECT_LT(results[0].first.value, results[1].first.value);
  EXPECT_LT(results[1].first.value, results[2].first.value);
  EXPECT_EQ(results[0].second, 300);
  EXPECT_EQ(results[1].second, 400);
  EXPECT_EQ(results[2].second, 500);
}
