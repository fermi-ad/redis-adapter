//
//  test_bulk.cpp
//
//  Integration tests for bulk add operations
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"

using namespace std;

TEST(Bulk, Strings)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bulk_s");

  TimeStringList data;
  data.emplace_back(RAL_Time(), "alpha");
  data.emplace_back(RAL_Time(), "beta");
  data.emplace_back(RAL_Time(), "gamma");

  auto ids = redis.addStrings("t_bulk_s", data, 10);
  EXPECT_EQ(ids.size(), 3u);
  for (auto& id : ids) EXPECT_TRUE(id.ok());

  auto results = redis.getStrings("t_bulk_s");
  EXPECT_EQ(results.size(), 3u);
  EXPECT_EQ(results[0].second, "alpha");
  EXPECT_EQ(results[1].second, "beta");
  EXPECT_EQ(results[2].second, "gamma");
}

TEST(Bulk, Doubles)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bulk_d");

  TimeDoubleList data;
  data.emplace_back(RAL_Time(), 1.1);
  data.emplace_back(RAL_Time(), 2.2);
  data.emplace_back(RAL_Time(), 3.3);

  auto ids = redis.addDoubles("t_bulk_d", data, 10);
  EXPECT_EQ(ids.size(), 3u);

  auto results = redis.getDoubles("t_bulk_d");
  EXPECT_EQ(results.size(), 3u);
  EXPECT_DOUBLE_EQ(results[0].second, 1.1);
  EXPECT_DOUBLE_EQ(results[1].second, 2.2);
  EXPECT_DOUBLE_EQ(results[2].second, 3.3);
}

TEST(Bulk, Ints)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bulk_i");

  TimeIntList data;
  data.emplace_back(RAL_Time(), 100);
  data.emplace_back(RAL_Time(), 200);
  data.emplace_back(RAL_Time(), 300);

  auto ids = redis.addInts("t_bulk_i", data, 10);
  EXPECT_EQ(ids.size(), 3u);

  auto results = redis.getInts("t_bulk_i");
  EXPECT_EQ(results.size(), 3u);
  EXPECT_EQ(results[0].second, 100);
  EXPECT_EQ(results[1].second, 200);
  EXPECT_EQ(results[2].second, 300);
}

TEST(Bulk, Blobs)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bulk_b");

  TimeBlobList data;
  data.emplace_back(RAL_Time(), vector<uint8_t>{ 0x01, 0x02 });
  data.emplace_back(RAL_Time(), vector<uint8_t>{ 0x03, 0x04 });

  auto ids = redis.addBlobs("t_bulk_b", data, 10);
  EXPECT_EQ(ids.size(), 2u);

  auto results = redis.getBlobs("t_bulk_b");
  EXPECT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].second, (vector<uint8_t>{ 0x01, 0x02 }));
  EXPECT_EQ(results[1].second, (vector<uint8_t>{ 0x03, 0x04 }));
}

TEST(Bulk, AttrsBatch)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bulk_a");

  TimeAttrsList data;
  data.emplace_back(RAL_Time(), Attrs{{ "x", "1" }});
  data.emplace_back(RAL_Time(), Attrs{{ "x", "2" }});

  auto ids = redis.addAttrsBatch("t_bulk_a", data, 10);
  EXPECT_EQ(ids.size(), 2u);

  auto results = redis.getAttrsRange("t_bulk_a");
  EXPECT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].second.at("x"), "1");
  EXPECT_EQ(results[1].second.at("x"), "2");
}

TEST(Bulk, WithTrim)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bulk_trim");

  TimeIntList data;
  for (int i = 0; i < 10; i++)
    data.emplace_back(RAL_Time(), i);

  auto ids = redis.addInts("t_bulk_trim", data, 5);
  EXPECT_EQ(ids.size(), 10u);

  auto results = redis.getInts("t_bulk_trim");
  EXPECT_LE(results.size(), 10u);
  EXPECT_GE(results.size(), 1u);
}

TEST(Bulk, Empty)
{
  RedisAdapterLite redis("TEST");

  TimeIntList empty;
  auto ids = redis.addInts("t_bulk_empty", empty, 1);
  EXPECT_TRUE(ids.empty());
}
