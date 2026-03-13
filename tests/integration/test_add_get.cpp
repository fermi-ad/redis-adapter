//
//  test_add_get.cpp
//
//  Integration tests for add/get single values
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"
#include <cstring>

using namespace std;

TEST(AddGet, String)
{
  RedisAdapterLite redis("TEST");

  EXPECT_TRUE(redis.addString("t_str", "hello").ok());
  string s;
  EXPECT_TRUE(redis.getString("t_str", s).ok());
  EXPECT_EQ(s, "hello");

  EXPECT_TRUE(redis.addString("t_str", "world").ok());
  EXPECT_TRUE(redis.getString("t_str", s).ok());
  EXPECT_EQ(s, "world");
}

TEST(AddGet, StringEmpty)
{
  RedisAdapterLite redis("TEST");

  EXPECT_TRUE(redis.addString("t_str_empty", "").ok());
  string s = "placeholder";
  EXPECT_TRUE(redis.getString("t_str_empty", s).ok());
  EXPECT_EQ(s, "");
}

TEST(AddGet, Double)
{
  RedisAdapterLite redis("TEST");

  EXPECT_TRUE(redis.addDouble("t_dbl", 3.14159).ok());
  double d = 0;
  EXPECT_TRUE(redis.getDouble("t_dbl", d).ok());
  EXPECT_DOUBLE_EQ(d, 3.14159);

  EXPECT_TRUE(redis.addDouble("t_dbl", -1.23e10).ok());
  EXPECT_TRUE(redis.getDouble("t_dbl", d).ok());
  EXPECT_DOUBLE_EQ(d, -1.23e10);
}

TEST(AddGet, DoubleZero)
{
  RedisAdapterLite redis("TEST");

  EXPECT_TRUE(redis.addDouble("t_dbl_z", 0.0).ok());
  double d = 1.0;
  EXPECT_TRUE(redis.getDouble("t_dbl_z", d).ok());
  EXPECT_DOUBLE_EQ(d, 0.0);
}

TEST(AddGet, Int)
{
  RedisAdapterLite redis("TEST");

  EXPECT_TRUE(redis.addInt("t_int", 42).ok());
  int64_t i = 0;
  EXPECT_TRUE(redis.getInt("t_int", i).ok());
  EXPECT_EQ(i, 42);

  EXPECT_TRUE(redis.addInt("t_int", -999).ok());
  EXPECT_TRUE(redis.getInt("t_int", i).ok());
  EXPECT_EQ(i, -999);
}

TEST(AddGet, IntLargeValues)
{
  RedisAdapterLite redis("TEST");

  int64_t big = INT64_MAX;
  EXPECT_TRUE(redis.addInt("t_int_big", big).ok());
  int64_t result = 0;
  EXPECT_TRUE(redis.getInt("t_int_big", result).ok());
  EXPECT_EQ(result, big);

  int64_t small = INT64_MIN;
  EXPECT_TRUE(redis.addInt("t_int_small", small).ok());
  EXPECT_TRUE(redis.getInt("t_int_small", result).ok());
  EXPECT_EQ(result, small);
}

TEST(AddGet, Blob)
{
  RedisAdapterLite redis("TEST");

  vector<float> vf = { 1.23f, 3.45f, 5.67f };
  EXPECT_TRUE(redis.addBlob("t_blob", vf.data(), vf.size() * sizeof(float)).ok());

  vector<uint8_t> blob;
  EXPECT_TRUE(redis.getBlob("t_blob", blob).ok());
  EXPECT_EQ(blob.size(), 3 * sizeof(float));

  float result[3];
  memcpy(result, blob.data(), blob.size());
  EXPECT_FLOAT_EQ(result[0], 1.23f);
  EXPECT_FLOAT_EQ(result[1], 3.45f);
  EXPECT_FLOAT_EQ(result[2], 5.67f);
}

TEST(AddGet, BlobBinaryData)
{
  RedisAdapterLite redis("TEST");

  uint8_t raw[] = { 0x00, 0xFF, 0x01, 0xFE, 0x00, 0x80 };
  EXPECT_TRUE(redis.addBlob("t_blob_bin", raw, sizeof(raw)).ok());

  vector<uint8_t> blob;
  EXPECT_TRUE(redis.getBlob("t_blob_bin", blob).ok());
  EXPECT_EQ(blob.size(), sizeof(raw));
  EXPECT_EQ(memcmp(blob.data(), raw, sizeof(raw)), 0);
}

TEST(AddGet, Attrs)
{
  RedisAdapterLite redis("TEST");

  Attrs data = {{ "a", "1" }, { "b", "2" }};
  EXPECT_TRUE(redis.addAttrs("t_attr", data).ok());

  Attrs dest;
  EXPECT_TRUE(redis.getAttrs("t_attr", dest).ok());
  EXPECT_EQ(dest.at("a"), "1");
  EXPECT_EQ(dest.at("b"), "2");
}

TEST(AddGet, AttrsMultiField)
{
  RedisAdapterLite redis("TEST");

  Attrs data = {{ "x", "10" }, { "y", "20" }, { "z", "30" }};
  EXPECT_TRUE(redis.addAttrs("t_attr_mf", data).ok());

  Attrs dest;
  EXPECT_TRUE(redis.getAttrs("t_attr_mf", dest).ok());
  EXPECT_EQ(dest.size(), 3u);
  EXPECT_EQ(dest.at("x"), "10");
  EXPECT_EQ(dest.at("y"), "20");
  EXPECT_EQ(dest.at("z"), "30");
}

TEST(AddGet, NonexistentKey)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_noexist");

  string s;
  EXPECT_FALSE(redis.getString("t_noexist", s).ok());

  double d = 0;
  EXPECT_FALSE(redis.getDouble("t_noexist", d).ok());

  int64_t i = 0;
  EXPECT_FALSE(redis.getInt("t_noexist", i).ok());

  vector<uint8_t> blob;
  EXPECT_FALSE(redis.getBlob("t_noexist", blob).ok());

  Attrs attrs;
  EXPECT_FALSE(redis.getAttrs("t_noexist", attrs).ok());
}

TEST(AddGet, WithTrim)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_trim");

  for (int i = 0; i < 5; i++)
    redis.addInt("t_trim", i, { .trim = 3 });

  auto results = redis.getInts("t_trim");
  EXPECT_LE(results.size(), 5u);
  EXPECT_GE(results.size(), 1u);
}
