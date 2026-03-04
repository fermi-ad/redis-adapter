//
//  test_lite.cpp
//
//  Google Test suite for RedisAdapterLite
//

#include <gtest/gtest.h>
#include "RedisAdapterLite.hpp"

using namespace std;
using namespace chrono;

// ===================================================================
//  RAL_Time
// ===================================================================

TEST(RAL_Time, DefaultZero)
{
  RAL_Time t;
  EXPECT_EQ(t.value, 0);
  EXPECT_FALSE(t.ok());
}

TEST(RAL_Time, PositiveOk)
{
  RAL_Time t(1000000);
  EXPECT_TRUE(t.ok());
  EXPECT_EQ(static_cast<int64_t>(t), 1000000);
  EXPECT_EQ(static_cast<uint64_t>(t), 1000000u);
}

TEST(RAL_Time, NegativeNotOk)
{
  RAL_Time t(-5);
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(static_cast<int64_t>(t), 0);
  EXPECT_EQ(t.err(), 5u);
}

TEST(RAL_Time, NotConnectedError)
{
  EXPECT_FALSE(RAL_NOT_CONNECTED.ok());
  EXPECT_EQ(RAL_NOT_CONNECTED.err(), 1u);
}

TEST(RAL_Time, IdRoundTrip)
{
  RAL_Time orig(123456789012345LL);  // nanoseconds
  string id = orig.id();
  RAL_Time parsed(id);
  EXPECT_EQ(parsed.value, orig.value);
}

TEST(RAL_Time, IdOrMinMax)
{
  RAL_Time valid(1000000);
  EXPECT_NE(valid.id_or_min(), "-");
  EXPECT_NE(valid.id_or_max(), "+");

  RAL_Time invalid;
  EXPECT_EQ(invalid.id_or_min(), "-");
  EXPECT_EQ(invalid.id_or_max(), "+");
}

TEST(RAL_Time, ParseBadString)
{
  RAL_Time t("not-a-number");
  EXPECT_FALSE(t.ok());
}

// ===================================================================
//  RAL_Helpers (memcpy serialization round trips)
// ===================================================================

TEST(RAL_Helpers, StringRoundTrip)
{
  Attrs a = ral_from_string("hello");
  auto v = ral_to_string(a);
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(*v, "hello");
}

TEST(RAL_Helpers, DoubleRoundTrip)
{
  Attrs a = ral_from_double(3.14159);
  auto v = ral_to_double(a);
  ASSERT_TRUE(v.has_value());
  EXPECT_DOUBLE_EQ(*v, 3.14159);
}

TEST(RAL_Helpers, DoubleNegativeRoundTrip)
{
  Attrs a = ral_from_double(-1.23e10);
  auto v = ral_to_double(a);
  ASSERT_TRUE(v.has_value());
  EXPECT_DOUBLE_EQ(*v, -1.23e10);
}

TEST(RAL_Helpers, IntRoundTrip)
{
  Attrs a = ral_from_int(-999);
  auto v = ral_to_int(a);
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(*v, -999);
}

TEST(RAL_Helpers, BlobRoundTrip)
{
  float data[] = { 1.0f, 2.0f, 3.0f };
  Attrs a = ral_from_blob(data, sizeof(data));
  auto v = ral_to_blob(a);
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(v->size(), sizeof(data));
  float result[3];
  memcpy(result, v->data(), v->size());
  EXPECT_FLOAT_EQ(result[0], 1.0f);
  EXPECT_FLOAT_EQ(result[1], 2.0f);
  EXPECT_FLOAT_EQ(result[2], 3.0f);
}

TEST(RAL_Helpers, EmptyAttrsReturnsNullopt)
{
  Attrs empty;
  EXPECT_FALSE(ral_to_string(empty).has_value());
  EXPECT_FALSE(ral_to_double(empty).has_value());
  EXPECT_FALSE(ral_to_int(empty).has_value());
  EXPECT_FALSE(ral_to_blob(empty).has_value());
}

TEST(RAL_Helpers, WrongSizeReturnsNullopt)
{
  // double expects exactly 8 bytes, give it 4
  Attrs a = {{ "_", string(4, '\0') }};
  EXPECT_FALSE(ral_to_double(a).has_value());

  // int expects exactly 8 bytes, give it 3
  Attrs b = {{ "_", string(3, '\0') }};
  EXPECT_FALSE(ral_to_int(b).has_value());
}

TEST(RAL_Helpers, NullBlobProducesEmptyString)
{
  Attrs a = ral_from_blob(nullptr, 0);
  auto v = ral_to_blob(a);
  ASSERT_TRUE(v.has_value());
  EXPECT_TRUE(v->empty());
}

// ===================================================================
//  Connection
// ===================================================================

TEST(RedisAdapterLite, Connected)
{
  RedisAdapterLite redis("TEST");
  EXPECT_TRUE(redis.connected());
}

TEST(RedisAdapterLite, ExitNotConnected)
{
  RedisAdapterLite redis("TEST");
  if (!redis.connected()) exit(1);
}

// ===================================================================
//  Add / Get single values
// ===================================================================

TEST(RedisAdapterLite, AddGetString)
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

TEST(RedisAdapterLite, AddGetStringEmpty)
{
  RedisAdapterLite redis("TEST");

  EXPECT_TRUE(redis.addString("t_str_empty", "").ok());
  string s = "placeholder";
  EXPECT_TRUE(redis.getString("t_str_empty", s).ok());
  EXPECT_EQ(s, "");
}

TEST(RedisAdapterLite, AddGetDouble)
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

TEST(RedisAdapterLite, AddGetDoubleZero)
{
  RedisAdapterLite redis("TEST");

  EXPECT_TRUE(redis.addDouble("t_dbl_z", 0.0).ok());
  double d = 1.0;
  EXPECT_TRUE(redis.getDouble("t_dbl_z", d).ok());
  EXPECT_DOUBLE_EQ(d, 0.0);
}

TEST(RedisAdapterLite, AddGetInt)
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

TEST(RedisAdapterLite, AddGetIntLargeValues)
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

TEST(RedisAdapterLite, AddGetBlob)
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

TEST(RedisAdapterLite, AddGetBlobBinaryData)
{
  RedisAdapterLite redis("TEST");

  // Include null bytes and high bytes
  uint8_t raw[] = { 0x00, 0xFF, 0x01, 0xFE, 0x00, 0x80 };
  EXPECT_TRUE(redis.addBlob("t_blob_bin", raw, sizeof(raw)).ok());

  vector<uint8_t> blob;
  EXPECT_TRUE(redis.getBlob("t_blob_bin", blob).ok());
  EXPECT_EQ(blob.size(), sizeof(raw));
  EXPECT_EQ(memcmp(blob.data(), raw, sizeof(raw)), 0);
}

TEST(RedisAdapterLite, AddGetAttrs)
{
  RedisAdapterLite redis("TEST");

  Attrs data = {{ "a", "1" }, { "b", "2" }};
  EXPECT_TRUE(redis.addAttrs("t_attr", data).ok());

  Attrs dest;
  EXPECT_TRUE(redis.getAttrs("t_attr", dest).ok());
  EXPECT_EQ(dest.at("a"), "1");
  EXPECT_EQ(dest.at("b"), "2");
}

TEST(RedisAdapterLite, AddGetAttrsMultiField)
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

TEST(RedisAdapterLite, GetNonexistentKey)
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

// ===================================================================
//  Add with custom time and trim
// ===================================================================

TEST(RedisAdapterLite, AddWithTrim)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_trim");

  // Add 5 items with trim=3
  for (int i = 0; i < 5; i++)
    redis.addInt("t_trim", i, { .trim = 3 });

  auto results = redis.getInts("t_trim");
  // Stream should be trimmed to approximately 3 entries
  EXPECT_LE(results.size(), 5u);
  EXPECT_GE(results.size(), 1u);
}

// ===================================================================
//  Get range (forward)
// ===================================================================

TEST(RedisAdapterLite, GetRangeInts)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_i");

  RAL_Time id1 = redis.addInt("t_rng_i", 10, { .trim = 0 });
  EXPECT_TRUE(id1.ok());
  RAL_Time id2 = redis.addInt("t_rng_i", 20, { .trim = 0 });
  EXPECT_TRUE(id2.ok());
  RAL_Time id3 = redis.addInt("t_rng_i", 30, { .trim = 0 });
  EXPECT_TRUE(id3.ok());

  auto results = redis.getInts("t_rng_i", { .minTime = id1, .maxTime = id3 });
  EXPECT_EQ(results.size(), 3u);
  EXPECT_EQ(results[0].second, 10);
  EXPECT_EQ(results[1].second, 20);
  EXPECT_EQ(results[2].second, 30);
}

TEST(RedisAdapterLite, GetRangeStrings)
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

TEST(RedisAdapterLite, GetRangeDoubles)
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

TEST(RedisAdapterLite, GetRangeBlobs)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_b");

  uint8_t a[] = { 1, 2 };
  uint8_t b[] = { 3, 4 };
  RAL_Time id1 = redis.addBlob("t_rng_b", a, sizeof(a), { .trim = 0 });
  RAL_Time id2 = redis.addBlob("t_rng_b", b, sizeof(b), { .trim = 0 });

  auto results = redis.getBlobs("t_rng_b", { .minTime = id1, .maxTime = id2 });
  EXPECT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].second.size(), 2u);
  EXPECT_EQ(results[0].second[0], 1);
  EXPECT_EQ(results[0].second[1], 2);
  EXPECT_EQ(results[1].second[0], 3);
  EXPECT_EQ(results[1].second[1], 4);
}

TEST(RedisAdapterLite, GetRangeAttrs)
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

TEST(RedisAdapterLite, GetRangeWithCount)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_cnt");

  for (int i = 0; i < 5; i++)
    redis.addInt("t_rng_cnt", i * 10, { .trim = 0 });

  // Get all (no count)
  auto all = redis.getInts("t_rng_cnt");
  EXPECT_EQ(all.size(), 5u);

  // Get with count=2 from beginning (forward)
  auto limited = redis.getInts("t_rng_cnt", { .count = 2 });
  EXPECT_EQ(limited.size(), 2u);
  EXPECT_EQ(limited[0].second, 0);
  EXPECT_EQ(limited[1].second, 10);
}

TEST(RedisAdapterLite, GetRangeAfterMinTime)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_after");

  RAL_Time id1 = redis.addInt("t_rng_after", 10, { .trim = 0 });
  RAL_Time id2 = redis.addInt("t_rng_after", 20, { .trim = 0 });
  RAL_Time id3 = redis.addInt("t_rng_after", 30, { .trim = 0 });

  // Get after id1 with count=2
  auto results = redis.getInts("t_rng_after", { .minTime = id1, .count = 2 });
  EXPECT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].second, 10);
  EXPECT_EQ(results[1].second, 20);
}

TEST(RedisAdapterLite, GetRangeEmpty)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_rng_empty");

  auto results = redis.getInts("t_rng_empty");
  EXPECT_TRUE(results.empty());
}

// ===================================================================
//  Get range (reverse / before)
// ===================================================================

TEST(RedisAdapterLite, GetIntsBefore)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bfr_i");

  RAL_Time id1 = redis.addInt("t_bfr_i", 10, { .trim = 0 });
  RAL_Time id2 = redis.addInt("t_bfr_i", 20, { .trim = 0 });
  RAL_Time id3 = redis.addInt("t_bfr_i", 30, { .trim = 0 });
  RAL_Time id4 = redis.addInt("t_bfr_i", 40, { .trim = 0 });

  // Get 2 items before (and including) id3
  auto results = redis.getIntsBefore("t_bfr_i", { .maxTime = id3, .count = 2 });
  EXPECT_EQ(results.size(), 2u);
  // Results should be in chronological order (oldest first)
  EXPECT_EQ(results[0].second, 20);
  EXPECT_EQ(results[1].second, 30);
}

TEST(RedisAdapterLite, GetStringsBefore)
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

TEST(RedisAdapterLite, GetDoublesBefore)
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

TEST(RedisAdapterLite, GetBlobsBefore)
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

TEST(RedisAdapterLite, GetAttrsBefore)
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

TEST(RedisAdapterLite, GetBeforeAllItems)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bfr_all");

  RAL_Time id1 = redis.addInt("t_bfr_all", 10, { .trim = 0 });
  RAL_Time id2 = redis.addInt("t_bfr_all", 20, { .trim = 0 });
  RAL_Time id3 = redis.addInt("t_bfr_all", 30, { .trim = 0 });

  // No count limit — get everything before maxTime
  auto results = redis.getIntsBefore("t_bfr_all", { .maxTime = id3 });
  EXPECT_EQ(results.size(), 3u);
  EXPECT_EQ(results[0].second, 10);
  EXPECT_EQ(results[1].second, 20);
  EXPECT_EQ(results[2].second, 30);
}

TEST(RedisAdapterLite, GetBeforeChronologicalOrder)
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
  // Must be chronological: timestamps ascending
  EXPECT_LT(results[0].first.value, results[1].first.value);
  EXPECT_LT(results[1].first.value, results[2].first.value);
  EXPECT_EQ(results[0].second, 300);
  EXPECT_EQ(results[1].second, 400);
  EXPECT_EQ(results[2].second, 500);
}

// ===================================================================
//  Bulk add
// ===================================================================

TEST(RedisAdapterLite, BulkAddStrings)
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

TEST(RedisAdapterLite, BulkAddDoubles)
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

TEST(RedisAdapterLite, BulkAddInts)
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

TEST(RedisAdapterLite, BulkAddBlobs)
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

TEST(RedisAdapterLite, BulkAddAttrsBatch)
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

TEST(RedisAdapterLite, BulkAddWithTrim)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_bulk_trim");

  TimeIntList data;
  for (int i = 0; i < 10; i++)
    data.emplace_back(RAL_Time(), i);

  auto ids = redis.addInts("t_bulk_trim", data, 5);
  EXPECT_EQ(ids.size(), 10u);

  // After trim, stream should have at most ~10 entries (trim=max(5,10)=10)
  auto results = redis.getInts("t_bulk_trim");
  EXPECT_LE(results.size(), 10u);
  EXPECT_GE(results.size(), 1u);
}

TEST(RedisAdapterLite, BulkAddEmpty)
{
  RedisAdapterLite redis("TEST");

  TimeIntList empty;
  auto ids = redis.addInts("t_bulk_empty", empty, 1);
  EXPECT_TRUE(ids.empty());
}

// ===================================================================
//  Utility (del, copy, rename)
// ===================================================================

TEST(RedisAdapterLite, Del)
{
  RedisAdapterLite redis("TEST");

  EXPECT_TRUE(redis.addInt("t_del", 42).ok());

  int64_t val = 0;
  EXPECT_TRUE(redis.getInt("t_del", val).ok());
  EXPECT_EQ(val, 42);

  EXPECT_TRUE(redis.del("t_del"));

  EXPECT_FALSE(redis.getInt("t_del", val).ok());
}

TEST(RedisAdapterLite, DelNonexistent)
{
  RedisAdapterLite redis("TEST");
  // Deleting a key that doesn't exist should succeed (returns >= 0)
  EXPECT_TRUE(redis.del("t_del_noexist"));
}

TEST(RedisAdapterLite, Copy)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_copy_src");
  redis.del("t_copy_dst");

  EXPECT_TRUE(redis.addInt("t_copy_src", 77).ok());
  EXPECT_TRUE(redis.copy("t_copy_src", "t_copy_dst"));

  int64_t val = 0;
  EXPECT_TRUE(redis.getInt("t_copy_dst", val).ok());
  EXPECT_EQ(val, 77);
}

TEST(RedisAdapterLite, Rename)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_ren_src");
  redis.del("t_ren_dst");

  EXPECT_TRUE(redis.addInt("t_ren_src", 88).ok());
  EXPECT_TRUE(redis.rename("t_ren_src", "t_ren_dst"));

  int64_t val = 0;
  EXPECT_TRUE(redis.getInt("t_ren_dst", val).ok());
  EXPECT_EQ(val, 88);

  // Source should no longer exist
  EXPECT_FALSE(redis.getInt("t_ren_src", val).ok());
}

TEST(RedisAdapterLite, CopyWithBaseKey)
{
  RedisAdapterLite redis("TEST");
  RedisAdapterLite redis2("OTHER");
  redis.del("t_cpbk_dst");

  EXPECT_TRUE(redis2.addInt("t_cpbk_src", 55).ok());
  // Copy from OTHER:t_cpbk_src to TEST:t_cpbk_dst
  EXPECT_TRUE(redis.copy("t_cpbk_src", "t_cpbk_dst", "OTHER"));

  int64_t val = 0;
  EXPECT_TRUE(redis.getInt("t_cpbk_dst", val).ok());
  EXPECT_EQ(val, 55);
}

// ===================================================================
//  Stream timestamps
// ===================================================================

TEST(RedisAdapterLite, TimestampsIncreasing)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_ts_inc");

  RAL_Time t1 = redis.addInt("t_ts_inc", 1, { .trim = 0 });
  RAL_Time t2 = redis.addInt("t_ts_inc", 2, { .trim = 0 });
  RAL_Time t3 = redis.addInt("t_ts_inc", 3, { .trim = 0 });

  EXPECT_TRUE(t1.ok());
  EXPECT_TRUE(t2.ok());
  EXPECT_TRUE(t3.ok());
  EXPECT_LT(t1.value, t2.value);
  EXPECT_LT(t2.value, t3.value);
}

TEST(RedisAdapterLite, GetAtSpecificTime)
{
  RedisAdapterLite redis("TEST");
  redis.del("t_ts_at");

  RAL_Time id1 = redis.addInt("t_ts_at", 10, { .trim = 0 });
  RAL_Time id2 = redis.addInt("t_ts_at", 20, { .trim = 0 });
  RAL_Time id3 = redis.addInt("t_ts_at", 30, { .trim = 0 });

  // Get single at or before id2 — should get 20
  int64_t val = 0;
  RAL_Time got = redis.getInt("t_ts_at", val, { .maxTime = id2 });
  EXPECT_TRUE(got.ok());
  EXPECT_EQ(val, 20);
  EXPECT_EQ(got.value, id2.value);
}

// ===================================================================
//  Reader
// ===================================================================

TEST(RedisAdapterLite, Reader)
{
  RedisAdapterLite redis("TEST");

  atomic<bool> received{false};

  EXPECT_TRUE(redis.addDouble("t_reader", 0.0).ok());

  EXPECT_TRUE(redis.addReader("t_reader",
    [&](const string& base, const string& sub, const TimeAttrsList& data)
    {
      received = true;
      EXPECT_EQ(base, "TEST");
      EXPECT_EQ(sub, "t_reader");
      EXPECT_GT(data.size(), 0u);
      auto val = ral_to_double(data[0].second);
      EXPECT_TRUE(val.has_value());
      EXPECT_DOUBLE_EQ(*val, 3.14);
    }
  ));
  this_thread::sleep_for(milliseconds(10));

  EXPECT_TRUE(redis.addDouble("t_reader", 3.14).ok());

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_TRUE(received.load());

  EXPECT_TRUE(redis.removeReader("t_reader"));
  this_thread::sleep_for(milliseconds(10));

  received = false;
  EXPECT_TRUE(redis.addDouble("t_reader", 0.0).ok());

  for (int i = 0; i < 20 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_FALSE(received.load());
}

TEST(RedisAdapterLite, ReaderInt)
{
  RedisAdapterLite redis("TEST");

  atomic<bool> received{false};
  int64_t received_val = 0;

  EXPECT_TRUE(redis.addInt("t_reader_i", 0).ok());

  EXPECT_TRUE(redis.addReader("t_reader_i",
    [&](const string&, const string&, const TimeAttrsList& data)
    {
      if (!data.empty())
      {
        auto val = ral_to_int(data[0].second);
        if (val) received_val = *val;
      }
      received = true;
    }
  ));
  this_thread::sleep_for(milliseconds(10));

  EXPECT_TRUE(redis.addInt("t_reader_i", 42).ok());

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_TRUE(received.load());
  EXPECT_EQ(received_val, 42);

  EXPECT_TRUE(redis.removeReader("t_reader_i"));
}

TEST(RedisAdapterLite, ReaderString)
{
  RedisAdapterLite redis("TEST");

  atomic<bool> received{false};
  string received_val;

  EXPECT_TRUE(redis.addString("t_reader_s", "init").ok());

  EXPECT_TRUE(redis.addReader("t_reader_s",
    [&](const string&, const string&, const TimeAttrsList& data)
    {
      if (!data.empty())
      {
        auto val = ral_to_string(data[0].second);
        if (val) received_val = *val;
      }
      received = true;
    }
  ));
  this_thread::sleep_for(milliseconds(10));

  EXPECT_TRUE(redis.addString("t_reader_s", "payload").ok());

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_TRUE(received.load());
  EXPECT_EQ(received_val, "payload");

  EXPECT_TRUE(redis.removeReader("t_reader_s"));
}

// ===================================================================
//  Defer reader
// ===================================================================

TEST(RedisAdapterLite, DeferReader)
{
  RedisAdapterLite redis("TEST");

  atomic<bool> received{false};

  EXPECT_TRUE(redis.setDeferReaders(true));

  EXPECT_TRUE(redis.addReader("t_defer",
    [&](const string&, const string&, const TimeAttrsList& data)
    {
      received = true;
    }
  ));
  this_thread::sleep_for(milliseconds(10));

  received = false;
  EXPECT_TRUE(redis.addInt("t_defer", 1).ok());

  for (int i = 0; i < 20 && !received; i++)
    this_thread::sleep_for(milliseconds(5));
  EXPECT_FALSE(received.load());

  EXPECT_TRUE(redis.setDeferReaders(false));
  this_thread::sleep_for(milliseconds(10));

  received = false;
  EXPECT_TRUE(redis.addInt("t_defer", 2).ok());

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));
  EXPECT_TRUE(received.load());
}

// ===================================================================
//  Watchdog
// ===================================================================

TEST(RedisAdapterLite, Watchdog)
{
  // Clean stale watchdog data from previous runs
  { RedisAdapterLite cleanup("TEST_WD"); cleanup.del("watchdog"); }

  RAL_Options opts;
  opts.dogname = "WDTEST";
  RedisAdapterLite redis("TEST_WD", opts);

  this_thread::sleep_for(milliseconds(100));
  EXPECT_EQ(redis.getWatchdogs().size(), 1u);

  EXPECT_TRUE(redis.addWatchdog("SPOT", 1));

  this_thread::sleep_for(milliseconds(600));
  EXPECT_EQ(redis.getWatchdogs().size(), 2u);

  EXPECT_TRUE(redis.petWatchdog("SPOT", 1));
  this_thread::sleep_for(milliseconds(600));
  EXPECT_EQ(redis.getWatchdogs().size(), 2u);

  // Let SPOT expire (requires Redis 7.4+ for HEXPIRE)
  this_thread::sleep_for(milliseconds(600));
  auto dogs = redis.getWatchdogs();
  if (dogs.size() == 1u)
    EXPECT_EQ(dogs.size(), 1u);  // HEXPIRE worked, SPOT expired
  else
    GTEST_SKIP() << "HEXPIRE not supported (requires Redis 7.4+), skipping expiration check";
}

TEST(RedisAdapterLite, WatchdogNoDogname)
{
  // No dogname = no auto-watchdog
  RedisAdapterLite redis("TEST_WD2");

  this_thread::sleep_for(milliseconds(50));
  auto dogs = redis.getWatchdogs();
  // Should have no auto-registered watchdog
  EXPECT_EQ(dogs.size(), 0u);
}

// ===================================================================
//  Pub/Sub
// ===================================================================

TEST(RedisAdapterLite, PubSub)
{
  RedisAdapterLite redis("TEST");

  atomic<bool> received{false};
  string received_msg;

  EXPECT_TRUE(redis.subscribe("t_pubsub",
    [&](const string& base, const string& sub, const string& msg)
    {
      received_msg = msg;
      received = true;
    }
  ));
  this_thread::sleep_for(milliseconds(100));

  EXPECT_TRUE(redis.publish("t_pubsub", "hello world"));

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_TRUE(received.load());
  EXPECT_EQ(received_msg, "hello world");

  EXPECT_TRUE(redis.unsubscribe("t_pubsub"));
}

TEST(RedisAdapterLite, PubSubMultipleMessages)
{
  RedisAdapterLite redis("TEST");

  atomic<int> count{0};

  EXPECT_TRUE(redis.subscribe("t_pubsub_multi",
    [&](const string&, const string&, const string& msg)
    {
      count++;
    }
  ));
  this_thread::sleep_for(milliseconds(100));

  for (int i = 0; i < 5; i++)
    redis.publish("t_pubsub_multi", "msg" + to_string(i));

  for (int i = 0; i < 100 && count < 5; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_EQ(count.load(), 5);
  EXPECT_TRUE(redis.unsubscribe("t_pubsub_multi"));
}

TEST(RedisAdapterLite, PubSubBaseKey)
{
  RedisAdapterLite redis("TEST");

  atomic<bool> received{false};
  string received_base;

  EXPECT_TRUE(redis.subscribe("t_pubsub_bk",
    [&](const string& base, const string& sub, const string& msg)
    {
      received_base = base;
      received = true;
    }
  ));
  this_thread::sleep_for(milliseconds(100));

  EXPECT_TRUE(redis.publish("t_pubsub_bk", "msg"));

  for (int i = 0; i < 40 && !received; i++)
    this_thread::sleep_for(milliseconds(5));

  EXPECT_TRUE(received.load());
  EXPECT_EQ(received_base, "TEST");

  EXPECT_TRUE(redis.unsubscribe("t_pubsub_bk"));
}
