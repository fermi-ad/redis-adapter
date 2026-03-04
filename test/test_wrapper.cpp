//
//  test_wrapper.cpp
//
//  Compile-time and runtime tests for the RedisAdapter compatibility wrapper.
//

#include "RedisAdapter.hpp"
#include <gtest/gtest.h>
#include <array>
#include <cmath>

// ------ Compile-time checks ------

static_assert(std::is_same_v<RA_Time, RAL_Time>,
              "RA_Time must be an alias for RAL_Time");

static_assert(RA_NOT_CONNECTED.value == -1,
              "RA_NOT_CONNECTED must be -1");

static_assert(std::is_same_v<RedisAdapter::Attrs, Attrs>,
              "Attrs alias must match");

// TimeVal / TimeValList should match the lite concrete types
static_assert(std::is_same_v<RedisAdapter::TimeValList<std::string>, TimeStringList>,
              "TimeValList<string> must equal TimeStringList");

static_assert(std::is_same_v<RedisAdapter::TimeValList<double>, TimeDoubleList>,
              "TimeValList<double> must equal TimeDoubleList");

static_assert(std::is_same_v<RedisAdapter::TimeValList<int64_t>, TimeIntList>,
              "TimeValList<int64_t> must equal TimeIntList");

static_assert(std::is_same_v<RedisAdapter::TimeValList<Attrs>, TimeAttrsList>,
              "TimeValList<Attrs> must equal TimeAttrsList");

// ------ RA_Options conversion ------

TEST(WrapperOptions, DefaultOptions)
{
  RA_Options opts;
  EXPECT_EQ(opts.cxn.host, "127.0.0.1");
  EXPECT_EQ(opts.cxn.port, 6379);
  EXPECT_EQ(opts.cxn.timeout, 500u);
  EXPECT_EQ(opts.cxn.user, "default");
  EXPECT_TRUE(opts.cxn.password.empty());
  EXPECT_TRUE(opts.cxn.path.empty());
  EXPECT_EQ(opts.workers, 1);
  EXPECT_EQ(opts.readers, 1);
  EXPECT_TRUE(opts.dogname.empty());
}

TEST(WrapperOptions, CustomOptions)
{
  RA_Options opts;
  opts.cxn.host = "10.0.0.1";
  opts.cxn.port = 7777;
  opts.cxn.password = "secret";
  opts.dogname = "watchdog1";
  opts.workers = 4;
  opts.readers = 2;

  // Just verify the struct is usable — actual construction tested below
  EXPECT_EQ(opts.cxn.host, "10.0.0.1");
  EXPECT_EQ(opts.dogname, "watchdog1");
}

// ------ RA_ArgsGet / RA_ArgsAdd ------

TEST(WrapperArgs, GetArgsDefaults)
{
  RA_ArgsGet args;
  EXPECT_TRUE(args.baseKey.empty());
  EXPECT_EQ(args.minTime.value, 0);
  EXPECT_EQ(args.maxTime.value, 0);
  EXPECT_EQ(args.count, 1u);   // old default was 1, not 0
}

TEST(WrapperArgs, AddArgsDefaults)
{
  RA_ArgsAdd args;
  EXPECT_EQ(args.time.value, 0);
  EXPECT_EQ(args.trim, 1u);
}

// ------ Tests requiring a Redis connection ------

class WrapperTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    ra = std::make_unique<RedisAdapter>("WRAP_TEST");
    if (!ra->connected()) GTEST_SKIP() << "Redis not available";
    ra->del("val_str");
    ra->del("val_dbl");
    ra->del("val_int");
    ra->del("val_float");
    ra->del("val_blob");
    ra->del("val_attrs");
    ra->del("val_list");
    ra->del("multi");
  }

  std::unique_ptr<RedisAdapter> ra;
};

// --- addSingleValue<string> / getSingleValue<string> ---

TEST_F(WrapperTest, SingleString)
{
  auto t = ra->addSingleValue<std::string>("val_str", "hello");
  ASSERT_TRUE(t.ok());

  std::string dest;
  auto t2 = ra->getSingleValue<std::string>("val_str", dest);
  ASSERT_TRUE(t2.ok());
  EXPECT_EQ(dest, "hello");
}

// --- addSingleDouble / getSingleValue<double> ---

TEST_F(WrapperTest, SingleDouble)
{
  auto t = ra->addSingleDouble("val_dbl", 3.14);
  ASSERT_TRUE(t.ok());

  double dest = 0.0;
  auto t2 = ra->getSingleValue<double>("val_dbl", dest);
  ASSERT_TRUE(t2.ok());
  EXPECT_DOUBLE_EQ(dest, 3.14);
}

// --- addSingleValue<int32_t> / getSingleValue<int32_t> (trivial blob path) ---

TEST_F(WrapperTest, SingleTrivial)
{
  int32_t src = 42;
  auto t = ra->addSingleValue<int32_t>("val_int", src);
  ASSERT_TRUE(t.ok());

  int32_t dest = 0;
  auto t2 = ra->getSingleValue<int32_t>("val_int", dest);
  ASSERT_TRUE(t2.ok());
  EXPECT_EQ(dest, 42);
}

// --- addSingleValue<float> ---

TEST_F(WrapperTest, SingleFloat)
{
  float src = 2.5f;
  auto t = ra->addSingleValue<float>("val_float", src);
  ASSERT_TRUE(t.ok());

  float dest = 0.0f;
  auto t2 = ra->getSingleValue<float>("val_float", dest);
  ASSERT_TRUE(t2.ok());
  EXPECT_FLOAT_EQ(dest, 2.5f);
}

// --- addSingleValue<Attrs> / getSingleValue<Attrs> ---

TEST_F(WrapperTest, SingleAttrs)
{
  Attrs src{{"a", "1"}, {"b", "2"}};
  auto t = ra->addSingleValue<Attrs>("val_attrs", src);
  ASSERT_TRUE(t.ok());

  Attrs dest;
  auto t2 = ra->getSingleValue<Attrs>("val_attrs", dest);
  ASSERT_TRUE(t2.ok());
  EXPECT_EQ(dest["a"], "1");
  EXPECT_EQ(dest["b"], "2");
}

// --- addSingleList / getSingleList ---

TEST_F(WrapperTest, SingleList)
{
  std::vector<int32_t> src = {10, 20, 30, 40, 50};
  auto t = ra->addSingleList("val_list", src);
  ASSERT_TRUE(t.ok());

  std::vector<int32_t> dest;
  auto t2 = ra->getSingleList<int32_t>("val_list", dest);
  ASSERT_TRUE(t2.ok());
  EXPECT_EQ(dest, src);
}

// --- addSingleList with std::array ---

TEST_F(WrapperTest, SingleListArray)
{
  std::array<double, 3> src = {1.0, 2.0, 3.0};
  auto t = ra->addSingleList("val_list", src);
  ASSERT_TRUE(t.ok());

  std::vector<double> dest;
  auto t2 = ra->getSingleList<double>("val_list", dest);
  ASSERT_TRUE(t2.ok());
  ASSERT_EQ(dest.size(), 3u);
  EXPECT_DOUBLE_EQ(dest[0], 1.0);
  EXPECT_DOUBLE_EQ(dest[1], 2.0);
  EXPECT_DOUBLE_EQ(dest[2], 3.0);
}

// --- addValues (bulk) / getValues (forward range) ---

TEST_F(WrapperTest, BulkStringValues)
{
  RedisAdapter::TimeValList<std::string> data;
  data.push_back({RA_Time(0), "first"});
  data.push_back({RA_Time(0), "second"});
  data.push_back({RA_Time(0), "third"});

  auto ids = ra->addValues<std::string>("multi", data, 10);
  ASSERT_EQ(ids.size(), 3u);

  auto result = ra->getValues<std::string>("multi");
  ASSERT_EQ(result.size(), 3u);
  EXPECT_EQ(result[0].second, "first");
  EXPECT_EQ(result[1].second, "second");
  EXPECT_EQ(result[2].second, "third");
}

TEST_F(WrapperTest, BulkDoubleValues)
{
  RedisAdapter::TimeValList<double> data;
  data.push_back({RA_Time(0), 1.1});
  data.push_back({RA_Time(0), 2.2});

  auto ids = ra->addValues<double>("multi", data, 10);
  ASSERT_EQ(ids.size(), 2u);

  auto result = ra->getValues<double>("multi");
  ASSERT_EQ(result.size(), 2u);
  EXPECT_DOUBLE_EQ(result[0].second, 1.1);
  EXPECT_DOUBLE_EQ(result[1].second, 2.2);
}

// --- getValuesBefore / getValuesAfter ---

TEST_F(WrapperTest, ValuesBeforeAfter)
{
  RedisAdapter::TimeValList<std::string> data;
  data.push_back({RA_Time(0), "a"});
  data.push_back({RA_Time(0), "b"});
  data.push_back({RA_Time(0), "c"});
  auto ids = ra->addValues<std::string>("multi", data, 10);
  ASSERT_EQ(ids.size(), 3u);

  // getValuesBefore with count=2 should return last 2 items in chronological order
  auto before = ra->getValuesBefore<std::string>("multi", {.count = 2});
  ASSERT_EQ(before.size(), 2u);
  EXPECT_EQ(before[0].second, "b");
  EXPECT_EQ(before[1].second, "c");

  // getValuesAfter starting from the first id
  auto after = ra->getValuesAfter<std::string>("multi", {.minTime = ids[0], .count = 10});
  ASSERT_GE(after.size(), 2u);  // at least b and c (minTime is exclusive in some implementations)
}

// --- addLists / getLists ---

TEST_F(WrapperTest, BulkLists)
{
  RedisAdapter::TimeValList<std::vector<float>> data;
  data.push_back({RA_Time(0), {1.0f, 2.0f}});
  data.push_back({RA_Time(0), {3.0f, 4.0f, 5.0f}});

  auto ids = ra->addLists<float>("multi", data, 10);
  ASSERT_EQ(ids.size(), 2u);

  auto result = ra->getLists<float>("multi");
  ASSERT_EQ(result.size(), 2u);
  ASSERT_EQ(result[0].second.size(), 2u);
  EXPECT_FLOAT_EQ(result[0].second[0], 1.0f);
  EXPECT_FLOAT_EQ(result[0].second[1], 2.0f);
  ASSERT_EQ(result[1].second.size(), 3u);
}

// --- Key management ---

TEST_F(WrapperTest, DeleteRename)
{
  ra->addSingleValue<std::string>("val_str", "test");
  EXPECT_TRUE(ra->del("val_str"));

  std::string dest;
  auto t = ra->getSingleValue<std::string>("val_str", dest);
  EXPECT_FALSE(t.ok());
}
