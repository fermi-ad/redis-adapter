//
//  test_ral_helpers.cpp
//
//  Unit tests for memcpy serialization helpers
//

#include <gtest/gtest.h>
#include "RAL_Helpers.hpp"
#include <cstring>

using namespace std;

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
  Attrs a = {{ "_", string(4, '\0') }};
  EXPECT_FALSE(ral_to_double(a).has_value());

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
