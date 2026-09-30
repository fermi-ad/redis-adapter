#define MOCK_REDIS_ADAPTER
#include "RedisAdapter.hpp"
#include <gtest/gtest.h>
#include <array>

TEST(MockStreams, ValidatesWireBytesWithoutAConnection) {
  EXPECT_EQ(RA_Time("1-2").id(), "1-2");
  EXPECT_EQ(RA_Time(-3), RA_INVALID_PAYLOAD);
  int value = 42;
  EXPECT_FALSE(RedisAdapter::decodeScalar<int>({{"_", "bad"}}, value));
  EXPECT_EQ(value, 42);
  std::vector<int> array{42};
  EXPECT_TRUE(RedisAdapter::decodeArray<int>({{"_", ""}}, array));
  EXPECT_TRUE(array.empty());
  EXPECT_EQ(RedisAdapter::compareStreamIds("9", "9-0"), 0);
}

TEST(MockStreams, RecordsArraysAndVectorsWithByteSizesAndElementAllocations) {
  RedisAdapter mock;
  const std::array<uint32_t, 3> values{{10, 20, 30}};
  EXPECT_TRUE(mock.addSingleList("array", values).ok());
  ASSERT_EQ(mock.addSingleList_array_and_span_arguments.size(), 1u);
  const auto& array = mock.addSingleList_array_and_span_arguments.front();
  EXPECT_EQ(array.dataSize, values.size() * sizeof(uint32_t));
  EXPECT_EQ(static_cast<const uint32_t*>(array.data.get())[2], 30u);
  const std::vector<double> values2{1., 2.};
  EXPECT_TRUE(mock.addSingleList("vector", values2).ok());
  ASSERT_EQ(mock.addSingleList_vector_arguments.size(), 1u);
  const auto& vector = mock.addSingleList_vector_arguments.front();
  EXPECT_EQ(vector.dataSize, values2.size() * sizeof(double));
  EXPECT_EQ(static_cast<const double*>(vector.data.get())[1], 2.);
}
