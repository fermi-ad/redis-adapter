#define MOCK_REDIS_ADAPTER
#include "RedisAdapter.hpp"
#include <gtest/gtest.h>
#include <array>

TEST(MockStreams, ValidatesWireBytesWithoutAConnection) {
  const RedisAdapter mock;
  EXPECT_EQ(mock.readerBackendStatus().requested, RA_ReaderMode::Blocking);
  EXPECT_EQ(mock.readerBackendStatus().effective, RA_ReaderMode::Blocking);
  EXPECT_EQ(mock.readerBackendStatus().fallbackReason, RedisAdapter::ReaderFallbackReason::None);
  EXPECT_EQ(RA_Time("1-2").id(), "1-2");
  EXPECT_EQ(RA_Time(-3), RA_INVALID_PAYLOAD);
  EXPECT_EQ(RA_Time(-2), RA_REJECTED);
  EXPECT_NE(RA_NOT_CONNECTED, RA_REJECTED);
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

TEST(MockStreams, PipelineRecordsWritesInOrderAndReturnsScriptedResults) {
  RedisAdapter mock;
  const std::array<uint8_t, 3> bytes{{1, 2, 3}};
  const std::vector<uint8_t> more{4, 5};
  auto pipeline = mock.pipeline();
  EXPECT_EQ(pipeline.addSingleListView("a", bytes, {.time=RA_Time(100)}), 0u);
  EXPECT_EQ(pipeline.addSingleValue("b", RedisAdapter::Attrs{{"_", "x"}}, {.time=RA_Time(200)}), 1u);
  EXPECT_EQ(pipeline.addSingleList("a", more, {.time=RA_Time(300)}), 2u);
  mock.pipelineResults = {RA_Time(100), RA_REJECTED};
  const auto first = pipeline.exec();
  using Times = std::vector<RA_Time>;
  ASSERT_EQ(first.size(), 3u);
  EXPECT_EQ(first[0], Times{RA_Time(100)});
  EXPECT_EQ(first[1], Times{RA_REJECTED});
  EXPECT_EQ(first[2], Times{RA_Time(300)});
  pipeline.addSingleListView("a", more.data(), more.size(), {.time=RA_Time(400)});
  mock.pipelineResults = {RA_NOT_CONNECTED};
  EXPECT_EQ(pipeline.exec(), (std::vector<Times>{Times{RA_NOT_CONNECTED}}));
  ASSERT_EQ(mock.pipelineWrites.size(), 4u);
  EXPECT_EQ(mock.pipelineExecSizes, (std::vector<size_t>{3, 1}));
  EXPECT_EQ(mock.pipelineWrites[0].data, std::string("\x01\x02\x03", 3));
  EXPECT_TRUE(mock.pipelineWrites[0].view);
  EXPECT_EQ(mock.pipelineWrites[1].attrs.at("_"), "x");
  EXPECT_FALSE(mock.pipelineWrites[2].view);
  EXPECT_EQ(mock.pipelineWrites[3].exec, 1u);
  EXPECT_EQ(mock.pipelineWrites[3].data, std::string("\x04\x05", 2));
}
