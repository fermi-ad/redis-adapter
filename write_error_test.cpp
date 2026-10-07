#include "RedisAdapter.hpp"
#include <gtest/gtest.h>
#include <array>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;
using RA = RedisAdapter;

static RA_Options testOptions() {
  RA_Options options;
  const auto* port = std::getenv("REDIS_ADAPTER_TEST_PORT");
  if (!port || !std::getenv("REDIS_ADAPTER_ISOLATED_TEST")) throw std::runtime_error("private Redis fixture required");
  options.cxn.port = std::stoi(port);
  return options;
}
static std::string uniqueBase() {
  return "write-review-" + std::to_string(getpid()) + "-" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}
static void expect(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

class WriteErrors : public testing::Test {
protected:
  RA_Options options;
  std::string base;
  std::unique_ptr<sw::redis::Redis> redis;
  void SetUp() override {
    if (!std::getenv("REDIS_ADAPTER_ISOLATED_TEST")) GTEST_SKIP() << "Use the private Redis fixture";
    options = testOptions();
    base = uniqueBase();
    sw::redis::ConnectionOptions control;
    control.host = "127.0.0.1"; control.port = options.cxn.port;
    redis = std::make_unique<sw::redis::Redis>(control);
  }
  void TearDown() override {
    if (!redis) return;
    try {
      std::vector<std::string> keys;
      redis->keys("*" + base + "*", std::back_inserter(keys));
      if (!keys.empty()) redis->del(keys.begin(), keys.end());
    } catch (...) {}
  }
  std::string key(const std::string& sub) const { return "{" + base + "}:" + sub; }
};

TEST_F(WriteErrors, SingleScalarAndArrayPathsPreserveRejectedIds) {
  RA adapter(base, options);
  RA_ArgsAdd args; args.time = RA_Time(10000001);
  for (const auto trim : {0u, 1u}) {
    args.trim = trim;
    const auto suffix = std::to_string(trim);
    const RA::Attrs attrs{{"_", "value"}};
    const std::vector<int> vector{1, 2};
    const std::array<int, 2> array{{1, 2}};
    EXPECT_TRUE(adapter.addSingleDouble("double" + suffix, 1., args).ok());
    EXPECT_EQ(adapter.addSingleDouble("double" + suffix, 2., args), RA_REJECTED);
    EXPECT_TRUE(adapter.addSingleValue("int" + suffix, 1, args).ok());
    EXPECT_EQ(adapter.addSingleValue("int" + suffix, 2, args), RA_REJECTED);
    EXPECT_TRUE(adapter.addSingleValue("attrs" + suffix, attrs, args).ok());
    EXPECT_EQ(adapter.addSingleValue("attrs" + suffix, attrs, args), RA_REJECTED);
    EXPECT_TRUE(adapter.addSingleList("vector" + suffix, vector, args).ok());
    EXPECT_EQ(adapter.addSingleList("vector" + suffix, vector, args), RA_REJECTED);
    EXPECT_TRUE(adapter.addSingleList("array" + suffix, array, args).ok());
    EXPECT_EQ(adapter.addSingleList("array" + suffix, array, args), RA_REJECTED);
    EXPECT_EQ(redis->xlen(key("double" + suffix)), 1);
  }
}

TEST_F(WriteErrors, BatchesKeepAcceptedItemsSkipRejectionsAndCanTrimExactly) {
  RA adapter(base, options);
  const auto checkBatch = [&](const auto& write) {
    EXPECT_EQ(write(10000001, 10000002).size(), 2u);
    EXPECT_TRUE(write(10000001, 10000000).empty());
    const auto partial = write(10000001, 10000003);
    ASSERT_EQ(partial.size(), 1u);
    EXPECT_EQ(partial.front().value, 10000003);
  };
  checkBatch([&](int64_t first, int64_t second) { return adapter.addValues<int>("int", {{RA_Time(first), 1}, {RA_Time(second), 2}}, 10); });
  checkBatch([&](int64_t first, int64_t second) { return adapter.addValues<RA::Attrs>("attrs", {{RA_Time(first), {{"_", "one"}}}, {RA_Time(second), {{"_", "two"}}}}, 10); });
  checkBatch([&](int64_t first, int64_t second) { return adapter.addLists<int>("list", {{RA_Time(first), {1, 2}}, {RA_Time(second), {3, 4}}}, 10); });
  EXPECT_TRUE(adapter.addValues<int>("empty", {}, 10).empty());
  EXPECT_TRUE(adapter.addValues<RA::Attrs>("empty", {}, 10).empty());
  EXPECT_TRUE(adapter.addLists<int>("empty", {}, 10).empty());
  for (int64_t i = 1; i <= 150; ++i) ASSERT_EQ(adapter.addValues<int>("exact", {{RA_Time(i * 1000000), 1}}, 1, false).size(), 1u);
  EXPECT_EQ(redis->xlen(key("exact")), 1);
  for (int64_t i = 1; i <= 150; ++i) ASSERT_EQ(adapter.addLists<int>("exact-list", {{RA_Time(i * 1000000), {1}}}, 1, false).size(), 1u);
  EXPECT_EQ(redis->xlen(key("exact-list")), 1);
}

TEST_F(WriteErrors, LowLevelResultsKeepErrorTextAndStatusPointersAreUnambiguous) {
  RedisConnection connection(options.cxn);
  const RA::Attrs attrs{{"_", "value"}};
  ASSERT_EQ(connection.xaddResult(key("raw"), "1-0", attrs.begin(), attrs.end()).status, RedisConnection::CommandStatus::Accepted);
  const auto rejected = connection.xaddResult(key("raw"), "1-0", attrs.begin(), attrs.end());
  EXPECT_EQ(rejected.status, RedisConnection::CommandStatus::Rejected);
  EXPECT_FALSE(rejected.error.empty());
  EXPECT_FALSE(rejected.refreshConnection);
  redis->set(key("wrong"), "wrong type");
  RedisConnection::CommandStatus status = RedisConnection::CommandStatus::Accepted;
  EXPECT_EQ(connection.xtrim(key("wrong"), 1, &status), -1);
  EXPECT_EQ(status, RedisConnection::CommandStatus::Rejected);
  const auto missing = connection.xtrimResult(key("missing"), 1, false);
  EXPECT_EQ(missing.status, RedisConnection::CommandStatus::Accepted);
  EXPECT_EQ(missing.count, 0);
}

TEST_F(WriteErrors, UnavailableTransportIsDistinctFromRejection) {
  options.cxn.port = 0; options.cxn.timeout = 20; options.cxn.connectTimeout = 20;
  RA adapter(base, options);
  EXPECT_EQ(adapter.addSingleDouble("value", 1.), RA_NOT_CONNECTED);
  EXPECT_TRUE(adapter.addValues<int>("values", {{RA_Time(1), 1}}, 0).empty());
}

static int proxyScenario(const std::string& mode) {
  const auto options = testOptions();
  const auto base = uniqueBase();
  sw::redis::ConnectionOptions controlOptions;
  controlOptions.host = "127.0.0.1";
  controlOptions.port = std::stoi(std::getenv("REDIS_ADAPTER_CONTROL_PORT"));
  sw::redis::Redis control(controlOptions);
  struct Keys {
    sw::redis::Redis& control; std::string base;
    ~Keys() { try { std::vector<std::string> keys; control.keys("*" + base + "*", std::back_inserter(keys)); if (!keys.empty()) control.del(keys.begin(), keys.end()); } catch (...) {} }
  } cleanup{control, base};
  if (mode == "constructor") {
    RA adapter(base, options);
    expect(adapter.addSingleDouble("healthy", 1.).ok(), "constructor must publish a usable standalone connection");
  } else if (mode == "cluster-refusal") {
    RedisConnection connection(options.cxn);
    expect(!connection.connect(options.cxn), "detected Cluster mode must remain unsupported on reconnect");
    const RA::Attrs attrs{{"_", "value"}};
    expect(connection.xaddResult("{" + base + "}:unsupported", "1-0", attrs.begin(), attrs.end()).status ==
           RedisConnection::CommandStatus::Unavailable, "unsupported Cluster connection must not publish a writer");
  } else if (mode == "fault") {
    { RA adapter(base, options); expect(adapter.addSingleDouble("lost-reply", 1.) == RA_NOT_CONNECTED, "lost reply must remain ambiguous"); }
    {
      RA adapter(base, options);
      expect(adapter.addSingleDouble("healthy", 1.).ok(), "healthy write after recovery failed");
      const auto prefix = adapter.addValues<int>("mixed-transport", {{RA_Time(10000001), 1}, {RA_Time(10000002), 2}, {RA_Time(10000003), 3}}, 0);
      expect(prefix.size() == 1 && prefix.front().value == 10000001, "batch must stop at unavailable item and retain accepted prefix");
      expect(control.xlen("{" + base + "}:mixed-transport") == 2, "ambiguous item was accepted once; later item must not be sent");
    }
    { RA adapter(base, options); expect(adapter.addSingleDouble("healthy", 2.).ok(), "future write after mixed-batch failure failed"); }
  } else if (mode == "trim") {
    {
      RA adapter(base, options);
      const auto accepted = adapter.addValues<int>("trim-lost", {{RA_Time(10000001), 1}, {RA_Time(10000002), 2}, {RA_Time(10000003), 3}}, 1, false);
      expect(accepted.size() == 3, "final trim failure must preserve accepted timestamps");
    }
    { RA adapter(base, options); expect(adapter.addSingleDouble("healthy", 1.).ok(), "future write after final trim failure failed"); }
  } else if (mode == "readonly") {
    RA adapter(base, options);
    RA_ArgsAdd args; args.time = RA_Time(10000001); args.trim = 0;
    expect(adapter.addSingleDouble("readonly", 1., args) == RA_REJECTED, "READONLY is a known rejection");
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    bool accepted = false;
    do {
      std::this_thread::sleep_for(50ms);
      accepted = adapter.addSingleDouble("readonly", 1., args).ok();
    } while (!accepted && std::chrono::steady_clock::now() < deadline);
    expect(accepted, "READONLY must refresh connections for later writes");
    expect(control.xlen("{" + base + "}:readonly") == 1, "known refusal was not forwarded or replayed");
  } else if (mode == "rejection") {
    {
      RA adapter(base, options);
      RA_ArgsAdd args; args.time = RA_Time(10000001); args.trim = 0;
      expect(adapter.addSingleDouble("duplicate", 1., args).ok(), "seed rejected");
      expect(adapter.addSingleDouble("duplicate", 2., args) == RA_REJECTED, "duplicate must be rejected");
      control.set("{" + base + "}:wrong", "wrong type");
      expect(adapter.addSingleDouble("wrong", 1.) == RA_REJECTED, "wrong type must be rejected");
      for (unsigned i = 0; i < 20; ++i) {
        const auto snapshot = adapter.getStreamSnapshot("wrong");
        expect(snapshot.rejected && snapshot.id == "$", "snapshot rejection cannot become replay");
      }
      expect(adapter.addValues<int>("duplicate", {{RA_Time(10000001), 1}}, 1).empty(), "all-rejected batch must remain empty");
    }
    const auto user = base + "-acl";
    struct User { sw::redis::Redis& control; std::string user; ~User() { try { control.command<void>("ACL", "DELUSER", user); } catch (...) {} } } userCleanup{control, user};
    control.command<void>("ACL", "SETUSER", user, "on", "nopass", "~*", "+ping", "+xadd");
    auto aclOptions = options; aclOptions.cxn.user = user;
    {
      RA adapter(base + "-acl", aclOptions);
      const auto accepted = adapter.addValues<int>("trim-denied", {{RA_Time(10000001), 1}, {RA_Time(10000002), 2}}, 1, false);
      expect(accepted.size() == 2, "trim rejection must not undo accepted writes");
      expect(control.xlen("{" + base + "-acl}:trim-denied") == 2, "denied trim unexpectedly removed data");
    }
  } else throw std::runtime_error("unknown proxy scenario");
  std::cout << mode << " proxy scenario passed\n";
  return 0;
}

int main(int argc, char** argv) {
  try {
    if (const auto* mode = std::getenv("REDIS_ADAPTER_WRITE_SCENARIO")) return proxyScenario(mode);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n'; return 1;
  }
}
