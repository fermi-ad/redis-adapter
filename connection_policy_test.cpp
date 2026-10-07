#include "RedisConnection.hpp"
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <map>
#include <thread>
#include <unordered_map>
#include <unistd.h>

using namespace std::chrono_literals;
using Fields = std::unordered_map<std::string, std::string>;
using Entries = std::vector<std::pair<std::string, Fields>>;
using Streams = std::unordered_map<std::string, Entries>;

class ConnectionPolicy : public testing::Test {
protected:
  RedisConnection::Options options;
  void SetUp() override {
    const auto* port = std::getenv("REDIS_ADAPTER_TEST_PORT");
    if (!port || !std::getenv("REDIS_ADAPTER_ISOLATED_TEST")) {
      GTEST_SKIP() << "Use scripts/run-test-redis.py for a private Redis fixture";
    }
    options.port = std::stoi(port);
    options.size = 1;
    options.timeout = 100;
  }
};

TEST_F(ConnectionPolicy, IdleReadsKeepTheReaderConnectionUsable) {
  RedisConnection connection(options);
  std::map<std::string, std::string> cursor{{"policy-idle", "0-0"}};
  for (int iteration = 0; iteration < 5; ++iteration) {
    Streams values;
    EXPECT_TRUE(connection.xreadMultiBlock(cursor.begin(), cursor.end(), 100,
                                         std::inserter(values, values.end())));
    EXPECT_TRUE(values.empty());
    EXPECT_TRUE(connection.ping());
  }
}

TEST_F(ConnectionPolicy, FailedReplacementRetainsTheEstablishedClients) {
  RedisConnection connection(options);
  ASSERT_TRUE(connection.ping());
  auto rejected = options;
  rejected.path = std::string(std::getenv("REDIS_ADAPTER_TEST_SOCKET")) + "-missing";
  rejected.connectTimeout = 50;
  EXPECT_FALSE(connection.connect(rejected));
  EXPECT_TRUE(connection.ping());
  std::map<std::string, std::string> cursor{{"policy-replacement", "0-0"}};
  Streams values;
  EXPECT_TRUE(connection.xreadMultiBlock(cursor.begin(), cursor.end(), 50,
                                       std::inserter(values, values.end())));
}

TEST_F(ConnectionPolicy, EightBlockedReadersDoNotConsumeTheSingleCommandConnection) {
  constexpr unsigned readers = 8;
  RedisConnection connection(options, readers);
  sw::redis::ConnectionOptions controlOptions;
  controlOptions.host = "127.0.0.1";
  controlOptions.port = options.port;
  sw::redis::Redis control(controlOptions);
  const auto prefix = "policy-" + std::to_string(getpid()) + "-";
  std::atomic<bool> running{true};
  std::atomic<unsigned> failures{0};
  std::vector<std::thread> threads;
  struct JoinReaders {
    std::atomic<bool>& running;
    std::vector<std::thread>& threads;
    void stop() {
      running = false;
      for (auto& thread : threads) if (thread.joinable()) thread.join();
    }
    ~JoinReaders() { stop(); }
  } joined{running, threads};
  for (unsigned index = 0; index < readers; ++index) {
    threads.emplace_back([&, index] {
      std::map<std::string, std::string> cursor{{prefix + std::to_string(index), "0-0"}};
      while (running) {
        Streams values;
        if (!connection.xreadMultiBlock(cursor.begin(), cursor.end(), 100,
                                        std::inserter(values, values.end()))) ++failures;
      }
    });
  }
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  unsigned observed = 0;
  do {
    const auto list = control.command<std::string>("CLIENT", "LIST");
    observed = 0;
    for (size_t pos = 0; (pos = list.find("cmd=xread", pos)) != std::string::npos; ++pos) ++observed;
    if (observed == readers) break;
    std::this_thread::sleep_for(5ms);
  } while (std::chrono::steady_clock::now() < deadline);
  EXPECT_EQ(observed, readers);
  const auto started = std::chrono::steady_clock::now();
  const Fields fields{{"_", "value"}};
  for (unsigned index = 0; index < 100; ++index) {
    EXPECT_FALSE(connection.xadd(prefix + "writes", "*", fields.begin(), fields.end()).empty());
    EXPECT_TRUE(connection.ping());
  }
  EXPECT_LT(std::chrono::steady_clock::now() - started, 1s);
  joined.stop();
  EXPECT_EQ(failures.load(), 0u);
  control.del(prefix + "writes");
}

TEST_F(ConnectionPolicy, ZeroCommandDeadlineStillAllowsFinitePhysicalReadCycles) {
  options.timeout = 0;
  RedisConnection connection(options);
  std::map<std::string, std::string> cursor{{"policy-zero-timeout", "0-0"}};
  Streams values;
  const auto started = std::chrono::steady_clock::now();
  EXPECT_TRUE(connection.xreadMultiBlock(cursor.begin(), cursor.end(), 0,
                                       std::inserter(values, values.end())));
  EXPECT_LT(std::chrono::steady_clock::now() - started, 2s);
  EXPECT_TRUE(connection.ping());
}
