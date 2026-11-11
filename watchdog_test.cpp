// Watchdog behaviour against a live Redis: the field always carries an expiration, an expired
// field is re-created, a custom watchdog hash is honoured, and the field disappears once the
// adapter is gone. Fault injection between the write and the expiration is covered by
// watchdog_failure_test.py.
#include "RedisAdapter.hpp"
#include <gtest/gtest.h>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;

namespace {

bool waitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds limit = 3000ms) {
  const auto end = std::chrono::steady_clock::now() + limit;
  do { if (predicate()) return true; std::this_thread::sleep_for(20ms); } while (std::chrono::steady_clock::now() < end);
  return predicate();
}

class Watchdog : public testing::Test {
protected:
  RA_Options options;
  std::string base;
  std::unique_ptr<sw::redis::Redis> direct;   // checks Redis independently of the adapter

  void SetUp() override {
    const auto* port = std::getenv("REDIS_ADAPTER_TEST_PORT");
    if (!port || !std::getenv("REDIS_ADAPTER_ISOLATED_TEST")) GTEST_SKIP() << "Use the private Redis test fixture";
    options.cxn.port = std::stoi(port);
    base = std::string("watchdog-") + testing::UnitTest::GetInstance()->current_test_info()->name() +
           "-" + std::to_string(getpid());
    sw::redis::ConnectionOptions co;
    co.host = options.cxn.host;
    co.port = options.cxn.port;
    direct = std::make_unique<sw::redis::Redis>(co);
    direct->del(key("watchdog"));
    direct->del(key("service_watchdog"));
  }

  void TearDown() override {
    if (direct) { direct->del(key("watchdog")); direct->del(key("service_watchdog")); }
  }

  std::string key(const std::string& sub) const { return "{" + base + "}:" + sub; }

  bool present(const std::string& sub, const std::string& field) { return direct->hexists(key(sub), field); }

  // Remaining time to live of a hash field in ms: -1 no expiration, -2 no such field.
  long long pttl(const std::string& sub, const std::string& field) {
    auto r = direct->command<std::vector<long long>>("hpttl", key(sub), "FIELDS", "1", field);
    return r.empty() ? -2 : r.front();
  }
};

}  // namespace

TEST_F(Watchdog, AddSetsTheFieldWithItsExpiration) {
  RedisAdapter redis(base, options);
  ASSERT_TRUE(redis.addWatchdog("SPOT", 1));
  EXPECT_TRUE(present("watchdog", "SPOT"));
  auto ttl = pttl("watchdog", "SPOT");
  EXPECT_GT(ttl, 0);
  EXPECT_LE(ttl, 1000);
}

TEST_F(Watchdog, AddGivesAnExistingFieldAnExpiration) {
  direct->hset(key("watchdog"), "SPOT", "stale");   // e.g. left behind without an expiration
  ASSERT_EQ(pttl("watchdog", "SPOT"), -1);
  RedisAdapter redis(base, options);
  ASSERT_TRUE(redis.addWatchdog("SPOT", 1));
  EXPECT_GT(pttl("watchdog", "SPOT"), 0);
  EXPECT_TRUE(waitUntil([&] { return !present("watchdog", "SPOT"); }, 2000ms));
}

TEST_F(Watchdog, PetOnlyRenewsAnExistingField) {
  RedisAdapter redis(base, options);
  EXPECT_FALSE(redis.petWatchdog("NOPE", 1));
  EXPECT_FALSE(present("watchdog", "NOPE"));
  ASSERT_TRUE(redis.addWatchdog("SPOT", 1));
  EXPECT_TRUE(redis.petWatchdog("SPOT", 1));
}

TEST_F(Watchdog, AutomaticWatchdogKeepsItsExpiration) {
  options.dogname = "dog";
  RedisAdapter redis(base, options);
  ASSERT_TRUE(waitUntil([&] { return present("watchdog", "dog"); }));
  for (int i = 0; i < 15; ++i) {   // across several renewals
    auto ttl = pttl("watchdog", "dog");
    EXPECT_GT(ttl, 0) << "sample " << i;
    EXPECT_LE(ttl, 1000) << "sample " << i;
    std::this_thread::sleep_for(200ms);
  }
}

TEST_F(Watchdog, ExpiredFieldIsRecreated) {
  options.dogname = "dog";
  RedisAdapter redis(base, options);
  ASSERT_TRUE(waitUntil([&] { return present("watchdog", "dog"); }));
  direct->del(key("watchdog"));   // as if it expired, or Redis restarted without persistence
  EXPECT_TRUE(waitUntil([&] { return present("watchdog", "dog"); }, 2000ms));
  EXPECT_GT(pttl("watchdog", "dog"), 0);
}

TEST_F(Watchdog, RecreatedAfterTheConnectionIsKilled) {
  options.dogname = "dog";
  RedisAdapter redis(base, options);
  ASSERT_TRUE(waitUntil([&] { return present("watchdog", "dog"); }));
  direct->del(key("watchdog"));
  direct->command("client", "kill", "type", "normal", "skipme", "yes");
  EXPECT_TRUE(waitUntil([&] { return present("watchdog", "dog"); }, 4000ms));
  EXPECT_GT(pttl("watchdog", "dog"), 0);
}

TEST_F(Watchdog, CustomHashIsUsedInsteadOfTheDefault) {
  options.dogname = "data-mover-service";
  options.dogkey = "service_watchdog";
  RedisAdapter redis(base, options);
  ASSERT_TRUE(waitUntil([&] { return present("service_watchdog", "data-mover-service"); }));
  EXPECT_EQ(direct->exists(key("watchdog")), 0);
  EXPECT_EQ(redis.getWatchdogs(), std::vector<std::string>{"data-mover-service"});
  EXPECT_TRUE(redis.addWatchdog("SPOT", 1));
  EXPECT_TRUE(present("service_watchdog", "SPOT"));
}

TEST_F(Watchdog, FieldDisappearsOnceTheAdapterIsGone) {
  options.dogname = "dog";
  {
    RedisAdapter redis(base, options);
    ASSERT_TRUE(waitUntil([&] { return present("watchdog", "dog"); }));
  }
  EXPECT_TRUE(waitUntil([&] { return !present("watchdog", "dog"); }, 1500ms));
}

TEST_F(Watchdog, DestroyingRightAfterConstructionIsClean) {
  options.dogname = "dog";
  for (int i = 0; i < 20; ++i) {
    RedisAdapter redis(base, options);
    ASSERT_TRUE(redis.connected());
  }
  EXPECT_TRUE(waitUntil([&] { return !present("watchdog", "dog"); }, 1500ms));
}
