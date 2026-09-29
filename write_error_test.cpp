#include "RedisAdapter.hpp"
#include <array>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;

static long long connectionCount(sw::redis::Redis& redis) {
  const auto stats = redis.info("stats");
  const std::string label = "total_connections_received:";
  const auto offset = stats.find(label);
  assert(offset != std::string::npos);
  return std::stoll(stats.substr(offset + label.size()));
}

int main() {
  RA_Options options;
  if (const auto* port = std::getenv("REDIS_ADAPTER_TEST_PORT")) options.cxn.port = std::stoi(port);
  if (std::getenv("REDIS_ADAPTER_FAULT_TEST")) {
    options.cxn.timeout = 100;
    RedisAdapter adapter("write-fault-" + std::to_string(getpid()), options);
    // The proxy forwards the write and discards its successful reply. Acceptance
    // is ambiguous to the adapter; the test driver verifies exactly one attempt.
    assert(adapter.addSingleDouble("lost-reply", 1.).value == RA_NOT_CONNECTED.value);
    std::this_thread::sleep_for(300ms);
    assert(adapter.addSingleDouble("healthy", 1.).ok());
    const auto partial = adapter.addValues<int>("mixed-transport",
        {{RA_Time(10000001), 1}, {RA_Time(10000002), 2}, {RA_Time(10000003), 3}}, 0);
    assert(partial.size() == 2);
    assert(partial[0].value == 10000001 && partial[1].value == 10000003);
    std::this_thread::sleep_for(300ms);
    assert(adapter.addSingleDouble("healthy", 2.).ok());
    std::cout << "connection loss after acceptance and mixed-batch recovery passed\n";
    return 0;
  }
  sw::redis::ConnectionOptions control;
  control.port = options.cxn.port;
  sw::redis::Redis redis(control);
  const auto base = "write-errors-" + std::to_string(getpid());
  RedisAdapter adapter(base, options);
  RA_ArgsAdd args;
  args.time = RA_Time(10000001);
  args.trim = 0;
  assert(adapter.addSingleDouble("double", 1., args).ok());
  const auto initialConnections = connectionCount(redis);
  assert(adapter.addSingleDouble("double", 2., args).value == RA_REJECTED.value);
  args.time = RA_Time(10000000);
  assert(adapter.addSingleDouble("double", 3., args).value == RA_REJECTED.value);
  args.time = RA_Time(10000002);
  assert(adapter.addSingleDouble("double", 4., args).ok());
  assert(redis.xlen("{" + base + "}:double") == 2);
  // Rejected IDs are not silently replaced with '*' or a newer host timestamp.
  args.time = RA_Time(10000001);
  assert(adapter.addSingleDouble("double", 5., args).err() == RA_REJECTED.err());

  // Cover all scalar/array paths, with and without MAXLEN in XADD.
  for (const auto trim : {0u, 1u}) {
    args.trim = trim;
    const auto suffix = std::to_string(trim);
    const RedisAdapter::Attrs attrs{{"_", "value"}};
    const std::vector<int> vector{1, 2};
    const std::array<int, 2> array{{1, 2}};
    assert(adapter.addSingleValue("int" + suffix, 1, args).ok());
    assert(adapter.addSingleValue("int" + suffix, 2, args).value == RA_REJECTED.value);
    assert(adapter.addSingleValue("attrs" + suffix, attrs, args).ok());
    assert(adapter.addSingleValue("attrs" + suffix, attrs, args).value == RA_REJECTED.value);
    assert(adapter.addSingleList("vector" + suffix, vector, args).ok());
    assert(adapter.addSingleList("vector" + suffix, vector, args).value == RA_REJECTED.value);
    assert(adapter.addSingleList("array" + suffix, array, args).ok());
    assert(adapter.addSingleList("array" + suffix, array, args).value == RA_REJECTED.value);
  }

  const auto checkBatch = [&](const auto& write) {
    assert(write(10000001, 10000002).size() == 2);
    assert(write(10000001, 10000000).empty());
    const auto partial = write(10000001, 10000003);
    assert(partial.size() == 1 && partial.front().value == 10000003);
  };
  checkBatch([&](int64_t first, int64_t second) {
    return adapter.addValues<int>("batch-int", {{RA_Time(first), 1}, {RA_Time(second), 2}}, 10);
  });
  checkBatch([&](int64_t first, int64_t second) {
    return adapter.addValues<RedisAdapter::Attrs>("batch-attrs",
        {{RA_Time(first), {{"_", "one"}}}, {RA_Time(second), {{"_", "two"}}}}, 10);
  });
  checkBatch([&](int64_t first, int64_t second) {
    return adapter.addLists<int>("batch-list",
        {{RA_Time(first), {1, 2}}, {RA_Time(second), {3, 4}}}, 10);
  });
  assert(adapter.addValues<int>("empty", {}, 10).empty());
  assert(adapter.addValues<RedisAdapter::Attrs>("empty", {}, 10).empty());
  assert(adapter.addLists<int>("empty", {}, 10).empty());

  redis.set("{" + base + "}:wrong-type", "string");
  assert(adapter.addSingleDouble("wrong-type", 1.).value == RA_REJECTED.value);
  assert(adapter.addValues<int>("wrong-type", {{RA_Time(1), 1}}, 0).empty());
  std::this_thread::sleep_for(300ms); // an erroneous async reconnect would create new connections
  assert(connectionCount(redis) == initialConnections);
  assert(adapter.connected());

  RedisConnection raw(options.cxn);
  RedisConnection::CommandStatus status;
  assert(raw.xtrim("{" + base + "}:wrong-type", 1, true, &status) == -1);
  assert(status == RedisConnection::CommandStatus::Rejected);
  assert(raw.xtrim("{" + base + "}:missing", 1, true, &status) == 0);
  assert(status == RedisConnection::CommandStatus::Accepted);

  // Port zero cannot be a listening TCP endpoint; this is a real connection
  // failure, distinct from all the healthy-server rejections above.
  options.cxn.port = 0;
  options.cxn.timeout = 20;
  RedisAdapter unavailable(base + "-unavailable", options);
  assert(unavailable.addSingleDouble("value", 1.).value == RA_NOT_CONNECTED.value);
  assert(unavailable.addValues<int>("values", {{RA_Time(1), 1}}, 0).empty());
  std::cout << "single/batch rejection, partial results, no spurious reconnect and transport errors passed\n";
}
