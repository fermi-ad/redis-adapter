#include "RedisAdapter.hpp"
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <unistd.h>

using namespace std::chrono_literals;

struct Observations {
  std::mutex mutex;
  std::condition_variable changed;
  std::vector<RedisAdapter::StreamEntry> entries;
  void append(const RedisAdapter::StreamBatch& batch) {
    std::lock_guard<std::mutex> guard(mutex);
    entries.insert(entries.end(), batch.begin(), batch.end());
    changed.notify_all();
  }
  bool waitFor(const std::string& id) {
    std::unique_lock<std::mutex> lock(mutex);
    return changed.wait_for(lock, 3s, [&] {
      for (const auto& entry : entries) if (entry.first == id) return true;
      return false;
    });
  }
  size_t size() {
    std::lock_guard<std::mutex> guard(mutex);
    return entries.size();
  }
};

int main() {
  using RA = RedisAdapter;
  double scalar = 9.;
  assert(!RA::decodeScalar<double>({{"_", "x"}}, scalar));
  assert(scalar == 9.);
  assert(!RA::decodeScalar<double>({{"_", std::string(16, '\0')}}, scalar));
  std::vector<double> array{9.};
  assert(!RA::decodeArray<double>({{"_", "x"}}, array));
  assert(array == std::vector<double>{9.});
  assert(RA::decodeArray<double>({{"_", ""}}, array) && array.empty());
  assert(!RA::decodeArray<double>({}, array));
  assert(!RA::decodeArray<double>({{"_", std::string(16, '\0')}}, array, 8));
  bool flag = false;
  assert(!RA::decodeScalar<bool>({{"_", std::string(1, '\2')}}, flag));
  assert(RA::compareStreamIds("1-18446744073709551615", "2-0") < 0);
  assert(RA::compareStreamIds("123-999999999999", "123-1000000000000") < 0);
  assert(RA_Time("18446744073709551615-1").value == 0);
  assert(RA_Time("12x-1").value == 0);
  bool rejected = false;
  try { RA::compareStreamIds("1-2x", "1-2"); } catch (const std::invalid_argument&) { rejected = true; }
  assert(rejected);

  RA_Options options;
  if (const auto* port = std::getenv("REDIS_ADAPTER_TEST_PORT")) options.cxn.port = std::stoi(port);
  const auto base = "lifecycle-" + std::to_string(getpid());
  Observations first, second, gap, exact, survived;
  RA adapter(base, options);
  RA producer(base, options);
  assert(adapter.connected());
  const auto seed = producer.addSingleDouble("shared", 1.);
  assert(seed.ok());
  const auto snapshot = adapter.getStreamSnapshot("shared");
  assert(snapshot.connected && snapshot.id == seed.id());
  auto a = adapter.subscribeStream("shared", [&](const auto&, const auto&, const auto& entries) {
    first.append(entries);
  }, snapshot.id);
  auto b = adapter.subscribeStream("shared", [&](const auto&, const auto&, const auto& entries) {
    second.append(entries);
  }, snapshot.id);
  auto two = producer.addSingleDouble("shared", 2.);
  assert(first.waitFor(two.id()) && second.waitFor(two.id()));
  assert(first.size() == 1 && second.size() == 1);
  a.reset();
  const auto three = producer.addSingleDouble("shared", 3.);
  assert(second.waitFor(three.id()));
  assert(first.size() == 1);
  {
    auto rejectedStage = adapter.subscribeStream("shared", [](const auto&, const auto&, const auto&) {}, three.id());
  }
  const auto four = producer.addSingleDouble("shared", 4.);
  assert(second.waitFor(four.id()));

  producer.addSingleDouble("gap", 1.);
  const auto before = adapter.getStreamSnapshot("gap");
  const auto after = producer.addSingleDouble("gap", 2.);
  auto gapReader = adapter.subscribeStream("gap", [&](const auto&, const auto&, const auto& entries) {
    gap.append(entries);
  }, before.id);
  assert(gap.waitFor(after.id()));

  RedisConnection connection(options.cxn);
  RA::Attrs fields{{"_", "raw"}};
  const auto exactKey = "{" + base + "}:exact";
  assert(connection.xadd(exactKey, "1-18446744073709551615", fields.begin(), fields.end()) == "1-18446744073709551615");
  auto exactSnapshot = adapter.getStreamSnapshot("exact");
  assert(exactSnapshot.id == "1-18446744073709551615");
  auto exactReader = adapter.subscribeStream("exact", [&](const auto&, const auto&, const auto& entries) {
    exact.append(entries);
  }, exactSnapshot.id);
  assert(connection.xadd(exactKey, "2-0", fields.begin(), fields.end()) == "2-0");
  assert(exact.waitFor("2-0"));

  producer.addSingleList<double>("empty", std::vector<double>{});
  std::vector<double> empty{1.};
  assert(adapter.getSingleList<double>("empty", empty).ok() && empty.empty());

  auto throwing = adapter.subscribeStream("errors", [](const auto&, const auto&, const auto&) {
    throw std::runtime_error("intentional callback exception");
  }, "0-0");
  auto following = adapter.subscribeStream("errors", [&](const auto&, const auto&, const auto& entries) {
    survived.append(entries);
  }, "0-0");
  const auto healthy = producer.addSingleDouble("errors", 7.);
  assert(survived.waitFor(healthy.id()));

  RA::ReaderHandle orphan;
  {
    RA temporary(base + "-temporary", options);
    orphan = temporary.subscribeStream("key", [](const auto&, const auto&, const auto&) {}, "0-0");
  }
  orphan.reset();
  std::cout << "owned subscriptions, exact cursors, safe decoding and callback lifetime passed\n";
}
