#include "RedisAdapter.hpp"
#include <gtest/gtest.h>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <sstream>
#include <thread>
#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace std::chrono_literals;
using RA = RedisAdapter;

static bool eventually(const std::function<bool()>& predicate) {
  const auto end = std::chrono::steady_clock::now() + 5s;
  do { if (predicate()) return true; std::this_thread::sleep_for(5ms); }
  while (std::chrono::steady_clock::now() < end);
  return predicate();
}
#define EVENTUALLY(...) ASSERT_TRUE(eventually([&] { return (__VA_ARGS__); })) << #__VA_ARGS__

TEST(ReaderWake, CoalescesNotificationsButRetainsArrivalsDuringAReadAndDrainsFairly) {
  RedisReaderWake wake;
  wake.reset({"hot", "quiet"});
  auto initial = wake.take(64);
  for (const auto& ticket : initial) wake.finish(ticket, 0, 2);
  for (int index = 0; index < 10000; ++index) wake.mark("hot", true);
  auto reading = wake.take(64);
  ASSERT_EQ(reading.size(), 1u);
  wake.mark("hot", true); // arrives after the command took its generation
  wake.mark("quiet", true);
  wake.finish(reading.front(), 1, 2);
  auto next = wake.take(1);
  ASSERT_EQ(next.size(), 1u);
  EXPECT_EQ(next.front().key, "quiet");
  wake.finish(next.front(), 0, 2);
  next = wake.take(1);
  ASSERT_EQ(next.size(), 1u);
  EXPECT_EQ(next.front().key, "hot");
  wake.finish(next.front(), 2, 2); // a full reply drains without another event
  next = wake.take(64);
  ASSERT_EQ(next.size(), 1u);
  wake.finish(next.front(), 0, 2);
  EXPECT_TRUE(wake.take(64).empty());
  wake.mark("hot", true); // also retain arrivals after completion
  EXPECT_EQ(wake.take(64).size(), 1u);
}

TEST(ReaderWake, BatchesAreBoundedAndReconciliationAllowsDelayedNotifications) {
  RedisReaderWake wake;
  std::vector<std::string> keys;
  for (int index = 0; index < 140; ++index) keys.push_back(std::to_string(index));
  wake.reset(keys);
  for (const auto expected : {64u, 64u, 12u}) {
    const auto batch = wake.take(64);
    ASSERT_EQ(batch.size(), expected);
    for (const auto& ticket : batch) wake.finish(ticket, 0, 64);
  }
  wake.mark("0", false, true);
  auto batch = wake.take(64);
  ASSERT_EQ(batch.size(), 1u);
  wake.finish(batch.front(), 1, 64);
  wake.mark("0", true);
  std::this_thread::sleep_for(270ms);
  EXPECT_FALSE(wake.missedNotification());
  batch = wake.take(64);
  for (const auto& ticket : batch) wake.finish(ticket, 0, 64);
  wake.mark("0", false, true);
  batch = wake.take(64);
  wake.finish(batch.front(), 1, 64);
  std::this_thread::sleep_for(270ms);
  EXPECT_TRUE(wake.missedNotification());
  EXPECT_FALSE(wake.missedNotification());
}

TEST(KeyspaceOptions, ValidatesReconciliationBeforeConnecting) {
  RA_Options options;
  options.readerReconcileMs = 0;
  EXPECT_THROW(RA("invalid", options), std::invalid_argument);
  options.readerReconcileMs = 99;
  EXPECT_THROW(RA("invalid", options), std::invalid_argument);
  options.readerReconcileMs = 60001;
  EXPECT_THROW(RA("invalid", options), std::invalid_argument);
}

struct Delivered {
  std::mutex mutex;
  std::vector<std::string> ids;
  size_t largest = 0;
  void append(const RA::StreamBatch& batch) {
    std::lock_guard<std::mutex> lock(mutex);
    largest = std::max(largest, batch.size());
    for (const auto& entry : batch) ids.push_back(entry.first);
  }
  size_t size() { std::lock_guard<std::mutex> lock(mutex); return ids.size(); }
  std::vector<std::string> snapshot() { std::lock_guard<std::mutex> lock(mutex); return ids; }
};

// An isolated test proxy can blackhole only the first subscriber's replies
// while command/read sockets keep working. Later subscriber connections pass
// normally, proving recovery requires replacing the stalled socket.
class StalledSubscriberProxy {
public:
  StalledSubscriberProxy(uint16_t upstream, bool stall) : upstream_(upstream), stall_(stall) {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listener_ < 0) throw std::runtime_error("cannot create private proxy socket");
    sockaddr_in address{}; address.sin_family = AF_INET; address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(listener_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) || ::listen(listener_, 16)) {
      ::close(listener_); throw std::runtime_error("cannot bind private proxy");
    }
    socklen_t size = sizeof(address);
    ::getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &size);
    port_ = ntohs(address.sin_port);
    acceptor_ = std::thread([this] {
      while (running_) {
        pollfd ready{listener_, POLLIN, 0};
        if (::poll(&ready, 1, 20) <= 0 || !(ready.revents & POLLIN)) continue;
        const auto client = ::accept(listener_, nullptr, nullptr);
        if (client >= 0) workers_.emplace_back([this, client] { forward(client); });
      }
    });
  }
  ~StalledSubscriberProxy() {
    running_ = false;
    acceptor_.join();
    ::close(listener_);
    for (auto& worker : workers_) worker.join();
  }
  uint16_t port() const { return port_; }
  void stall() { stall_ = true; }
  unsigned subscribers() const { return subscribers_; }
private:
  static void boundedSocket(int socket) {
    timeval deadline{0, 500000};
    ::setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &deadline, sizeof(deadline));
#ifdef SO_NOSIGPIPE
    int enabled = 1;
    ::setsockopt(socket, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif
  }
  static bool sendAll(int socket, const char* data, size_t length) {
    while (length) {
#ifdef MSG_NOSIGNAL
      const auto sent = ::send(socket, data, length, MSG_NOSIGNAL);
#else
      const auto sent = ::send(socket, data, length, 0);
#endif
      if (sent <= 0) return false;
      data += sent; length -= sent;
    }
    return true;
  }
  void forward(int client) {
    const auto upstream = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{}; address.sin_family = AF_INET; address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(upstream_);
    if (upstream < 0 || ::connect(upstream, reinterpret_cast<sockaddr*>(&address), sizeof(address))) {
      if (upstream >= 0) ::close(upstream);
      ::close(client); return;
    }
    boundedSocket(client); boundedSocket(upstream);
    bool identified = false, first = false;
    std::string commandTail;
    char buffer[8192];
    bool flowing = true;
    while (running_ && flowing) {
      pollfd ready[]{{client, POLLIN, 0}, {upstream, POLLIN, 0}};
      if (::poll(ready, 2, 20) <= 0) continue;
      for (unsigned index = 0; index < 2; ++index) {
        if (!(ready[index].revents & (POLLIN | POLLHUP | POLLERR))) continue;
        const auto size = ::recv(ready[index].fd, buffer, sizeof(buffer), 0);
        if (size <= 0) { flowing = false; break; }
        if (index == 0 && !identified) {
          commandTail.append(buffer, static_cast<size_t>(size));
          if (commandTail.find("\r\nSUBSCRIBE\r\n") != std::string::npos) {
            identified = true; first = subscribers_.fetch_add(1) == 0;
          }
          if (commandTail.size() > 64) commandTail.erase(0, commandTail.size() - 64);
        }
        if (index == 1 && first && stall_) continue;
        if (!sendAll(index == 0 ? upstream : client, buffer, static_cast<size_t>(size))) {
          flowing = false; break;
        }
      }
    }
    ::close(client); ::close(upstream);
  }
  uint16_t upstream_, port_ = 0;
  int listener_ = -1;
  std::atomic<bool> running_{true}, stall_;
  std::atomic<unsigned> subscribers_{0};
  std::thread acceptor_;
  std::vector<std::thread> workers_;
};

class KeyspaceReaders : public testing::Test {
protected:
  RA_Options options;
  std::string base, user, originalFlags;
  std::unique_ptr<sw::redis::Redis> control;
  RA::Attrs fields{{"_", "value"}};
  void SetUp() override {
    const auto* port = std::getenv("REDIS_ADAPTER_TEST_PORT");
    if (!port || !std::getenv("REDIS_ADAPTER_ISOLATED_TEST")) GTEST_SKIP() << "Use the private Redis fixture";
    options.cxn.port = std::stoi(port); options.cxn.timeout = 100;
    options.readerMode = RA_ReaderMode::Keyspace;
    options.readerReconcileMs = 100;
    options.readerBatchCount = 3;
    base = "keyspace-" + std::to_string(getpid()) + "-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    user = "reader-" + base;
    sw::redis::ConnectionOptions co; co.host = "127.0.0.1"; co.port = options.cxn.port;
    control = std::make_unique<sw::redis::Redis>(co);
    const auto config = control->command<std::unordered_map<std::string, std::string>>("CONFIG", "GET", "notify-keyspace-events");
    originalFlags = config.at("notify-keyspace-events");
    control->command<void>("CONFIG", "SET", "notify-keyspace-events", "Kt");
    control->command<void>("ACL", "SETUSER", user, "reset", "on", "nopass", "~{" + base + "}:*", "&__keyspace@0__:{" + base + "}:*", "+@all");
    options.cxn.user = user;
  }
  void TearDown() override {
    if (!control) return;
    try { control->command<void>("CONFIG", "SET", "notify-keyspace-events", originalFlags); } catch (...) {}
    try { control->command<void>("ACL", "DELUSER", user); } catch (...) {}
    try {
      std::vector<std::string> keys;
      control->keys("*" + base + "*", std::back_inserter(keys));
      if (!keys.empty()) control->del(keys.begin(), keys.end());
    } catch (...) {}
  }
  std::string key(const std::string& sub) const { return "{" + base + "}:" + sub; }
  void add(const std::string& sub, unsigned id) {
    EXPECT_EQ(control->xadd(key(sub), std::to_string(id) + "-0", fields.begin(), fields.end()), std::to_string(id) + "-0");
  }
  RA::ReaderHandle subscribe(RA& adapter, const std::string& sub, Delivered& seen, const std::string& after = "0-0") {
    return adapter.subscribeStream(sub, [&](const auto&, const auto&, const auto& batch) { seen.append(batch); }, after);
  }
  bool active(const RA& adapter) { return adapter.readerBackendStatus().effective == RA_ReaderMode::Keyspace; }
};

TEST_F(KeyspaceReaders, AcknowledgedSubscriptionCatchesUpAndDrainsEveryRetainedEntry) {
  Delivered seen;
  RA adapter(base, options);
  adapter.setDeferReaders(true);
  add("value", 1);
  auto handle = subscribe(adapter, "value", seen, "$");
  for (unsigned id = 2; id <= 40; ++id) add("value", id);
  EVENTUALLY(active(adapter));
  adapter.setDeferReaders(false);
  EVENTUALLY(seen.size() == 39);
  const auto ids = seen.snapshot();
  for (size_t index = 0; index < ids.size(); ++index) EXPECT_EQ(ids[index], std::to_string(index + 2) + "-0");
  EXPECT_LE(seen.largest, options.readerBatchCount);
  EXPECT_EQ(handle.status().cursor, "40-0");
  EXPECT_EQ(adapter.readerBackendStatus().fallbackReason, RA::ReaderFallbackReason::None);
}

TEST_F(KeyspaceReaders, NotificationConnectionWaitsForTheFirstStreamRegistration) {
  Delivered seen;
  RA adapter(base, options);
  const auto clients = [&] {
    std::vector<std::string> ids;
    std::istringstream lines(control->command<std::string>("CLIENT", "LIST"));
    for (std::string line; std::getline(lines, line);) {
      if (line.find("user=" + user + " ") == std::string::npos) continue;
      const auto start = line.find("id=");
      ids.push_back(line.substr(start + 3, line.find(' ', start) - start - 3));
    }
    std::sort(ids.begin(), ids.end());
    return ids;
  };
  ASSERT_TRUE(adapter.connected());
  const auto original = clients();
  ASSERT_EQ(original.size(), 1u);
  control->set(key("wrong"), "wrong type");
  for (unsigned index = 0; index < 20; ++index) EXPECT_TRUE(adapter.getStreamSnapshot("wrong").rejected);
  // Give an incorrectly eager background connection time to appear. Neither
  // a rejected snapshot nor an unused notification backend may replace/add it.
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(clients(), original);
  auto handle = subscribe(adapter, "value", seen);
  EVENTUALLY(active(adapter));
  add("value", 1);
  EVENTUALLY(seen.size() == 1);
  const auto registered = clients();
  EXPECT_NE(std::find(registered.begin(), registered.end(), original.front()), registered.end());
  EXPECT_GT(registered.size(), original.size());
}

TEST_F(KeyspaceReaders, MissingConfigurationFallsBackAndActualDeliveryEnablesIt) {
  control->command<void>("CONFIG", "SET", "notify-keyspace-events", "");
  Delivered seen;
  RA adapter(base, options);
  auto handle = subscribe(adapter, "value", seen);
  EVENTUALLY(adapter.readerBackendStatus().fallbackReason == RA::ReaderFallbackReason::NotificationsDisabled);
  add("value", 1);
  EVENTUALLY(seen.size() == 1);
  EXPECT_TRUE(handle.status().connected);
  control->command<void>("CONFIG", "SET", "notify-keyspace-events", "Kt");
  add("value", 2);
  EVENTUALLY(active(adapter));
  EVENTUALLY(seen.size() == 2);
}

TEST_F(KeyspaceReaders, DeniedConfigWaitsForEvidenceAndSilentDisableFallsBack) {
  control->command<void>("ACL", "SETUSER", user, "-config");
  Delivered seen;
  RA adapter(base, options);
  auto handle = subscribe(adapter, "value", seen);
  EVENTUALLY(adapter.readerBackendStatus().fallbackReason == RA::ReaderFallbackReason::UnverifiedNotifications);
  add("value", 1);
  EVENTUALLY(active(adapter));
  EVENTUALLY(seen.size() == 1);
  // Let the initial promotion catch-up finish before testing reconciliation.
  std::this_thread::sleep_for(250ms);
  control->command<void>("CONFIG", "SET", "notify-keyspace-events", "");
  add("value", 2);
  EVENTUALLY(seen.size() == 2);
  EVENTUALLY(adapter.readerBackendStatus().fallbackReason == RA::ReaderFallbackReason::MissedNotifications);
  control->command<void>("CONFIG", "SET", "notify-keyspace-events", "Kt");
  add("value", 3);
  EVENTUALLY(active(adapter));
  EVENTUALLY(seen.size() == 3);
  EXPECT_EQ(handle.status().epoch, 1u);
}

TEST_F(KeyspaceReaders, ChannelAclFallbackRetriesWithoutStoppingDataReads) {
  control->command<void>("ACL", "SETUSER", user, "resetchannels");
  Delivered seen;
  RA adapter(base, options);
  auto handle = subscribe(adapter, "value", seen);
  EVENTUALLY(adapter.readerBackendStatus().fallbackReason == RA::ReaderFallbackReason::SubscriptionRejected);
  add("value", 1);
  EVENTUALLY(seen.size() == 1);
  control->command<void>("ACL", "SETUSER", user, "&__keyspace@0__:{" + base + "}:*");
  EVENTUALLY(active(adapter));
  add("value", 2);
  EVENTUALLY(seen.size() == 2);
}

TEST_F(KeyspaceReaders, SubscriberOnlyDisconnectPreservesCursorsAndIdlePolling) {
  Delivered seen;
  RA adapter(base, options);
  auto handle = subscribe(adapter, "value", seen);
  EVENTUALLY(active(adapter));
  add("value", 1);
  EVENTUALLY(seen.size() == 1);
  std::string subscriberId;
  std::istringstream clients(control->command<std::string>("CLIENT", "LIST"));
  for (std::string line; std::getline(clients, line);) {
    if (line.find("user=" + user + " ") == std::string::npos || line.find("cmd=subscribe") == std::string::npos) continue;
    const auto start = line.find("id=");
    subscriberId = line.substr(start + 3, line.find(' ', start) - start - 3);
  }
  ASSERT_FALSE(subscriberId.empty());
  EXPECT_EQ(control->command<long long>("CLIENT", "KILL", "ID", subscriberId), 1);
  for (unsigned id = 2; id <= 25; ++id) add("value", id);
  EVENTUALLY(seen.size() == 25);
  EVENTUALLY(active(adapter));
  std::this_thread::sleep_for(350ms);
  EXPECT_TRUE(active(adapter));
  EXPECT_EQ(handle.status().epoch, 1u);
  const auto ids = seen.snapshot();
  for (size_t index = 0; index < ids.size(); ++index) EXPECT_EQ(ids[index], std::to_string(index + 1) + "-0");
}

TEST_F(KeyspaceReaders, MoreThan64KeysAndChurnKeepEveryBucketFlowing) {
  options.readers = 1; // exercise several 64-key slices in the same bucket
  RA adapter(base, options);
  Delivered seen;
  std::vector<RA::ReaderHandle> handles;
  adapter.setDeferReaders(true);
  for (unsigned index = 0; index < 160; ++index) handles.push_back(subscribe(adapter, "key-" + std::to_string(index), seen));
  adapter.setDeferReaders(false);
  EVENTUALLY(active(adapter));
  for (unsigned index = 0; index < 160; ++index) add("key-" + std::to_string(index), 1);
  EVENTUALLY(seen.size() == 160);
  for (unsigned index = 0; index < 6; ++index) {
    Delivered ignored;
    auto churn = subscribe(adapter, "churn", ignored);
    churn.reset();
  }
  EVENTUALLY(active(adapter));
  for (unsigned index = 0; index < 160; ++index) add("key-" + std::to_string(index), 2);
  EVENTUALLY(seen.size() == 320);
  control->command<void>("CONFIG", "SET", "notify-keyspace-events", "");
  for (unsigned index = 0; index < 160; ++index) add("key-" + std::to_string(index), 3);
  EVENTUALLY(seen.size() == 480);
  EVENTUALLY(adapter.readerBackendStatus().effective == RA_ReaderMode::Blocking);
}

TEST_F(KeyspaceReaders, ZeroCommandTimeoutStillHasBoundedCancellation) {
  options.cxn.timeout = 0;
  auto adapter = std::make_unique<RA>(base, options);
  auto handle = adapter->subscribeStream("quiet", [](const auto&, const auto&, const auto&) {}, "0-0");
  EVENTUALLY(active(*adapter));
  const auto started = std::chrono::steady_clock::now();
  adapter.reset();
  EXPECT_LT(std::chrono::steady_clock::now() - started, 2s);
  EXPECT_FALSE(handle);
}

TEST_F(KeyspaceReaders, ActiveSharedRewindPreservesIndependentCursorsAndDeferral) {
  Delivered first, second;
  RA adapter(base, options);
  add("shared", 10);
  auto a = subscribe(adapter, "shared", first, "$");
  EVENTUALLY(active(adapter));
  add("shared", 11);
  EVENTUALLY(first.size() == 1);
  auto b = subscribe(adapter, "shared", second);
  EVENTUALLY(second.size() == 2);
  EXPECT_TRUE(active(adapter));
  const auto channels = control->command<std::unordered_map<std::string, long long>>(
      "PUBSUB", "NUMSUB", "__keyspace@0__:" + key("shared"));
  EXPECT_EQ(channels.at("__keyspace@0__:" + key("shared")), 1);
  add("shared", 12);
  EVENTUALLY(first.size() == 2 && second.size() == 3);
  a.reset();
  adapter.setDeferReaders(true);
  add("shared", 13);
  adapter.setDeferReaders(false);
  EVENTUALLY(second.size() == 4);
  EXPECT_EQ(first.size(), 2u);
  EXPECT_EQ(second.snapshot(), (std::vector<std::string>{"10-0", "11-0", "12-0", "13-0"}));
  EXPECT_EQ(b.status().epoch, 1u);
}

TEST_F(KeyspaceReaders, ActiveReadersKeepWrongTypeIsolationAndLowerIdRecovery) {
  Delivered healthy, repaired;
  options.readerProbeMs = 20;
  RA adapter(base, options);
  control->set(key("broken"), "wrong type");
  auto a = subscribe(adapter, "healthy", healthy);
  auto b = subscribe(adapter, "broken", repaired);
  EVENTUALLY(active(adapter));
  EVENTUALLY(b.status().streamKind == RA::StreamKind::Invalid || b.status().readRejections > 0);
  add("healthy", 100);
  EVENTUALLY(healthy.size() == 1);
  control->del(key("broken"));
  add("broken", 1); add("broken", 2);
  EVENTUALLY(repaired.size() == 2);
  auto transaction = control->transaction();
  transaction.del(key("healthy")).xadd(key("healthy"), "1-0", fields.begin(), fields.end()).exec();
  EVENTUALLY(a.status().epoch == 2 && healthy.size() == 2);
  EXPECT_EQ(healthy.snapshot(), (std::vector<std::string>{"100-0", "1-0"}));
  EXPECT_TRUE(active(adapter));
}

TEST_F(KeyspaceReaders, SubscriptionAckCannotProveDeniedAndDisabledConfiguration) {
  control->command<void>("CONFIG", "SET", "notify-keyspace-events", "");
  control->command<void>("ACL", "SETUSER", user, "-config");
  Delivered seen;
  RA adapter(base, options);
  const std::string literal = "sensor[*]?\\path";
  auto handle = subscribe(adapter, literal, seen);
  EVENTUALLY(adapter.readerBackendStatus().fallbackReason == RA::ReaderFallbackReason::UnverifiedNotifications);
  add(literal, 1);
  EVENTUALLY(seen.size() == 1);
  EXPECT_FALSE(active(adapter));
  control->command<void>("CONFIG", "SET", "notify-keyspace-events", "Kt");
  add(literal, 2);
  EVENTUALLY(active(adapter));
  EVENTUALLY(seen.size() == 2);
  add("sensorX\\path", 1);
  add(literal, 3);
  EVENTUALLY(seen.size() == 3);
  EXPECT_EQ(handle.status().cursor, "3-0");
}

TEST_F(KeyspaceReaders, StalledSubscriptionAcknowledgementReplacesOnlyTheSubscriber) {
  StalledSubscriberProxy proxy(options.cxn.port, true);
  options.cxn.port = proxy.port();
  Delivered seen;
  RA adapter(base, options);
  auto handle = subscribe(adapter, "value", seen);
  add("value", 1);
  EVENTUALLY(seen.size() == 1); // blocking fallback remains usable
  EVENTUALLY(proxy.subscribers() >= 2 && active(adapter));
  add("value", 2);
  EVENTUALLY(seen.size() == 2);
  EXPECT_EQ(handle.status().epoch, 1u);
}

TEST_F(KeyspaceReaders, SilentActiveSubscriberIsReplacedAfterCoverageFallback) {
  StalledSubscriberProxy proxy(options.cxn.port, false);
  options.cxn.port = proxy.port();
  Delivered seen;
  RA adapter(base, options);
  auto handle = subscribe(adapter, "value", seen);
  EVENTUALLY(active(adapter));
  add("value", 1);
  EVENTUALLY(seen.size() == 1);
  std::this_thread::sleep_for(200ms);
  proxy.stall();
  add("value", 2);
  EVENTUALLY(seen.size() == 2);
  EVENTUALLY(adapter.readerBackendStatus().fallbackReason == RA::ReaderFallbackReason::MissedNotifications);
  EVENTUALLY(proxy.subscribers() >= 2);
  // Wait for the replacement's ACK before providing new notification evidence.
  std::this_thread::sleep_for(100ms);
  add("value", 3);
  EVENTUALLY(active(adapter));
  EVENTUALLY(seen.size() == 3);
  EXPECT_EQ(handle.status().epoch, 1u);
}
