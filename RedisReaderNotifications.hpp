#pragma once
#include "RedisConnection.hpp"
#include "RedisReaderMode.hpp"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

// One mailbox per reader bucket. Notifications never touch its stream cursors.
// Queue storage is bounded by registered keys, even while a reader is deferred.
class RedisReaderWake {
public:
  using Clock = std::chrono::steady_clock;
  struct Ticket {
    std::string key;
    uint64_t generation, notifications;
    bool checkCoverage;
  };
  void reset(const std::vector<std::string>& keys);
  void mark(const std::string& key, bool notification = false, bool reconcile = false);
  void catchUp();
  std::vector<Ticket> take(size_t maximum);
  void finish(const Ticket& ticket, size_t entries, size_t count, bool retry = false);
  bool missedNotification();
  void waitUntil(Clock::time_point deadline, const std::atomic<bool>& running);
  void pause(std::chrono::milliseconds delay, const std::atomic<bool>& running);
  void wake() { changed_.notify_all(); }
private:
  struct Key {
    uint64_t generation = 0, notifications = 0, readNotifications = 0;
    bool queued = false, inFlight = false, catchup = false, reconcile = false;
    Clock::time_point coverageDeadline{};
  };
  void enqueue(const std::string& key, Key& state);
  std::mutex mutex_;
  std::condition_variable changed_;
  std::unordered_map<std::string, Key> keys_;
  std::deque<std::string> queue_;
  std::unordered_map<std::string, Clock::time_point> coverage_;
};

// Owns the Subscriber on exactly one thread. Registry locks are independent of
// the adapter's reader lifecycle lock; callbacks hold only shared mailboxes.
class RedisReaderNotifications {
public:
  explicit RedisReaderNotifications(const RedisConnection::Options& options);
  ~RedisReaderNotifications();
  void stop();
  void update(uint32_t bucket, const std::shared_ptr<RedisReaderWake>& wake,
              const std::vector<std::string>& keys);
  void remove(uint32_t bucket);
  RA_ReaderBackendStatus status() const;
  void missedNotification();
private:
  using Clock = std::chrono::steady_clock;
  struct Bucket { std::weak_ptr<RedisReaderWake> wake; std::vector<std::string> keys; };
  struct Channel { bool active = false, pending = false; uint64_t operation = 0; };
  void run();
  void select(RA_ReaderMode mode, RA_ReaderFallbackReason reason);
  void selectLocked(RA_ReaderMode mode, RA_ReaderFallbackReason reason);
  bool pause(std::chrono::milliseconds delay);
  const RedisConnection::Options options_;
  mutable std::mutex mutex_;
  std::condition_variable changed_;
  bool running_ = true;
  uint64_t revision_ = 0;
  std::unordered_map<uint32_t, Bucket> buckets_;
  std::unordered_map<std::string, std::weak_ptr<RedisReaderWake>> routes_;
  RA_ReaderBackendStatus status_{RA_ReaderMode::Keyspace, RA_ReaderMode::Blocking,
                               RA_ReaderFallbackReason::Starting};
  Clock::time_point missedAt_{};
  std::thread thread_;
};
