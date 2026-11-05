#include "RedisReaderNotifications.hpp"
#include <algorithm>
#include <unordered_set>

using namespace std;
using namespace std::chrono;

void RedisReaderWake::enqueue(const string& key, Key& state) {
  if (!state.queued && !state.inFlight) { queue_.push_back(key); state.queued = true; }
}

void RedisReaderWake::reset(const vector<string>& keys) {
  lock_guard<mutex> lock(mutex_);
  unordered_map<string, Key> retained;
  queue_.clear();
  coverage_.clear();
  for (const auto& key : keys) {
    const auto old = keys_.find(key);
    auto& state = retained[key];
    if (old != keys_.end()) state = old->second;
    ++state.generation;
    state.queued = state.inFlight = state.reconcile = false;
    state.catchup = true;
    state.coverageDeadline = {};
    enqueue(key, state);
  }
  keys_ = std::move(retained);
  changed_.notify_all();
}

void RedisReaderWake::mark(const string& key, bool notification, bool reconcile) {
  lock_guard<mutex> lock(mutex_);
  const auto found = keys_.find(key);
  if (found == keys_.end()) return;
  auto& state = found->second;
  ++state.generation;
  if (notification) { ++state.notifications; state.coverageDeadline = {}; coverage_.erase(key); }
  else if (reconcile) state.reconcile = true;
  else state.catchup = true;
  enqueue(key, state);
  changed_.notify_one();
}

void RedisReaderWake::catchUp() {
  lock_guard<mutex> lock(mutex_);
  coverage_.clear();
  for (auto& key : keys_) {
    ++key.second.generation; key.second.catchup = true;
    key.second.coverageDeadline = {};
    enqueue(key.first, key.second);
  }
  changed_.notify_all();
}

vector<RedisReaderWake::Ticket> RedisReaderWake::take(size_t maximum) {
  lock_guard<mutex> lock(mutex_);
  vector<Ticket> result;
  while (!queue_.empty() && result.size() < maximum) {
    auto name = std::move(queue_.front()); queue_.pop_front();
    auto& state = keys_.at(name);
    state.queued = false; state.inFlight = true;
    result.push_back({std::move(name), state.generation, state.notifications,
        state.reconcile && !state.catchup && state.notifications == state.readNotifications});
    state.catchup = state.reconcile = false;
  }
  return result;
}

void RedisReaderWake::finish(const Ticket& ticket, size_t entries, size_t count, bool retry) {
  lock_guard<mutex> lock(mutex_);
  auto& state = keys_.at(ticket.key); // topology changes only after its reader stops
  state.inFlight = false;
  if (!retry) {
    state.readNotifications = ticket.notifications;
    if (ticket.checkCoverage && entries && state.notifications == ticket.notifications &&
        state.coverageDeadline == Clock::time_point{}) {
      state.coverageDeadline = Clock::now() + milliseconds(250);
      coverage_[ticket.key] = state.coverageDeadline;
    }
  }
  const bool drain = entries >= count;
  if (retry || drain) state.catchup = true;
  if (retry || drain || state.generation != ticket.generation) enqueue(ticket.key, state);
}

bool RedisReaderWake::missedNotification() {
  lock_guard<mutex> lock(mutex_);
  bool missed = false;
  const auto now = Clock::now();
  for (auto it = coverage_.begin(); it != coverage_.end();) {
    if (it->second > now) { ++it; continue; }
    keys_.at(it->first).coverageDeadline = {}; missed = true;
    it = coverage_.erase(it);
  }
  return missed;
}

void RedisReaderWake::waitUntil(Clock::time_point deadline, const atomic<bool>& running) {
  unique_lock<mutex> lock(mutex_);
  for (const auto& key : coverage_) deadline = min(deadline, key.second);
  changed_.wait_until(lock, deadline, [&] { return !running || !queue_.empty(); });
}

void RedisReaderWake::pause(milliseconds delay, const atomic<bool>& running) {
  unique_lock<mutex> lock(mutex_);
  changed_.wait_for(lock, delay, [&] { return !running; });
}

RedisReaderNotifications::RedisReaderNotifications(const RedisConnection::Options& options)
    : options_(options), thread_([this] { run(); }) {}

RedisReaderNotifications::~RedisReaderNotifications() {
  stop();
}

void RedisReaderNotifications::stop() {
  { lock_guard<mutex> lock(mutex_); running_ = false; }
  changed_.notify_all();
  if (thread_.joinable()) thread_.join();
}

void RedisReaderNotifications::update(uint32_t bucket, const shared_ptr<RedisReaderWake>& wake,
                                      const vector<string>& keys) {
  lock_guard<mutex> lock(mutex_);
  auto& previous = buckets_[bucket];
  const unordered_set<string> wanted(keys.begin(), keys.end());
  for (const auto& key : previous.keys) if (!wanted.count(key)) routes_.erase(key);
  bool added = false;
  for (const auto& key : keys) {
    added |= routes_.find(key) == routes_.end();
    routes_[key] = wake;
  }
  previous = {wake, keys};
  ++revision_;
  if (added) selectLocked(RA_ReaderMode::Blocking, RA_ReaderFallbackReason::Starting);
  changed_.notify_all();
}

void RedisReaderNotifications::remove(uint32_t bucket) {
  lock_guard<mutex> lock(mutex_);
  const auto found = buckets_.find(bucket);
  if (found == buckets_.end()) return;
  for (const auto& key : found->second.keys) routes_.erase(key);
  buckets_.erase(found);
  ++revision_;
  changed_.notify_all();
}

RA_ReaderBackendStatus RedisReaderNotifications::status() const {
  lock_guard<mutex> lock(mutex_);
  return status_;
}

void RedisReaderNotifications::missedNotification() {
  lock_guard<mutex> lock(mutex_);
  if (missedAt_ == Clock::time_point{}) missedAt_ = Clock::now();
  ++revision_;
  selectLocked(RA_ReaderMode::Blocking, RA_ReaderFallbackReason::MissedNotifications);
}

void RedisReaderNotifications::selectLocked(RA_ReaderMode mode, RA_ReaderFallbackReason reason) {
  if (status_.effective == mode && status_.fallbackReason == reason) return;
  status_.effective = mode; status_.fallbackReason = reason;
  const char* message = "ready";
  switch (reason) {
  case RA_ReaderFallbackReason::None: break;
  case RA_ReaderFallbackReason::Starting: message = "waiting for subscription acknowledgements"; break;
  case RA_ReaderFallbackReason::UnverifiedNotifications: message = "notification configuration unavailable; waiting for an xadd notification"; break;
  case RA_ReaderFallbackReason::NotificationsDisabled: message = "notify-keyspace-events requires K and t (or A)"; break;
  case RA_ReaderFallbackReason::SubscriptionRejected: message = "notification subscription rejected"; break;
  case RA_ReaderFallbackReason::SubscriberUnavailable: message = "notification connection unavailable"; break;
  case RA_ReaderFallbackReason::MissedNotifications: message = "reconciliation recovered data without a notification"; break;
  }
  const bool normal = reason == RA_ReaderFallbackReason::None || reason == RA_ReaderFallbackReason::Starting;
  syslog(normal ? LOG_INFO : LOG_WARNING, "stream reader backend %s: %s",
      mode == RA_ReaderMode::Keyspace ? "keyspace" : "blocking", message);
  // Mailboxes never call this helper while holding their own mutex. No adapter
  // lifecycle lock is taken here, including while a reader is being joined.
  for (const auto& bucket : buckets_) if (auto wake = bucket.second.wake.lock()) wake->catchUp();
}

void RedisReaderNotifications::select(RA_ReaderMode mode, RA_ReaderFallbackReason reason) {
  lock_guard<mutex> lock(mutex_);
  selectLocked(mode, reason);
}

bool RedisReaderNotifications::pause(milliseconds delay) {
  unique_lock<mutex> lock(mutex_);
  changed_.wait_for(lock, delay, [&] { return !running_; });
  return running_;
}

void RedisReaderNotifications::run() {
  constexpr const char* prefix = "__keyspace@0__:";
  const string channelPrefix(prefix);
  uint32_t retry = 50;
  for (;;) {
    { lock_guard<mutex> lock(mutex_); if (!running_) return; }
    try {
      auto subscriber = RedisConnection::notificationSubscriber(options_);
      unordered_map<string, Channel> channels;
      auto config = RedisConnection::NotificationConfig{RedisConnection::ReadStatus::Rejected};
      auto nextConfig = Clock::time_point{};
      auto lastEvent = Clock::time_point{};
      const auto connectedAt = Clock::now();
      struct Acknowledgement { Clock::time_point deadline; string key; uint64_t operation; };
      deque<Acknowledgement> acknowledgements;
      uint64_t nextOperation = 0;
      unordered_set<string> wanted;
      uint64_t revision = UINT64_MAX;
      bool updateNeeded = true;
      subscriber->on_meta([&](sw::redis::Subscriber::MsgType type, sw::redis::OptionalString channel, long long) {
        if (!channel || channel->rfind(channelPrefix, 0) != 0) return;
        const auto key = channel->substr(channelPrefix.size());
        const auto found = channels.find(key);
        if (found == channels.end()) return;
        if (type != sw::redis::Subscriber::MsgType::SUBSCRIBE &&
            type != sw::redis::Subscriber::MsgType::UNSUBSCRIBE) return;
        found->second.active = type == sw::redis::Subscriber::MsgType::SUBSCRIBE;
        found->second.pending = false;
        updateNeeded = true;
        if (found->second.active) {
          lock_guard<mutex> lock(mutex_);
          const auto route = routes_.find(key);
          if (route != routes_.end()) if (auto wake = route->second.lock()) wake->mark(key);
        }
      });
      subscriber->on_message([&](string channel, string event) {
        if (event != "xadd" || channel.rfind(channelPrefix, 0) != 0) return;
        const auto key = channel.substr(channelPrefix.size());
        shared_ptr<RedisReaderWake> wake;
        {
          lock_guard<mutex> lock(mutex_);
          const auto route = routes_.find(key);
          if (route != routes_.end()) wake = route->second.lock();
        }
        if (!wake) return;
        lastEvent = Clock::now();
        // Actual delivery is sufficient evidence even if CONFIG is denied or
        // the administrator has just enabled notifications since our last GET.
        updateNeeded |= config.status != RedisConnection::ReadStatus::Accepted || !config.enabled;
        config = {RedisConnection::ReadStatus::Accepted, true};
        wake->mark(key, true);
      });

      for (;;) {
        bool replaceAfterCoverageLoss = false;
        {
          lock_guard<mutex> lock(mutex_);
          if (!running_) return;
          if (revision != revision_) {
            wanted.clear();
            for (const auto& route : routes_) wanted.insert(route.first);
            revision = revision_; updateNeeded = true;
          }
          updateNeeded |= missedAt_ != Clock::time_point{} && lastEvent > missedAt_ &&
                          Clock::now() >= missedAt_ + seconds(1);
          // A silent half-open subscriber must not strand recovery forever.
          // Known-disabled configuration needs no socket churn; otherwise a
          // new bounded subscription attempt can restore notification delivery.
          replaceAfterCoverageLoss = missedAt_ != Clock::time_point{} && lastEvent <= missedAt_ &&
              Clock::now() >= max(missedAt_, connectedAt) + seconds(1) &&
              (config.status != RedisConnection::ReadStatus::Accepted || config.enabled);
        }
        if (replaceAfterCoverageLoss) throw sw::redis::Error("notification coverage lost");
        while (!acknowledgements.empty()) {
          const auto& pending = acknowledgements.front();
          const auto channel = channels.find(pending.key);
          if (channel == channels.end() || !channel->second.pending || channel->second.operation != pending.operation) {
            acknowledgements.pop_front(); continue;
          }
          if (pending.deadline <= Clock::now()) throw sw::redis::Error("notification acknowledgement timed out");
          break;
        }
        if (wanted.empty() && channels.empty()) {
          unique_lock<mutex> lock(mutex_);
          changed_.wait_for(lock, milliseconds(100), [&] { return !running_ || revision != revision_; });
          if (!running_) return;
          continue;
        }
        if (Clock::now() >= nextConfig) {
          config = RedisConnection::notificationConfig(options_);
          if (config.status != RedisConnection::ReadStatus::Accepted && lastEvent != Clock::time_point{})
            config = {RedisConnection::ReadStatus::Accepted, true};
          nextConfig = Clock::now() + seconds(30);
          updateNeeded = true;
        }
        if (updateNeeded) {
          vector<string> add, remove;
          for (const auto& key : wanted) {
            auto& channel = channels[key];
            if (!channel.active && !channel.pending && add.size() < 64) {
              add.push_back(channelPrefix + key); channel.pending = true;
              channel.operation = ++nextOperation;
              acknowledgements.push_back({Clock::now() + seconds(1), key, channel.operation});
            }
          }
          for (auto it = channels.begin(); it != channels.end();) {
            auto& channel = it->second;
            if (!wanted.count(it->first) && !channel.pending) {
              if (!channel.active) { it = channels.erase(it); continue; }
              if (remove.size() < 64) {
                remove.push_back(channelPrefix + it->first); channel.pending = true;
                channel.operation = ++nextOperation;
                acknowledgements.push_back({Clock::now() + seconds(1), it->first, channel.operation});
              }
            }
            ++it;
          }
          if (!add.empty()) subscriber->subscribe(add.begin(), add.end());
          if (!remove.empty()) subscriber->unsubscribe(remove.begin(), remove.end());
          {
            lock_guard<mutex> lock(mutex_);
            // Compare against the live registry, not the snapshot taken before
            // network I/O: a newly added key must wait for its own acknowledgement.
            bool ready = !routes_.empty();
            for (const auto& route : routes_) {
              const auto channel = channels.find(route.first);
              ready &= channel != channels.end() && channel->second.active && !channel->second.pending;
            }
            // Partial ACK progress must not reset a persistent later-batch ACL
            // refusal into a rapid reconnect loop.
            if (ready) retry = 50;
            if (missedAt_ != Clock::time_point{} && lastEvent > missedAt_ && Clock::now() >= missedAt_ + seconds(1))
              missedAt_ = {};
            if (missedAt_ != Clock::time_point{}) selectLocked(RA_ReaderMode::Blocking, RA_ReaderFallbackReason::MissedNotifications);
            else if (config.status != RedisConnection::ReadStatus::Accepted)
              selectLocked(RA_ReaderMode::Blocking, RA_ReaderFallbackReason::UnverifiedNotifications);
            else if (!config.enabled) selectLocked(RA_ReaderMode::Blocking, RA_ReaderFallbackReason::NotificationsDisabled);
            else if (!ready) selectLocked(RA_ReaderMode::Blocking, RA_ReaderFallbackReason::Starting);
            else selectLocked(RA_ReaderMode::Keyspace, RA_ReaderFallbackReason::None);
          }
          updateNeeded = false;
        }
        try { subscriber->consume(); }
        catch (const sw::redis::TimeoutError&) {} // redis++ resets the idle socket error
      }
    } catch (const sw::redis::ReplyError&) {
      select(RA_ReaderMode::Blocking, RA_ReaderFallbackReason::SubscriptionRejected);
      if (!pause(milliseconds(retry))) return;
      retry = min<uint32_t>(30000, retry * 2);
    } catch (const exception&) {
      {
        lock_guard<mutex> lock(mutex_);
        selectLocked(RA_ReaderMode::Blocking, missedAt_ == Clock::time_point{}
            ? RA_ReaderFallbackReason::SubscriberUnavailable : RA_ReaderFallbackReason::MissedNotifications);
      }
      if (!pause(milliseconds(min<uint32_t>(retry, 1000)))) return;
      retry = min<uint32_t>(1000, retry * 2);
    }
  }
}
