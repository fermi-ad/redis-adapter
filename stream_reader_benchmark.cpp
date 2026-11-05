// Production-shaped stream-reader comparison against an explicitly isolated fixture.
#include "RedisAdapter.hpp"
#include <hiredis.h>
#include <algorithm>
#include <atomic>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
using Nanoseconds = std::chrono::nanoseconds;
using Reply = std::unique_ptr<redisReply, decltype(&freeReplyObject)>;

struct Configuration {
  std::string socket, mode = "blocking", pattern = "sparse";
  size_t adapters = 7, keys = 769, readers = 1, workers = 1, payloadBytes = 64;
  uint32_t reconcileMs = 1000, batchCount = 64, burstMs = 100;
  uint64_t seed = 139;
  double rate = 5400, warmup = 10, duration = 30, drain = 10;
};

uint64_t number(const std::string& value) {
  uint64_t result = 0;
  const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
  if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size())
    throw std::invalid_argument("invalid unsigned integer: " + value);
  return result;
}

uint32_t number32(const std::string& value) {
  const auto result = number(value);
  if (result > UINT32_MAX) throw std::invalid_argument("integer exceeds uint32_t: " + value);
  return static_cast<uint32_t>(result);
}

double seconds(const std::string& value) {
  size_t used = 0;
  const auto result = std::stod(value, &used);
  if (used != value.size() || !std::isfinite(result)) throw std::invalid_argument("invalid number: " + value);
  return result;
}

Configuration arguments(int argc, char** argv) {
  Configuration c;
  for (int index = 1; index < argc; ++index) {
    const std::string name = argv[index];
    if (name == "--help") {
      std::cout << "Use scripts/run-stream-reader-benchmark.py to create a private fixture.\n"
                   "Options: --socket PATH --mode blocking|keyspace --pattern sparse|burst\n"
                   " --adapters 7 --keys 769 --readers 1 --workers 1 --rate 5400\n"
                   " --warmup 10 --duration 30 --drain 10 --payload-bytes 64\n"
                   " --reconcile-ms 1000 --batch-count 64 --burst-ms 100 --seed 139\n";
      std::exit(0);
    }
    if (++index == argc) throw std::invalid_argument("missing value for " + name);
    const std::string value = argv[index];
    if (name == "--socket") c.socket = value;
    else if (name == "--mode") c.mode = value;
    else if (name == "--pattern") c.pattern = value;
    else if (name == "--adapters") c.adapters = number(value);
    else if (name == "--keys") c.keys = number(value);
    else if (name == "--readers") c.readers = number(value);
    else if (name == "--workers") c.workers = number(value);
    else if (name == "--payload-bytes") c.payloadBytes = number(value);
    else if (name == "--reconcile-ms") c.reconcileMs = number32(value);
    else if (name == "--batch-count") c.batchCount = number32(value);
    else if (name == "--burst-ms") c.burstMs = number32(value);
    else if (name == "--seed") c.seed = number(value);
    else if (name == "--rate") c.rate = seconds(value);
    else if (name == "--warmup") c.warmup = seconds(value);
    else if (name == "--duration") c.duration = seconds(value);
    else if (name == "--drain") c.drain = seconds(value);
    else throw std::invalid_argument("unknown argument: " + name);
  }
  if (c.socket.empty()) throw std::invalid_argument("--socket and an isolated fixture token are required");
  if (c.mode != "blocking" && c.mode != "keyspace") throw std::invalid_argument("invalid reader mode");
  if (c.pattern != "sparse" && c.pattern != "burst") throw std::invalid_argument("invalid pattern");
  if (!c.adapters || c.adapters > 64 || !c.keys || c.keys > 100000 || !c.readers || c.readers > 256 ||
      !c.workers || c.workers > 256 || c.payloadBytes > 1024 * 1024 ||
      c.reconcileMs < 100 || c.reconcileMs > 60000 || !c.batchCount || c.batchCount > 65536 ||
      !c.burstMs || c.burstMs > 60000 || c.rate <= 0 || c.rate > 1000000 ||
      c.warmup < 0 || c.warmup > 3600 || c.duration <= 0 || c.duration > 3600 ||
      c.drain < 0 || c.drain > 300 || c.rate * (c.warmup + c.duration) > 100000000)
    throw std::invalid_argument("workload parameter outside its bounded range");
  return c;
}

std::string quote(const std::string& value) {
  std::ostringstream out;
  out << '"';
  for (const unsigned char ch : value) {
    if (ch == '"' || ch == '\\') out << '\\' << ch;
    else if (ch < 32) out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << unsigned(ch) << std::dec;
    else out << ch;
  }
  return out.str() + '"';
}

class Connection {
  std::unique_ptr<redisContext, decltype(&redisFree)> context_{nullptr, redisFree};
public:
  explicit Connection(const std::string& path) {
    const timeval timeout{2, 0};
    context_.reset(redisConnectUnixWithTimeout(path.c_str(), timeout));
    if (!context_ || context_->err) throw std::runtime_error("cannot connect to private fixture");
    if (redisSetTimeout(context_.get(), timeout) != REDIS_OK) throw std::runtime_error("cannot set Redis timeout");
  }
  Reply command(const std::vector<std::string>& args) {
    std::vector<const char*> values;
    std::vector<size_t> sizes;
    for (const auto& arg : args) { values.push_back(arg.data()); sizes.push_back(arg.size()); }
    Reply reply(static_cast<redisReply*>(redisCommandArgv(context_.get(), int(args.size()), values.data(), sizes.data())), freeReplyObject);
    if (!reply) throw std::runtime_error("Redis command transport failure");
    if (reply->type == REDIS_REPLY_ERROR) throw std::runtime_error(std::string(reply->str, reply->len));
    return reply;
  }
  void append(const std::vector<std::string>& args) {
    std::vector<const char*> values;
    std::vector<size_t> sizes;
    for (const auto& arg : args) { values.push_back(arg.data()); sizes.push_back(arg.size()); }
    if (redisAppendCommandArgv(context_.get(), int(args.size()), values.data(), sizes.data()) != REDIS_OK)
      throw std::runtime_error("Redis pipeline append failure");
  }
  Reply receive() {
    void* raw = nullptr;
    if (redisGetReply(context_.get(), &raw) != REDIS_OK || !raw) throw std::runtime_error("Redis pipeline transport failure");
    return Reply(static_cast<redisReply*>(raw), freeReplyObject);
  }
};

std::string replyString(const Reply& reply) {
  return reply->str ? std::string(reply->str, reply->len) : std::string{};
}

struct Snapshot {
  Clock::time_point when;
  std::map<std::string, std::string> info;
  double value(const std::string& key) const {
    const auto found = info.find(key);
    return found == info.end() ? 0 : std::stod(found->second);
  }
};

Snapshot snapshot(Connection& connection) {
  const auto reply = connection.command({"INFO", "ALL"});
  Snapshot result{Clock::now(), {}};
  std::istringstream lines(replyString(reply));
  for (std::string line; std::getline(lines, line);) {
    if (!line.empty() && line.back() == '\r') line.pop_back();
    const auto colon = line.find(':');
    if (!line.empty() && line.front() != '#' && colon != std::string::npos)
      result.info.emplace(line.substr(0, colon), line.substr(colon + 1));
  }
  return result;
}

std::map<std::string, double> commandStats(const std::string& text) {
  std::map<std::string, double> result;
  std::istringstream fields(text);
  for (std::string field; std::getline(fields, field, ',');) {
    const auto equal = field.find('=');
    if (equal != std::string::npos) result[field.substr(0, equal)] = std::stod(field.substr(equal + 1));
  }
  return result;
}

struct Expected { uint64_t sequence, sent; bool measured, accepted = false; };
struct Observed { uint64_t sequence, sent, received; bool measured; };
struct Key {
  std::string subkey, name;
  std::vector<Expected> expected; // Only the producer touches this until its join.
  std::mutex mutex;
  std::vector<Observed> observed;
  uint64_t malformed = 0, outOfOrder = 0, highest = 0;
};
struct Adapter {
  std::vector<std::unique_ptr<Key>> keys;
  std::unique_ptr<RedisAdapter> reader;
  std::vector<RedisAdapter::ReaderHandle> handles;
  uint64_t scheduled = 0, scheduledMeasured = 0, unsent = 0, errors = 0;
  double worstScheduleLagMs = 0;
  std::string error;
};

uint64_t nanos(Clock::time_point time) { return std::chrono::duration_cast<Nanoseconds>(time.time_since_epoch()).count(); }
Clock::time_point addSeconds(Clock::time_point time, double delta) { return time + Nanoseconds(static_cast<int64_t>(delta * 1e9)); }

void observe(Key& key, const RedisAdapter::StreamBatch& batch, const std::string& payload) {
  const auto received = nanos(Clock::now());
  std::lock_guard<std::mutex> lock(key.mutex);
  for (const auto& entry : batch) {
    try {
      const auto sequence = number(entry.second.at("sequence"));
      const auto sent = number(entry.second.at("sent_ns"));
      const auto phase = entry.second.at("measured");
      if (!sequence || entry.first != std::to_string(sequence) + "-0" || sent > received ||
          (phase != "0" && phase != "1") || entry.second.at("_") != payload)
        throw std::invalid_argument("invalid benchmark payload");
      if (sequence < key.highest) ++key.outOfOrder;
      key.highest = std::max(key.highest, sequence);
      key.observed.push_back({sequence, sent, received, phase == "1"});
    } catch (const std::exception&) { ++key.malformed; }
  }
}

struct Slot { Clock::time_point due; size_t key; };

struct ThreadJoiner {
  std::vector<std::thread>& threads;
  ~ThreadJoiner() { for (auto& thread : threads) if (thread.joinable()) thread.join(); }
};

void produce(Adapter& adapter, size_t ordinal, const Configuration& config,
             Clock::time_point start, Clock::time_point measuredStart, Clock::time_point end,
             const std::string& payload) {
  try {
    Connection connection(config.socket);
    std::mt19937_64 random(config.seed + ordinal * 7919);
    std::uniform_real_distribution<double> jitter(0.05, 0.95);
    std::uniform_int_distribution<size_t> keyIndex(0, config.keys - 1);
    const double period = double(config.adapters) / config.rate;
    std::vector<Slot> slots;
    for (uint64_t index = 0;; ++index) {
      const double continuous = (double(index) + jitter(random)) * period;
      if (continuous >= config.warmup + config.duration) break;
      const double offset = config.pattern == "sparse" ? continuous :
          std::floor(continuous * 1000 / config.burstMs) * config.burstMs / 1000.0;
      const auto due = addSeconds(start, offset);
      slots.push_back({due, keyIndex(random)});
      ++adapter.scheduled;
      if (due >= measuredStart) ++adapter.scheduledMeasured;
    }
    size_t index = 0;
    for (; index < slots.size();) {
      std::this_thread::sleep_until(slots[index].due);
      if (Clock::now() >= end) break;
      size_t stop = index + 1;
      if (config.pattern == "burst")
        while (stop < slots.size() && stop - index < 256 && slots[stop].due == slots[index].due) ++stop;
      std::vector<std::pair<Key*, size_t>> pending;
      for (size_t next = index; next < stop; ++next) {
        auto& key = *adapter.keys[slots[next].key];
        const auto now = Clock::now();
        const auto sequence = key.expected.size() + 1;
        const auto sent = nanos(now);
        const bool measured = now >= measuredStart && now < end;
        adapter.worstScheduleLagMs = std::max(adapter.worstScheduleLagMs,
            std::chrono::duration<double, std::milli>(now - slots[next].due).count());
        key.expected.push_back({sequence, sent, measured, false});
        pending.emplace_back(&key, key.expected.size() - 1);
        connection.append({"XADD", key.name, std::to_string(sequence) + "-0", "sequence", std::to_string(sequence),
                           "sent_ns", std::to_string(sent), "measured", measured ? "1" : "0", "_", payload});
      }
      for (const auto& item : pending) {
        const auto reply = connection.receive();
        auto& expected = item.first->expected[item.second];
        if (reply->type == REDIS_REPLY_STRING && replyString(reply) == std::to_string(expected.sequence) + "-0")
          expected.accepted = true;
        else { ++adapter.errors; if (adapter.error.empty()) adapter.error = replyString(reply); }
      }
      index = stop;
    }
    adapter.unsent = slots.size() - index;
  } catch (const std::exception& error) {
    ++adapter.errors;
    adapter.error = error.what();
    uint64_t attempted = 0;
    for (const auto& key : adapter.keys) attempted += key->expected.size();
    adapter.unsent = adapter.scheduled > attempted ? adapter.scheduled - attempted : 0;
  }
}

bool tailsReceived(const std::vector<std::unique_ptr<Adapter>>& adapters) {
  for (const auto& adapter : adapters) for (const auto& key : adapter->keys) {
    uint64_t last = 0, accepted = 0;
    for (const auto& item : key->expected) if (item.accepted) { last = item.sequence; ++accepted; }
    std::lock_guard<std::mutex> lock(key->mutex);
    if (key->highest < last || key->observed.size() < accepted) return false;
  }
  return true;
}

double percentile(std::vector<double> values, double fraction) {
  if (values.empty()) return 0;
  const auto index = std::min(values.size() - 1, size_t(std::ceil(fraction * values.size()) - 1));
  std::nth_element(values.begin(), values.begin() + index, values.end());
  return values[index];
}

uint64_t peakRss() {
  rusage usage{};
  if (getrusage(RUSAGE_SELF, &usage) != 0) return 0;
#ifdef __APPLE__
  return usage.ru_maxrss;
#else
  return uint64_t(usage.ru_maxrss) * 1024;
#endif
}

std::string modeName(RA_ReaderMode mode) { return mode == RA_ReaderMode::Keyspace ? "keyspace" : "blocking"; }

int run(const Configuration& config) {
  const char* token = std::getenv("REDIS_ADAPTER_STREAM_BENCHMARK_FIXTURE");
  if (!token || !*token) throw std::runtime_error("a private fixture token is required before connecting");
  Connection monitor(config.socket);
  if (replyString(monitor.command({"GET", "stream-reader-benchmark:fixture"})) != token)
    throw std::runtime_error("refusing a Redis endpoint without the private fixture token");
  const std::string payload(config.payloadBytes, 'x');
  const auto requested = config.mode == "keyspace" ? RA_ReaderMode::Keyspace : RA_ReaderMode::Blocking;
  std::vector<std::unique_ptr<Adapter>> adapters;
  std::cerr << "Preparing " << config.adapters << " adapters x " << config.keys << " streams, "
            << config.mode << ", readers=" << config.readers << ", " << config.pattern << "\n";
  for (size_t index = 0; index < config.adapters; ++index) {
    auto adapter = std::make_unique<Adapter>();
    RA_Options options;
    options.cxn.path = config.socket;
    options.readers = config.readers;
    options.workers = config.workers;
    options.readerMode = requested;
    options.readerReconcileMs = config.reconcileMs;
    options.readerBatchCount = config.batchCount;
    const auto base = "stream-benchmark-" + std::to_string(index);
    adapter->reader = std::make_unique<RedisAdapter>(base, options);
    adapter->reader->setDeferReaders(true);
    for (size_t item = 0; item < config.keys; ++item) {
      auto key = std::make_unique<Key>();
      key->subkey = "stream-" + std::to_string(item);
      key->name = "{" + base + "}:" + key->subkey;
      const auto pointer = key.get();
      adapter->handles.push_back(adapter->reader->subscribeStream(key->subkey,
          [pointer, &payload](const auto&, const auto&, const auto& batch) { observe(*pointer, batch, payload); }, "0-0"));
      adapter->keys.push_back(std::move(key));
    }
    adapter->reader->setDeferReaders(false);
    adapters.push_back(std::move(adapter));
  }
  const auto readinessDeadline = Clock::now() + std::chrono::seconds(10);
  if (requested == RA_ReaderMode::Keyspace) {
    while (Clock::now() < readinessDeadline) {
      bool ready = true;
      for (const auto& adapter : adapters) ready &= adapter->reader->readerBackendStatus().effective == requested;
      if (ready) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  const auto start = Clock::now() + std::chrono::milliseconds(200);
  const auto measuredStart = addSeconds(start, config.warmup);
  const auto end = addSeconds(measuredStart, config.duration);
  std::vector<std::thread> producers;
  ThreadJoiner joiner{producers};
  for (size_t index = 0; index < adapters.size(); ++index)
    producers.emplace_back([&, index] { produce(*adapters[index], index, config, start, measuredStart, end, payload); });
  std::this_thread::sleep_until(measuredStart);
  const auto before = snapshot(monitor);
  auto after = before;
  double peakClients = before.value("connected_clients"), peakServerRss = before.value("used_memory_rss");
  double peakServerMemory = before.value("used_memory");
  bool backendMatchedThroughout = true;
  const auto inspectBackends = [&] {
    for (const auto& adapter : adapters)
      backendMatchedThroughout &= adapter->reader->readerBackendStatus().effective == requested;
  };
  inspectBackends();
  for (auto next = measuredStart + std::chrono::seconds(1);;) {
    std::this_thread::sleep_until(std::min(next, end));
    after = snapshot(monitor);
    peakClients = std::max(peakClients, after.value("connected_clients"));
    peakServerRss = std::max(peakServerRss, after.value("used_memory_rss"));
    peakServerMemory = std::max(peakServerMemory, after.value("used_memory"));
    inspectBackends();
    if (Clock::now() >= end) break;
    next += std::chrono::seconds(1);
  }
  for (auto& producer : producers) producer.join();
  const auto drainStart = Clock::now();
  const auto drainDeadline = addSeconds(drainStart, config.drain);
  while (!tailsReceived(adapters) && Clock::now() < drainDeadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  // Let already queued duplicate callbacks appear before cancellation fences them.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  const auto drainSeconds = std::chrono::duration<double>(Clock::now() - drainStart).count();
  std::vector<RedisAdapter::ReaderBackendStatus> backendStatuses;
  for (auto& adapter : adapters) {
    backendStatuses.push_back(adapter->reader->readerBackendStatus());
    // Destroy each adapter once before handles, avoiding thousands of topology restarts.
    adapter->reader.reset();
    adapter->handles.clear();
  }

  uint64_t scheduled = 0, scheduledMeasured = 0, unsent = 0, attempted = 0, accepted = 0, acceptedMeasured = 0;
  uint64_t observed = 0, omitted = 0, duplicates = 0, unexpected = 0, malformed = 0, outOfOrder = 0, writeErrors = 0;
  double scheduleLag = 0;
  std::vector<double> latencies;
  std::vector<std::string> producerErrors;
  for (const auto& adapter : adapters) {
    scheduled += adapter->scheduled;
    scheduledMeasured += adapter->scheduledMeasured;
    unsent += adapter->unsent;
    writeErrors += adapter->errors;
    scheduleLag = std::max(scheduleLag, adapter->worstScheduleLagMs);
    if (!adapter->error.empty()) producerErrors.push_back(adapter->error);
    for (const auto& key : adapter->keys) {
      attempted += key->expected.size();
      std::vector<uint32_t> seen(key->expected.size(), 0);
      malformed += key->malformed;
      outOfOrder += key->outOfOrder;
      observed += key->observed.size();
      for (const auto& item : key->observed) {
        if (!item.sequence || item.sequence > key->expected.size()) { ++unexpected; continue; }
        const auto& expected = key->expected[item.sequence - 1];
        if (!expected.accepted) ++unexpected;
        if (seen[item.sequence - 1]++) ++duplicates;
        if (item.sent != expected.sent || item.measured != expected.measured) ++malformed;
        if (item.measured) latencies.push_back(double(item.received - item.sent) / 1e6);
      }
      for (size_t index = 0; index < key->expected.size(); ++index) if (key->expected[index].accepted) {
        ++accepted;
        if (key->expected[index].measured) ++acceptedMeasured;
        if (!seen[index]) ++omitted;
      }
    }
  }
  const auto wall = std::chrono::duration<double>(after.when - before.when).count();
  const auto cpu = after.value("used_cpu_sys") + after.value("used_cpu_user") -
                   before.value("used_cpu_sys") - before.value("used_cpu_user");
  const auto inputBytes = after.value("total_net_input_bytes") - before.value("total_net_input_bytes");
  const bool correct = !omitted && !duplicates && !unexpected && !malformed && !outOfOrder && !writeErrors && accepted == attempted;
  const auto maxLatency = latencies.empty() ? 0 : *std::max_element(latencies.begin(), latencies.end());
  std::string revision = RA_VERSION;
  if (revision.size() > 1 && revision.front() == '"' && revision.back() == '"')
    revision = revision.substr(1, revision.size() - 2);
  std::ostringstream out;
  out << std::setprecision(10) << "{\n  \"schema_version\":1,\n  \"configuration\":{"
      << "\"mode\":" << quote(config.mode) << ",\"pattern\":" << quote(config.pattern)
      << ",\"adapters\":" << config.adapters << ",\"keys_per_adapter\":" << config.keys
      << ",\"readers\":" << config.readers << ",\"workers\":" << config.workers
      << ",\"rate_per_second\":" << config.rate << ",\"warmup_seconds\":" << config.warmup
      << ",\"measurement_seconds\":" << config.duration << ",\"payload_bytes\":" << config.payloadBytes
      << ",\"reconcile_ms\":" << config.reconcileMs << ",\"batch_count\":" << config.batchCount
      << ",\"burst_ms\":" << config.burstMs << ",\"seed\":" << config.seed << "},\n"
      << "  \"redis_version\":" << quote(before.info.at("redis_version")) << ",\n"
      << "  \"adapter_revision\":" << quote(revision) << ",\n"
      << "  \"correctness\":{\"pass\":" << (correct ? "true" : "false")
      << ",\"attempted\":" << attempted << ",\"accepted\":" << accepted << ",\"observed\":" << observed
      << ",\"omissions\":" << omitted << ",\"duplicates\":" << duplicates << ",\"out_of_order\":" << outOfOrder
      << ",\"unexpected\":" << unexpected << ",\"malformed\":" << malformed << ",\"write_errors\":" << writeErrors << "},\n"
      << "  \"producer\":{\"scheduled\":" << scheduled << ",\"scheduled_in_measurement\":" << scheduledMeasured
      << ",\"unsent\":" << unsent << ",\"accepted_in_measurement\":" << acceptedMeasured
      << ",\"accepted_per_second\":" << acceptedMeasured / config.duration
      << ",\"max_schedule_lag_ms\":" << scheduleLag << ",\"errors\":[";
  for (size_t index = 0; index < producerErrors.size(); ++index) out << (index ? "," : "") << quote(producerErrors[index]);
  out << "]},\n  \"latency_ms\":{\"samples\":" << latencies.size()
      << ",\"p50\":" << percentile(latencies, .50) << ",\"p95\":" << percentile(latencies, .95)
      << ",\"p99\":" << percentile(latencies, .99) << ",\"max\":" << maxLatency << "},\n"
      << "  \"redis\":{\"sample_seconds\":" << wall << ",\"cpu_seconds\":" << cpu
      << ",\"cpu_percent_one_core\":" << cpu / wall * 100 << ",\"input_bytes\":" << inputBytes
      << ",\"input_bytes_per_second\":" << inputBytes / wall
      << ",\"connections_before\":" << before.value("connected_clients")
      << ",\"connections_after\":" << after.value("connected_clients") << ",\"connections_sampled_peak\":" << peakClients
      << ",\"used_memory_bytes_before\":" << before.value("used_memory")
      << ",\"used_memory_bytes_after\":" << after.value("used_memory")
      << ",\"used_memory_bytes_sampled_peak\":" << peakServerMemory
      << ",\"rss_bytes_sampled_peak\":" << peakServerRss << ",\"command_stats_delta\":{";
  bool firstCommand = true;
  for (const auto& item : after.info) if (item.first.rfind("cmdstat_", 0) == 0) {
    const auto final = commandStats(item.second);
    const auto old = before.info.find(item.first);
    const auto initial = old == before.info.end() ? std::map<std::string, double>{} : commandStats(old->second);
    out << (firstCommand ? "" : ",") << quote(item.first.substr(8)) << ":{";
    firstCommand = false;
    bool firstField = true;
    for (const auto& field : final) if (field.first != "usec_per_call") {
      const auto previous = initial.find(field.first);
      out << (firstField ? "" : ",") << quote(field.first) << ':'
          << field.second - (previous == initial.end() ? 0 : previous->second);
      firstField = false;
    }
    out << '}';
  }
  out << "}},\n  \"benchmark_process_peak_rss_bytes\":" << peakRss()
      << ",\n  \"drain_seconds\":" << drainSeconds
      << ",\n  \"backend_matched_at_all_measurement_samples\":" << (backendMatchedThroughout ? "true" : "false")
      << ",\n  \"backend_status_after_drain\":[";
  for (size_t index = 0; index < backendStatuses.size(); ++index) {
    const auto& status = backendStatuses[index];
    out << (index ? "," : "") << "{\"requested\":" << quote(modeName(status.requested))
        << ",\"effective\":" << quote(modeName(status.effective))
        << ",\"fallback_reason_code\":" << static_cast<int>(status.fallbackReason) << '}';
  }
  out << "]\n}\n";
  std::cout << out.str();
  return !correct ? 2 : !backendMatchedThroughout ? 3 : 0;
}
} // namespace

int main(int argc, char** argv) {
  try { return run(arguments(argc, argv)); }
  catch (const std::exception& error) { std::cerr << "Stream reader benchmark: " << error.what() << '\n'; return 1; }
}
