//
//  benchmark_lite.cpp
//
//  Google Benchmark suite for RedisAdapterLite.
//  Requires a running Redis on localhost:6379 (TCP).
//  If a Unix domain socket exists at /tmp/redis.sock, UDS benchmarks run too.
//
//  Run with: ./ral-benchmark
//  TCP only:    ./ral-benchmark --benchmark_filter="TCP"
//  UDS only:    ./ral-benchmark --benchmark_filter="UDS"
//  Stress only: ./ral-benchmark --benchmark_filter="Stress"
//

#include "RedisAdapterLite.hpp"
#include <benchmark/benchmark.h>
#include <sys/stat.h>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <vector>
#include <random>

// ============================================================================
//  Transport configuration
// ============================================================================

static const std::string UDS_PATH = "/tmp/redis.sock";

static bool uds_available()
{
  struct stat st;
  if (stat(UDS_PATH.c_str(), &st) != 0)
    return false;
  // Try a quick connection
  RAL_Options opts;
  opts.path = UDS_PATH;
  RedisAdapterLite test("BM_PROBE", opts);
  return test.connected();
}

static bool g_uds_available = false;

static RAL_Options tcp_opts()
{
  return {};  // defaults: 127.0.0.1:6379
}

static RAL_Options uds_opts()
{
  RAL_Options opts;
  opts.path = UDS_PATH;
  return opts;
}

// Per-transport singleton adapters
static RedisAdapterLite& get_tcp()
{
  static RedisAdapterLite redis("BENCH", tcp_opts());
  return redis;
}

static RedisAdapterLite& get_uds()
{
  static RedisAdapterLite redis("BENCH", uds_opts());
  return redis;
}

// ============================================================================
//  Serialization helpers (no Redis, transport-independent)
// ============================================================================

static void BM_FromDouble(benchmark::State& state)
{
  for (auto _ : state)
  {
    auto attrs = ral_from_double(3.14159);
    benchmark::DoNotOptimize(attrs);
  }
}
BENCHMARK(BM_FromDouble);

static void BM_ToDouble(benchmark::State& state)
{
  auto attrs = ral_from_double(3.14159);
  for (auto _ : state)
  {
    auto val = ral_to_double(attrs);
    benchmark::DoNotOptimize(val);
  }
}
BENCHMARK(BM_ToDouble);

static void BM_FromInt(benchmark::State& state)
{
  for (auto _ : state)
  {
    auto attrs = ral_from_int(42);
    benchmark::DoNotOptimize(attrs);
  }
}
BENCHMARK(BM_FromInt);

static void BM_ToInt(benchmark::State& state)
{
  auto attrs = ral_from_int(42);
  for (auto _ : state)
  {
    auto val = ral_to_int(attrs);
    benchmark::DoNotOptimize(val);
  }
}
BENCHMARK(BM_ToInt);

static void BM_FromBlob(benchmark::State& state)
{
  std::vector<uint8_t> blob(state.range(0), 0xAB);
  for (auto _ : state)
  {
    auto attrs = ral_from_blob(blob.data(), blob.size());
    benchmark::DoNotOptimize(attrs);
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_FromBlob)->Arg(64)->Arg(1024)->Arg(65536);

static void BM_TimeToStreamId(benchmark::State& state)
{
  RAL_Time t(1700000000123456789LL);
  for (auto _ : state)
  {
    auto id = t.id();
    benchmark::DoNotOptimize(id);
  }
}
BENCHMARK(BM_TimeToStreamId);

static void BM_TimeFromStreamId(benchmark::State& state)
{
  std::string id = "1700000000123-456789";
  for (auto _ : state)
  {
    RAL_Time t(id);
    benchmark::DoNotOptimize(t);
  }
}
BENCHMARK(BM_TimeFromStreamId);

// ============================================================================
//  Macro-based benchmark generation for TCP and UDS
// ============================================================================
//
//  Each DEFINE_ macro creates a benchmark function. We register it twice:
//  once for TCP, once for UDS (if available). The UDS variant skips itself
//  at runtime if the socket wasn't found at startup.

#define SKIP_IF_NO_UDS() \
  if (!g_uds_available) { state.SkipWithMessage("UDS not available"); return; }

// --- Add single values ---

static void BM_TCP_AddDouble(benchmark::State& state)
{
  get_tcp().del("bm_add_dbl");
  for (auto _ : state)
  {
    auto t = get_tcp().addDouble("bm_add_dbl", 3.14, {.trim = 0});
    benchmark::DoNotOptimize(t);
  }
  get_tcp().del("bm_add_dbl");
}
BENCHMARK(BM_TCP_AddDouble);

static void BM_UDS_AddDouble(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  get_uds().del("bm_add_dbl");
  for (auto _ : state)
  {
    auto t = get_uds().addDouble("bm_add_dbl", 3.14, {.trim = 0});
    benchmark::DoNotOptimize(t);
  }
  get_uds().del("bm_add_dbl");
}
BENCHMARK(BM_UDS_AddDouble);

static void BM_TCP_AddInt(benchmark::State& state)
{
  get_tcp().del("bm_add_int");
  for (auto _ : state)
  {
    auto t = get_tcp().addInt("bm_add_int", 42, {.trim = 0});
    benchmark::DoNotOptimize(t);
  }
  get_tcp().del("bm_add_int");
}
BENCHMARK(BM_TCP_AddInt);

static void BM_UDS_AddInt(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  get_uds().del("bm_add_int");
  for (auto _ : state)
  {
    auto t = get_uds().addInt("bm_add_int", 42, {.trim = 0});
    benchmark::DoNotOptimize(t);
  }
  get_uds().del("bm_add_int");
}
BENCHMARK(BM_UDS_AddInt);

static void BM_TCP_AddString(benchmark::State& state)
{
  get_tcp().del("bm_add_str");
  std::string val(state.range(0), 'x');
  for (auto _ : state)
  {
    auto t = get_tcp().addString("bm_add_str", val, {.trim = 0});
    benchmark::DoNotOptimize(t);
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
  get_tcp().del("bm_add_str");
}
BENCHMARK(BM_TCP_AddString)->Arg(16)->Arg(256)->Arg(4096);

static void BM_UDS_AddString(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  get_uds().del("bm_add_str");
  std::string val(state.range(0), 'x');
  for (auto _ : state)
  {
    auto t = get_uds().addString("bm_add_str", val, {.trim = 0});
    benchmark::DoNotOptimize(t);
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
  get_uds().del("bm_add_str");
}
BENCHMARK(BM_UDS_AddString)->Arg(16)->Arg(256)->Arg(4096);

static void BM_TCP_AddBlob(benchmark::State& state)
{
  get_tcp().del("bm_add_blob");
  std::vector<uint8_t> blob(state.range(0), 0xCD);
  for (auto _ : state)
  {
    auto t = get_tcp().addBlob("bm_add_blob", blob.data(), blob.size(), {.trim = 0});
    benchmark::DoNotOptimize(t);
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
  get_tcp().del("bm_add_blob");
}
BENCHMARK(BM_TCP_AddBlob)->Arg(64)->Arg(1024)->Arg(65536)->Arg(1048576);

static void BM_UDS_AddBlob(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  get_uds().del("bm_add_blob");
  std::vector<uint8_t> blob(state.range(0), 0xCD);
  for (auto _ : state)
  {
    auto t = get_uds().addBlob("bm_add_blob", blob.data(), blob.size(), {.trim = 0});
    benchmark::DoNotOptimize(t);
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
  get_uds().del("bm_add_blob");
}
BENCHMARK(BM_UDS_AddBlob)->Arg(64)->Arg(1024)->Arg(65536);

// --- Get single values ---

static void BM_TCP_GetDouble(benchmark::State& state)
{
  get_tcp().del("bm_get_dbl");
  get_tcp().addDouble("bm_get_dbl", 2.718);
  double dest;
  for (auto _ : state)
  {
    auto t = get_tcp().getDouble("bm_get_dbl", dest);
    benchmark::DoNotOptimize(t);
    benchmark::DoNotOptimize(dest);
  }
  get_tcp().del("bm_get_dbl");
}
BENCHMARK(BM_TCP_GetDouble);

static void BM_UDS_GetDouble(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  get_uds().del("bm_get_dbl");
  get_uds().addDouble("bm_get_dbl", 2.718);
  double dest;
  for (auto _ : state)
  {
    auto t = get_uds().getDouble("bm_get_dbl", dest);
    benchmark::DoNotOptimize(t);
    benchmark::DoNotOptimize(dest);
  }
  get_uds().del("bm_get_dbl");
}
BENCHMARK(BM_UDS_GetDouble);

static void BM_TCP_GetInt(benchmark::State& state)
{
  get_tcp().del("bm_get_int");
  get_tcp().addInt("bm_get_int", 99);
  int64_t dest;
  for (auto _ : state)
  {
    auto t = get_tcp().getInt("bm_get_int", dest);
    benchmark::DoNotOptimize(t);
    benchmark::DoNotOptimize(dest);
  }
  get_tcp().del("bm_get_int");
}
BENCHMARK(BM_TCP_GetInt);

static void BM_UDS_GetInt(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  get_uds().del("bm_get_int");
  get_uds().addInt("bm_get_int", 99);
  int64_t dest;
  for (auto _ : state)
  {
    auto t = get_uds().getInt("bm_get_int", dest);
    benchmark::DoNotOptimize(t);
    benchmark::DoNotOptimize(dest);
  }
  get_uds().del("bm_get_int");
}
BENCHMARK(BM_UDS_GetInt);

static void BM_TCP_GetString(benchmark::State& state)
{
  get_tcp().del("bm_get_str");
  std::string val(state.range(0), 'y');
  get_tcp().addString("bm_get_str", val);
  std::string dest;
  for (auto _ : state)
  {
    auto t = get_tcp().getString("bm_get_str", dest);
    benchmark::DoNotOptimize(t);
    benchmark::DoNotOptimize(dest);
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
  get_tcp().del("bm_get_str");
}
BENCHMARK(BM_TCP_GetString)->Arg(16)->Arg(256)->Arg(4096);

static void BM_UDS_GetString(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  get_uds().del("bm_get_str");
  std::string val(state.range(0), 'y');
  get_uds().addString("bm_get_str", val);
  std::string dest;
  for (auto _ : state)
  {
    auto t = get_uds().getString("bm_get_str", dest);
    benchmark::DoNotOptimize(t);
    benchmark::DoNotOptimize(dest);
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
  get_uds().del("bm_get_str");
}
BENCHMARK(BM_UDS_GetString)->Arg(16)->Arg(256)->Arg(4096);

// --- Range queries ---

static void BM_TCP_GetDoubles_Range(benchmark::State& state)
{
  get_tcp().del("bm_rng_dbl");
  for (int i = 0; i < state.range(0); ++i)
    get_tcp().addDouble("bm_rng_dbl", static_cast<double>(i), {.trim = 0});
  for (auto _ : state)
  {
    auto r = get_tcp().getDoubles("bm_rng_dbl", {.count = static_cast<uint32_t>(state.range(0))});
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
  get_tcp().del("bm_rng_dbl");
}
BENCHMARK(BM_TCP_GetDoubles_Range)->Arg(10)->Arg(100)->Arg(1000);

static void BM_UDS_GetDoubles_Range(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  get_uds().del("bm_rng_dbl");
  for (int i = 0; i < state.range(0); ++i)
    get_uds().addDouble("bm_rng_dbl", static_cast<double>(i), {.trim = 0});
  for (auto _ : state)
  {
    auto r = get_uds().getDoubles("bm_rng_dbl", {.count = static_cast<uint32_t>(state.range(0))});
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
  get_uds().del("bm_rng_dbl");
}
BENCHMARK(BM_UDS_GetDoubles_Range)->Arg(10)->Arg(100)->Arg(1000);

static void BM_TCP_GetDoublesBefore_Range(benchmark::State& state)
{
  get_tcp().del("bm_rev_dbl");
  for (int i = 0; i < state.range(0); ++i)
    get_tcp().addDouble("bm_rev_dbl", static_cast<double>(i), {.trim = 0});
  for (auto _ : state)
  {
    auto r = get_tcp().getDoublesBefore("bm_rev_dbl", {.count = static_cast<uint32_t>(state.range(0))});
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
  get_tcp().del("bm_rev_dbl");
}
BENCHMARK(BM_TCP_GetDoublesBefore_Range)->Arg(10)->Arg(100)->Arg(1000);

static void BM_UDS_GetDoublesBefore_Range(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  get_uds().del("bm_rev_dbl");
  for (int i = 0; i < state.range(0); ++i)
    get_uds().addDouble("bm_rev_dbl", static_cast<double>(i), {.trim = 0});
  for (auto _ : state)
  {
    auto r = get_uds().getDoublesBefore("bm_rev_dbl", {.count = static_cast<uint32_t>(state.range(0))});
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
  get_uds().del("bm_rev_dbl");
}
BENCHMARK(BM_UDS_GetDoublesBefore_Range)->Arg(10)->Arg(100)->Arg(1000);

// --- Bulk add ---

static void BM_TCP_AddDoubles_Bulk(benchmark::State& state)
{
  TimeDoubleList data;
  for (int i = 0; i < state.range(0); ++i)
    data.push_back({RAL_Time(), static_cast<double>(i)});
  for (auto _ : state)
  {
    get_tcp().del("bm_bulk_dbl");
    auto ids = get_tcp().addDoubles("bm_bulk_dbl", data, 0);
    benchmark::DoNotOptimize(ids);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
  get_tcp().del("bm_bulk_dbl");
}
BENCHMARK(BM_TCP_AddDoubles_Bulk)->Arg(10)->Arg(100)->Arg(1000);

static void BM_UDS_AddDoubles_Bulk(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  TimeDoubleList data;
  for (int i = 0; i < state.range(0); ++i)
    data.push_back({RAL_Time(), static_cast<double>(i)});
  for (auto _ : state)
  {
    get_uds().del("bm_bulk_dbl");
    auto ids = get_uds().addDoubles("bm_bulk_dbl", data, 0);
    benchmark::DoNotOptimize(ids);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
  get_uds().del("bm_bulk_dbl");
}
BENCHMARK(BM_UDS_AddDoubles_Bulk)->Arg(10)->Arg(100)->Arg(1000);

// --- Attrs ---

static void BM_TCP_AddAttrs(benchmark::State& state)
{
  get_tcp().del("bm_add_attr");
  Attrs attrs;
  for (int i = 0; i < state.range(0); ++i)
    attrs["field_" + std::to_string(i)] = "value_" + std::to_string(i);
  for (auto _ : state)
  {
    auto t = get_tcp().addAttrs("bm_add_attr", attrs, {.trim = 0});
    benchmark::DoNotOptimize(t);
  }
  get_tcp().del("bm_add_attr");
}
BENCHMARK(BM_TCP_AddAttrs)->Arg(1)->Arg(5)->Arg(20);

static void BM_UDS_AddAttrs(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  get_uds().del("bm_add_attr");
  Attrs attrs;
  for (int i = 0; i < state.range(0); ++i)
    attrs["field_" + std::to_string(i)] = "value_" + std::to_string(i);
  for (auto _ : state)
  {
    auto t = get_uds().addAttrs("bm_add_attr", attrs, {.trim = 0});
    benchmark::DoNotOptimize(t);
  }
  get_uds().del("bm_add_attr");
}
BENCHMARK(BM_UDS_AddAttrs)->Arg(1)->Arg(5)->Arg(20);

static void BM_TCP_GetAttrs(benchmark::State& state)
{
  get_tcp().del("bm_get_attr");
  Attrs attrs;
  for (int i = 0; i < state.range(0); ++i)
    attrs["field_" + std::to_string(i)] = "value_" + std::to_string(i);
  get_tcp().addAttrs("bm_get_attr", attrs);
  Attrs dest;
  for (auto _ : state)
  {
    auto t = get_tcp().getAttrs("bm_get_attr", dest);
    benchmark::DoNotOptimize(t);
    benchmark::DoNotOptimize(dest);
  }
  get_tcp().del("bm_get_attr");
}
BENCHMARK(BM_TCP_GetAttrs)->Arg(1)->Arg(5)->Arg(20);

static void BM_UDS_GetAttrs(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  get_uds().del("bm_get_attr");
  Attrs attrs;
  for (int i = 0; i < state.range(0); ++i)
    attrs["field_" + std::to_string(i)] = "value_" + std::to_string(i);
  get_uds().addAttrs("bm_get_attr", attrs);
  Attrs dest;
  for (auto _ : state)
  {
    auto t = get_uds().getAttrs("bm_get_attr", dest);
    benchmark::DoNotOptimize(t);
    benchmark::DoNotOptimize(dest);
  }
  get_uds().del("bm_get_attr");
}
BENCHMARK(BM_UDS_GetAttrs)->Arg(1)->Arg(5)->Arg(20);

// --- Connected (round-trip baseline) ---

static void BM_TCP_Connected(benchmark::State& state)
{
  for (auto _ : state)
  {
    bool c = get_tcp().connected();
    benchmark::DoNotOptimize(c);
  }
}
BENCHMARK(BM_TCP_Connected);

static void BM_UDS_Connected(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  for (auto _ : state)
  {
    bool c = get_uds().connected();
    benchmark::DoNotOptimize(c);
  }
}
BENCHMARK(BM_UDS_Connected);

// --- Add+Get cycle ---

static void BM_TCP_AddGetCycle(benchmark::State& state)
{
  get_tcp().del("bm_cycle");
  double dest;
  int64_t i = 0;
  for (auto _ : state)
  {
    get_tcp().addDouble("bm_cycle", static_cast<double>(i++));
    get_tcp().getDouble("bm_cycle", dest);
    benchmark::DoNotOptimize(dest);
  }
  get_tcp().del("bm_cycle");
}
BENCHMARK(BM_TCP_AddGetCycle);

static void BM_UDS_AddGetCycle(benchmark::State& state)
{
  SKIP_IF_NO_UDS();
  get_uds().del("bm_cycle");
  double dest;
  int64_t i = 0;
  for (auto _ : state)
  {
    get_uds().addDouble("bm_cycle", static_cast<double>(i++));
    get_uds().getDouble("bm_cycle", dest);
    benchmark::DoNotOptimize(dest);
  }
  get_uds().del("bm_cycle");
}
BENCHMARK(BM_UDS_AddGetCycle);

// ============================================================================
//  STRESS TESTS (TCP and UDS variants)
// ============================================================================

// Helper to create adapter with transport option
static RedisAdapterLite make_adapter(const std::string& base, bool use_uds)
{
  return use_uds ? RedisAdapterLite(base, uds_opts())
                 : RedisAdapterLite(base, tcp_opts());
}

// --- Stress: Parallel writers ---

static void Stress_ParallelWriters(benchmark::State& state, bool use_uds)
{
  if (use_uds) { SKIP_IF_NO_UDS(); }
  const int num_threads = state.range(0);
  const int ops_per_thread = 1000;

  int64_t all_ops = 0;
  for (auto _ : state)
  {
    std::vector<std::thread> threads;
    std::atomic<int64_t> total_ops{0};

    for (int t = 0; t < num_threads; ++t)
    {
      threads.emplace_back([t, ops_per_thread, &total_ops, use_uds]()
      {
        auto redis = make_adapter("STRESS_W" + std::to_string(t), use_uds);
        std::string key = "stress_pw_" + std::to_string(t);
        redis.del(key);
        for (int i = 0; i < ops_per_thread; ++i)
        {
          redis.addDouble(key, static_cast<double>(i), {.trim = 0});
          total_ops.fetch_add(1, std::memory_order_relaxed);
        }
        redis.del(key);
      });
    }
    for (auto& th : threads) th.join();
    all_ops += total_ops.load();
  }
  state.SetItemsProcessed(all_ops);
}

static void BM_TCP_Stress_ParallelWriters(benchmark::State& s) { Stress_ParallelWriters(s, false); }
static void BM_UDS_Stress_ParallelWriters(benchmark::State& s) { Stress_ParallelWriters(s, true); }
BENCHMARK(BM_TCP_Stress_ParallelWriters)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->UseRealTime();
BENCHMARK(BM_UDS_Stress_ParallelWriters)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->UseRealTime();

// --- Stress: Parallel readers ---

static void Stress_ParallelReaders(benchmark::State& state, bool use_uds)
{
  if (use_uds) { SKIP_IF_NO_UDS(); }
  const int num_threads = state.range(0);
  const int stream_size = 10000;
  const int reads_per_thread = 100;
  const uint32_t read_count = 100;

  {
    auto redis = make_adapter("STRESS_R", use_uds);
    redis.del("stress_pr");
    for (int i = 0; i < stream_size; ++i)
      redis.addDouble("stress_pr", static_cast<double>(i), {.trim = 0});
  }

  int64_t all_items = 0;
  for (auto _ : state)
  {
    std::vector<std::thread> threads;
    std::atomic<int64_t> total_items{0};

    for (int t = 0; t < num_threads; ++t)
    {
      threads.emplace_back([reads_per_thread, read_count, &total_items, use_uds]()
      {
        auto redis = make_adapter("STRESS_R", use_uds);
        for (int i = 0; i < reads_per_thread; ++i)
        {
          auto results = redis.getDoubles("stress_pr", {.count = read_count});
          total_items.fetch_add(results.size(), std::memory_order_relaxed);
        }
      });
    }
    for (auto& th : threads) th.join();
    all_items += total_items.load();
  }
  state.SetItemsProcessed(all_items);

  { auto redis = make_adapter("STRESS_R", use_uds); redis.del("stress_pr"); }
}

static void BM_TCP_Stress_ParallelReaders(benchmark::State& s) { Stress_ParallelReaders(s, false); }
static void BM_UDS_Stress_ParallelReaders(benchmark::State& s) { Stress_ParallelReaders(s, true); }
BENCHMARK(BM_TCP_Stress_ParallelReaders)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->UseRealTime();
BENCHMARK(BM_UDS_Stress_ParallelReaders)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->UseRealTime();

// --- Stress: Mixed read/write ---

static void Stress_MixedReadWrite(benchmark::State& state, bool use_uds)
{
  if (use_uds) { SKIP_IF_NO_UDS(); }
  const int total_threads = state.range(0);
  const int writers = total_threads / 2;
  const int readers = total_threads - writers;
  const int ops = 500;

  int64_t all_ops = 0;
  for (auto _ : state)
  {
    auto setup = make_adapter("STRESS_MIX", use_uds);
    setup.del("stress_mix");
    for (int i = 0; i < 100; ++i)
      setup.addDouble("stress_mix", static_cast<double>(i), {.trim = 0});

    std::atomic<int64_t> total_ops{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < writers; ++t)
    {
      threads.emplace_back([ops, &total_ops, use_uds]()
      {
        auto redis = make_adapter("STRESS_MIX", use_uds);
        for (int i = 0; i < ops; ++i)
        {
          redis.addDouble("stress_mix", static_cast<double>(i), {.trim = 0});
          total_ops.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }

    for (int t = 0; t < readers; ++t)
    {
      threads.emplace_back([ops, &total_ops, use_uds]()
      {
        auto redis = make_adapter("STRESS_MIX", use_uds);
        double dest;
        for (int i = 0; i < ops; ++i)
        {
          redis.getDouble("stress_mix", dest);
          total_ops.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }

    for (auto& th : threads) th.join();
    all_ops += total_ops.load();
    setup.del("stress_mix");
  }
  state.SetItemsProcessed(all_ops);
}

static void BM_TCP_Stress_MixedReadWrite(benchmark::State& s) { Stress_MixedReadWrite(s, false); }
static void BM_UDS_Stress_MixedReadWrite(benchmark::State& s) { Stress_MixedReadWrite(s, true); }
BENCHMARK(BM_TCP_Stress_MixedReadWrite)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->UseRealTime();
BENCHMARK(BM_UDS_Stress_MixedReadWrite)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->UseRealTime();

// --- Stress: Large blobs ---

static void Stress_LargeBlob(benchmark::State& state, bool use_uds)
{
  if (use_uds) { SKIP_IF_NO_UDS(); }
  auto& redis = use_uds ? get_uds() : get_tcp();
  const size_t size = state.range(0);
  std::vector<uint8_t> blob(size);
  std::mt19937 rng(42);
  for (auto& b : blob) b = static_cast<uint8_t>(rng());

  redis.del("stress_blob");
  for (auto _ : state)
  {
    auto t = redis.addBlob("stress_blob", blob.data(), blob.size(), {.trim = 0});
    benchmark::DoNotOptimize(t);
  }
  state.SetBytesProcessed(state.iterations() * size);
  redis.del("stress_blob");
}

static void BM_TCP_Stress_LargeBlob(benchmark::State& s) { Stress_LargeBlob(s, false); }
static void BM_UDS_Stress_LargeBlob(benchmark::State& s) { Stress_LargeBlob(s, true); }
BENCHMARK(BM_TCP_Stress_LargeBlob)->Arg(1<<10)->Arg(1<<14)->Arg(1<<16)->Arg(1<<18)->Arg(1<<20)->UseRealTime();
BENCHMARK(BM_UDS_Stress_LargeBlob)->Arg(1<<10)->Arg(1<<14)->Arg(1<<16)->Arg(1<<18)->Arg(1<<20)->UseRealTime();

// --- Stress: Deep stream ---

static void Stress_DeepStream(benchmark::State& state, bool use_uds)
{
  if (use_uds) { SKIP_IF_NO_UDS(); }
  auto& redis = use_uds ? get_uds() : get_tcp();
  const int stream_depth = 100000;
  const uint32_t query_count = static_cast<uint32_t>(state.range(0));

  // Use separate flags per transport
  static bool tcp_populated = false;
  static bool uds_populated = false;
  bool& populated = use_uds ? uds_populated : tcp_populated;

  if (!populated)
  {
    redis.del("stress_deep");
    fprintf(stderr, "Populating deep stream [%s] (%d entries)...\n",
            use_uds ? "UDS" : "TCP", stream_depth);
    for (int i = 0; i < stream_depth; ++i)
      redis.addDouble("stress_deep", static_cast<double>(i), {.trim = 0});
    fprintf(stderr, "Done.\n");
    populated = true;
  }

  for (auto _ : state)
  {
    auto results = redis.getDoubles("stress_deep", {.count = query_count});
    benchmark::DoNotOptimize(results);
  }
  state.SetItemsProcessed(state.iterations() * query_count);
}

static void BM_TCP_Stress_DeepStream(benchmark::State& s) { Stress_DeepStream(s, false); }
static void BM_UDS_Stress_DeepStream(benchmark::State& s) { Stress_DeepStream(s, true); }
BENCHMARK(BM_TCP_Stress_DeepStream)->Arg(100)->Arg(1000)->Arg(10000)->Arg(50000)->UseRealTime();
BENCHMARK(BM_UDS_Stress_DeepStream)->Arg(100)->Arg(1000)->Arg(10000)->Arg(50000)->UseRealTime();

// --- Stress: Many streams ---

static void Stress_ManyStreams(benchmark::State& state, bool use_uds)
{
  if (use_uds) { SKIP_IF_NO_UDS(); }
  const int num_streams = state.range(0);

  for (auto _ : state)
  {
    auto redis = make_adapter("STRESS_MANY", use_uds);
    for (int i = 0; i < num_streams; ++i)
    {
      std::string key = "stress_ms_" + std::to_string(i);
      redis.addDouble(key, static_cast<double>(i));
    }
    for (int i = 0; i < num_streams; ++i)
      redis.del("stress_ms_" + std::to_string(i));
  }
  state.SetItemsProcessed(state.iterations() * num_streams);
}

static void BM_TCP_Stress_ManyStreams(benchmark::State& s) { Stress_ManyStreams(s, false); }
static void BM_UDS_Stress_ManyStreams(benchmark::State& s) { Stress_ManyStreams(s, true); }
BENCHMARK(BM_TCP_Stress_ManyStreams)->Arg(10)->Arg(100)->Arg(1000)->UseRealTime();
BENCHMARK(BM_UDS_Stress_ManyStreams)->Arg(10)->Arg(100)->Arg(1000)->UseRealTime();

// ============================================================================

int main(int argc, char** argv)
{
  g_uds_available = uds_available();
  fprintf(stderr, "Unix domain socket (%s): %s\n",
          UDS_PATH.c_str(), g_uds_available ? "AVAILABLE" : "not found, UDS tests will skip");

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
