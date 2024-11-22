#include <benchmark/benchmark.h>
#include "RedisAdapter.hpp"
#include "RedisCache.hpp"
#include <cstdlib>
#include <vector>

using namespace std;
using namespace sw::redis;

// Helper function to retrieve the host from the environment variable
std::string get_redis_path()
{
    const char* env_host = std::getenv("REDIS_ADAPTER_BENCHMARK_HOST");
    if (env_host && std::string(env_host).find(".sock") != std::string::npos) { return env_host; }
    return "";
}

// Helper function to retrieve the host from the environment variable
std::string get_redis_host()
{
    const char* env_host = std::getenv("REDIS_ADAPTER_BENCHMARK_HOST");
    if (env_host && std::string(env_host).find(".sock") == std::string::npos) { return env_host; }
    return "localhost";
}

// Helper function to generate a vector of floats of a given size
std::vector<float> generate_list(size_t size, float value = 1.0f)
{
   return std::vector<float>(size, value);  // Fill the list with the given value
}

static void Benchmark_Baseline(benchmark::State& state)
{
    // Do to as close to nothing as possible to measure the overhead of google benchmark
    for (auto _ : state) { benchmark::DoNotOptimize(_); }
}

// Single value add benchmark
static void Benchmark_AddSingleValue(benchmark::State& state)
{
   RedisAdapter redis("TEST", { .path = get_redis_path(), .host = get_redis_host() });
   for (auto _ : state) { redis.addSingleValue("benchmark_key", "benchmark_value");}
}

// Single value get benchmark
static void Benchmark_GetSingleValue(benchmark::State& state)
{
   RedisAdapter redis("TEST", { .path = get_redis_path(), .host = get_redis_host() });
    redis.addSingleValue("benchmark_key", "benchmark_value");
    for (auto _ : state) { std::string value; redis.getSingleValue("benchmark_key", value);}
}

// Add list to Redis benchmark with dynamic size
static void Benchmark_AddList(benchmark::State& state)
{
   RedisAdapter redis("TEST", { .path = get_redis_path(), .host = get_redis_host() });
    size_t size = state.range(0);  // Use the range to define the size of the list
    std::vector<float> values = generate_list(size);

    for (auto _ : state) { redis.addSingleList("benchmark_list_key", values, {.trim = 100});}
}

// Get list from Redis benchmark with dynamic size
static void Benchmark_GetList(benchmark::State& state)
{
   RedisAdapter redis("TEST", { .path = get_redis_path(), .host = get_redis_host() });
    size_t size = state.range(0);
    std::vector<float> values = generate_list(size);
    redis.addSingleList("benchmark_list_key", values, {.trim = 100});

    for (auto _ : state) { std::vector<float> result; redis.getSingleList("benchmark_list_key", result);}
}

static void Benchmark_copyReadBuffer_Full(benchmark::State& state)
{
    std::shared_ptr<RedisAdapter> redis = std::make_shared<RedisAdapter>("TEST",
        RedisConnection::Options{ .path = get_redis_path(), .host = get_redis_host() });
    size_t size = state.range(0);
    std::vector<float> values = generate_list(size);
    redis->addSingleList("benchmark_list_key", values, {.trim = 100});
    std::string key = "benchmark_list_key";
    RedisCache<float> cache(redis, key);

    std::vector<float> tempInitilizaer;
    cache.copyReadBuffer(tempInitilizaer);

    for (auto _ : state) { std::vector<float> result; cache.copyReadBuffer(result);}
}

static void Benchmark_copyReadBuffer_SingleValue(benchmark::State& state)
{
    std::shared_ptr<RedisAdapter> redis = std::make_shared<RedisAdapter>("TEST",
        RedisConnection::Options{ .path = get_redis_path(), .host = get_redis_host() });
    size_t size = state.range(0);
    std::vector<float> values = generate_list(size);
    redis->addSingleList("benchmark_list_key", values, {.trim = 100});
    std::string key = "benchmark_list_key";
    RedisCache<float> cache(redis, key);

    std::vector<float> tempInitilizaer;
    cache.copyReadBuffer(tempInitilizaer);
    float result;
    std::span<float> resultSpan(&result, 1);
    int arbitraryStartIndex = 42;
    for (auto _ : state) { cache.copyReadBuffer(resultSpan, arbitraryStartIndex);}
}

static void Benchmark_copyReadBuffer_FiftyValues(benchmark::State& state)
{
    std::shared_ptr<RedisAdapter> redis = std::make_shared<RedisAdapter>("TEST",
        RedisConnection::Options{ .path = get_redis_path(), .host = get_redis_host() });
    size_t size = state.range(0);
    std::vector<float> values = generate_list(size);
    redis->addSingleList("benchmark_list_key", values, {.trim = 100});
    std::string key = "benchmark_list_key";
    RedisCache<float> cache(redis, key);

    std::vector<float> tempInitilizaer;
    cache.copyReadBuffer(tempInitilizaer);
    float result[50];
    std::span<float> resultSpan(result, 50);
    int arbitraryStartIndex = 42;
    for (auto _ : state) { cache.copyReadBuffer(resultSpan, arbitraryStartIndex);}
}

// Baseline
BENCHMARK(Benchmark_Baseline);
//Add Single Value
BENCHMARK(Benchmark_AddSingleValue);
//Get Single Value
BENCHMARK(Benchmark_GetSingleValue);

//Add List of different sizes
BENCHMARK(Benchmark_AddList)->Arg(256)->Arg(512)->Arg(1024)->Arg(1536)->Arg(2048)->Arg(3072)->Arg(4096)
                            ->Arg(6144)->Arg(8192)->Arg(12288)->Arg(16384)->Arg(24576)->Arg(32768)->Arg(49152)
                            ->Arg(65536)->Arg(131072)->Arg(262144)->Arg(524288)->Arg(1048576)->Arg(2097152)
                            ->Arg(4194304)->Arg(8388608);  // These are vector sizes, total size in bytes will be 4x
//Get List of different sizes
BENCHMARK(Benchmark_GetList)->Arg(256)->Arg(512)->Arg(1024)->Arg(1536)->Arg(2048)->Arg(3072)->Arg(4096)
                            ->Arg(6144)->Arg(8192)->Arg(12288)->Arg(16384)->Arg(24576)->Arg(32768)->Arg(49152)
                            ->Arg(65536)->Arg(131072)->Arg(262144)->Arg(524288)->Arg(1048576)->Arg(2097152)
                            ->Arg(4194304)->Arg(8388608);  // These are vector sizes, total size in bytes will be 4x

//Get List of different sizes Cached
BENCHMARK(Benchmark_copyReadBuffer_Full)->Arg(256)->Arg(512)->Arg(1024)->Arg(1536)->Arg(2048)->Arg(3072)->Arg(4096)
                            ->Arg(6144)->Arg(8192)->Arg(12288)->Arg(16384)->Arg(24576)->Arg(32768)->Arg(49152)
                            ->Arg(65536)->Arg(131072)->Arg(262144)->Arg(524288)->Arg(1048576)->Arg(2097152)
                            ->Arg(4194304)->Arg(8388608);  // These are vector sizes, total size in bytes will be 4x

//Get one element of List of different sizes Cached
BENCHMARK(Benchmark_copyReadBuffer_SingleValue)->Arg(256)->Arg(512)->Arg(1024)->Arg(1536)->Arg(2048)->Arg(3072)->Arg(4096)
                            ->Arg(6144)->Arg(8192)->Arg(12288)->Arg(16384)->Arg(24576)->Arg(32768)->Arg(49152)
                            ->Arg(65536)->Arg(131072)->Arg(262144)->Arg(524288)->Arg(1048576)->Arg(2097152)
                            ->Arg(4194304)->Arg(8388608);  // These are vector sizes, total size in bytes will be 4x

//Get Fifty elements of List of different sizes Cached
BENCHMARK(Benchmark_copyReadBuffer_FiftyValues)->Arg(256)->Arg(512)->Arg(1024)->Arg(1536)->Arg(2048)->Arg(3072)->Arg(4096)
                            ->Arg(6144)->Arg(8192)->Arg(12288)->Arg(16384)->Arg(24576)->Arg(32768)->Arg(49152)
                            ->Arg(65536)->Arg(131072)->Arg(262144)->Arg(524288)->Arg(1048576)->Arg(2097152)
                            ->Arg(4194304)->Arg(8388608);  // These are vector sizes, total size in bytes will be 4x

BENCHMARK_MAIN();

