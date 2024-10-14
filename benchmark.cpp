#include <benchmark/benchmark.h>
#include "RedisAdapter.hpp"
#include <cstdlib>  // For std::getenv
#include <vector>   // For std::vector

using namespace std;
using namespace sw::redis;

// Helper function to retrieve the host from the environment variable
std::string get_redis_host() {
    const char* env_host = std::getenv("REDIS_ADAPTER_BENCHMARK_HOST");
    return env_host ? std::string(env_host) : "localhost";  // Default to "localhost" if not set
}

// Helper function to generate a vector of floats of a given size
std::vector<float> generate_list(size_t size, float value = 1.0f) {
    return std::vector<float>(size, value);  // Fill the list with the given value
}

// Single value add benchmark
static void Benchmark_AddSingleValue(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = get_redis_host() });
    for (auto _ : state) {
        redis.addSingleValue("benchmark_key", "benchmark_value");
    }
}

// Single value get benchmark
static void Benchmark_GetSingleValue(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = get_redis_host() });
    redis.addSingleValue("benchmark_key", "benchmark_value");
    for (auto _ : state) {
        std::string value;
        redis.getSingleValue("benchmark_key", value);
    }
}

// Add list to Redis benchmark with dynamic size
static void Benchmark_AddList(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = get_redis_host() });
    size_t size = state.range(0);  // Use the range to define the size of the list
    std::vector<float> values = generate_list(size);

    for (auto _ : state) {
        redis.addSingleList("benchmark_list_key", values, {.trim = 100});
    }
}

// Get list from Redis benchmark with dynamic size
static void Benchmark_GetList(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = get_redis_host() });
    size_t size = state.range(0);
    std::vector<float> values = generate_list(size);
    redis.addSingleList("benchmark_list_key", values, {.trim = 100});

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_list_key", result);
    }
}

//Add Single Value
BENCHMARK(Benchmark_AddSingleValue);
//Get Single Value
BENCHMARK(Benchmark_GetSingleValue);

//Add List of different sizes
BENCHMARK(Benchmark_AddList)->Arg(256)->Arg(512)->Arg(1024)->Arg(2048)->Arg(4096)
                            ->Arg(8192)->Arg(16384)->Arg(32768)->Arg(65536)
                            ->Arg(131072)->Arg(262144)->Arg(524288)->Arg(1048576)
                            ->Arg(2097152)->Arg(4194304)->Arg(8388608);  // Up to 8MB
//Get List of different sizes
BENCHMARK(Benchmark_GetList)->Arg(256)->Arg(512)->Arg(1024)->Arg(2048)->Arg(4096)
                            ->Arg(8192)->Arg(16384)->Arg(32768)->Arg(65536)
                            ->Arg(131072)->Arg(262144)->Arg(524288)->Arg(1048576)
                            ->Arg(2097152)->Arg(4194304)->Arg(8388608);  // Up to 8MB

BENCHMARK_MAIN();

