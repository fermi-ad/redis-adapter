#include <benchmark/benchmark.h>
#include "RedisAdapter.hpp"

using namespace std;
using namespace sw::redis;

// Single value add benchmark
static void Benchmark_AddSingleValue(benchmark::State& state) {
    RedisAdapter redis("TEST");
    for (auto _ : state) {
        redis.addSingleValue("benchmark_key", "benchmark_value");
    }
}
BENCHMARK(Benchmark_AddSingleValue);

// Single value get benchmark
static void Benchmark_GetSingleValue(benchmark::State& state) {
    RedisAdapter redis("TEST");
    redis.addSingleValue("benchmark_key", "benchmark_value");
    for (auto _ : state) {
        std::string value;
        redis.getSingleValue("benchmark_key", value);
    }
}
BENCHMARK(Benchmark_GetSingleValue);

// Add list to Redis benchmark
static void Benchmark_AddList(benchmark::State& state) {
    RedisAdapter redis("TEST");
    std::vector<float> values = {1.1, 2.2, 3.3, 4.4, 5.5};

    for (auto _ : state) {
        redis.addSingleList("benchmark_list_key", values, {.trim = 100});
    }
}
BENCHMARK(Benchmark_AddList);

// Get list from Redis benchmark
static void Benchmark_GetList(benchmark::State& state) {
    RedisAdapter redis("TEST");
    std::vector<float> values = {1.1, 2.2, 3.3, 4.4, 5.5};
    redis.addSingleList("benchmark_list_key", values, {.trim = 100});

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_list_key", result);
    }
}
BENCHMARK(Benchmark_GetList);

// Benchmark for adding a 1 KB list to Redis
static void Benchmark_Add1KBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    // 1 KB = 1024 bytes; each float is 4 bytes, so 1024 / 4 = 256 elements
    std::vector<float> small_list(256, 1.0f);  // Fill with the value 1.0f

    for (auto _ : state) {
        redis.addSingleList("benchmark_1kb_list_key", small_list);
    }
}
BENCHMARK(Benchmark_Add1KBList);

// Benchmark for retrieving a 1 KB list from Redis
static void Benchmark_Get1KBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    // 1 KB = 1024 bytes; each float is 4 bytes, so 1024 / 4 = 256 elements
    std::vector<float> small_list(256, 1.0f);
    redis.addSingleList("benchmark_1kb_list_key", small_list, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_1kb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get1KBList);


// Benchmark for adding a 2 KB list to Redis
static void Benchmark_Add2KBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    // 2 KB = 2048 bytes; each float is 4 bytes, so 2048 / 4 = 512 elements
    std::vector<float> list_2kb(512, 1.0f);  // Fill with the value 1.0f

    for (auto _ : state) {
        redis.addSingleList("benchmark_2kb_list_key", list_2kb, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add2KBList);

// Benchmark for retrieving a 2 KB list from Redis
static void Benchmark_Get2KBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    std::vector<float> list_2kb(512, 1.0f);
    redis.addSingleList("benchmark_2kb_list_key", list_2kb, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_2kb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get2KBList);

// Benchmark for adding a 5 KB list to Redis
static void Benchmark_Add5KBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    // 5 KB = 5120 bytes; each float is 4 bytes, so 5120 / 4 = 1280 elements
    std::vector<float> list_5kb(1280, 1.0f);  // Fill with the value 1.0f

    for (auto _ : state) {
        redis.addSingleList("benchmark_5kb_list_key", list_5kb, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add5KBList);

// Benchmark for retrieving a 5 KB list from Redis
static void Benchmark_Get5KBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    std::vector<float> list_5kb(1280, 1.0f);
    redis.addSingleList("benchmark_5kb_list_key", list_5kb, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_5kb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get5KBList);

// Benchmark for adding a 10 KB list to Redis
static void Benchmark_Add10KBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    // 10 KB = 10240 bytes; each float is 4 bytes, so 10240 / 4 = 2560 elements
    std::vector<float> list_10kb(2560, 1.0f);  // Fill with the value 1.0f

    for (auto _ : state) {
        redis.addSingleList("benchmark_10kb_list_key", list_10kb, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add10KBList);

// Benchmark for retrieving a 10 KB list from Redis
static void Benchmark_Get10KBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    std::vector<float> list_10kb(2560, 1.0f);
    redis.addSingleList("benchmark_10kb_list_key", list_10kb, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_10kb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get10KBList);


// Benchmark for adding a 250 KB list to Redis
static void Benchmark_Add250KBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    // 250 KB = 250 * 1024 bytes; each float is 4 bytes, so (250 * 1024) / 4 = 64,000 elements
    std::vector<float> mid_list(64000, 1.0f);  // Fill with the value 1.0f

    for (auto _ : state) {
        redis.addSingleList("benchmark_250kb_list_key", mid_list, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add250KBList);

// Benchmark for retrieving a 250 KB list from Redis
static void Benchmark_Get250KBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    // 250 KB = 250 * 1024 bytes; each float is 4 bytes, so (250 * 1024) / 4 = 64,000 elements
    std::vector<float> mid_list(64000, 1.0f);
    redis.addSingleList("benchmark_250kb_list_key", mid_list, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_250kb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get250KBList);

// Benchmark for adding a 500 KB list to Redis
static void Benchmark_Add500KBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    // 500 KB = 512000 bytes; each float is 4 bytes, so 512000 / 4 = 128,000 elements
    std::vector<float> list_500kb(128000, 1.0f);  // Fill with the value 1.0f

    for (auto _ : state) {
        redis.addSingleList("benchmark_500kb_list_key", list_500kb, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add500KBList);

// Benchmark for retrieving a 500 KB list from Redis
static void Benchmark_Get500KBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    std::vector<float> list_500kb(128000, 1.0f);
    redis.addSingleList("benchmark_500kb_list_key", list_500kb, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_500kb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get500KBList);

// Benchmark for adding a 1 MB list to Redis
static void Benchmark_Add1MBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    // 1 MB = 1024 * 1024 bytes; each float is 4 bytes, so (1024 * 1024) / 4 = 262,144 elements
    std::vector<float> large_list(262144, 1.0f);  // Fill with the value 1.0f

    for (auto _ : state) {
        redis.addSingleList("benchmark_1mb_list_key", large_list, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add1MBList);

// Benchmark for retrieving a 1 MB list from Redis
static void Benchmark_Get1MBList(benchmark::State& state) {
    RedisAdapter redis("TEST");

    // 1 MB = 1024 * 1024 bytes; each float is 4 bytes, so (1024 * 1024) / 4 = 262,144 elements
    std::vector<float> large_list(262144, 1.0f);
    redis.addSingleList("benchmark_1mb_list_key", large_list, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_1mb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get1MBList);




// Domain socket connection benchmarks
// Single value add benchmark with domain socket connection
static void Benchmark_AddSingleValue_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });
    for (auto _ : state) {
        redis.addSingleValue("benchmark_key", "benchmark_value");
    }
}
BENCHMARK(Benchmark_AddSingleValue_Domain);

// Single value get benchmark with domain socket connection
static void Benchmark_GetSingleValue_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });
    redis.addSingleValue("benchmark_key", "benchmark_value");
    for (auto _ : state) {
        std::string value;
        redis.getSingleValue("benchmark_key", value);
    }
}
BENCHMARK(Benchmark_GetSingleValue_Domain);

// Add list to Redis benchmark with domain socket connection
static void Benchmark_AddList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });
    std::vector<float> values = {1.1, 2.2, 3.3, 4.4, 5.5};

    for (auto _ : state) {
        redis.addSingleList("benchmark_list_key", values, {.trim = 100});
    }
}
BENCHMARK(Benchmark_AddList_Domain);

// Get list from Redis benchmark with domain socket connection
static void Benchmark_GetList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });
    std::vector<float> values = {1.1, 2.2, 3.3, 4.4, 5.5};
    redis.addSingleList("benchmark_list_key", values, {.trim = 100});

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_list_key", result);
    }
}
BENCHMARK(Benchmark_GetList_Domain);

// Benchmark for adding a 1 KB list to Redis with domain socket connection
static void Benchmark_Add1KBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> small_list(256, 1.0f);  // 1 KB = 256 floats (each float is 4 bytes)
    for (auto _ : state) {
        redis.addSingleList("benchmark_1kb_list_key", small_list, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add1KBList_Domain);

// Benchmark for retrieving a 1 KB list from Redis with domain socket connection
static void Benchmark_Get1KBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> small_list(256, 1.0f);
    redis.addSingleList("benchmark_1kb_list_key", small_list, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_1kb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get1KBList_Domain);


// Benchmark for adding a 2 KB list to Redis with domain socket connection
static void Benchmark_Add2KBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> list_2kb(512, 1.0f);  // 2 KB = 512 floats (each float is 4 bytes)
    for (auto _ : state) {
        redis.addSingleList("benchmark_2kb_list_key", list_2kb, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add2KBList_Domain);

// Benchmark for retrieving a 2 KB list from Redis with domain socket connection
static void Benchmark_Get2KBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> list_2kb(512, 1.0f);
    redis.addSingleList("benchmark_2kb_list_key", list_2kb, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_2kb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get2KBList_Domain);

// Benchmark for adding a 5 KB list to Redis with domain socket connection
static void Benchmark_Add5KBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> list_5kb(1280, 1.0f);  // 5 KB = 1280 floats (each float is 4 bytes)
    for (auto _ : state) {
        redis.addSingleList("benchmark_5kb_list_key", list_5kb, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add5KBList_Domain);

// Benchmark for retrieving a 5 KB list from Redis with domain socket connection
static void Benchmark_Get5KBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> list_5kb(1280, 1.0f);
    redis.addSingleList("benchmark_5kb_list_key", list_5kb, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_5kb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get5KBList_Domain);

// Benchmark for adding a 10 KB list to Redis with domain socket connection
static void Benchmark_Add10KBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> list_10kb(2560, 1.0f);  // 10 KB = 2560 floats (each float is 4 bytes)
    for (auto _ : state) {
        redis.addSingleList("benchmark_10kb_list_key", list_10kb, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add10KBList_Domain);

// Benchmark for retrieving a 10 KB list from Redis with domain socket connection
static void Benchmark_Get10KBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> list_10kb(2560, 1.0f);
    redis.addSingleList("benchmark_10kb_list_key", list_10kb, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_10kb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get10KBList_Domain);

// Benchmark for adding a 250 KB list to Redis with domain socket connection
static void Benchmark_Add250KBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> mid_list(64000, 1.0f);  // 250 KB = 64000 floats (each float is 4 bytes)
    for (auto _ : state) {
        redis.addSingleList("benchmark_250kb_list_key", mid_list, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add250KBList_Domain);

// Benchmark for retrieving a 250 KB list from Redis with domain socket connection
static void Benchmark_Get250KBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> mid_list(64000, 1.0f);
    redis.addSingleList("benchmark_250kb_list_key", mid_list, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_250kb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get250KBList_Domain);

// Benchmark for adding a 500 KB list to Redis with domain socket connection
static void Benchmark_Add500KBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> list_500kb(128000, 1.0f);  // 500 KB = 128,000 floats (each float is 4 bytes)
    for (auto _ : state) {
        redis.addSingleList("benchmark_500kb_list_key", list_500kb, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add500KBList_Domain);

// Benchmark for retrieving a 500 KB list from Redis with domain socket connection
static void Benchmark_Get500KBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> list_500kb(128000, 1.0f);
    redis.addSingleList("benchmark_500kb_list_key", list_500kb, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_500kb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get500KBList_Domain);

// Benchmark for adding a 1 MB list to Redis with domain socket connection
static void Benchmark_Add1MBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> large_list(262144, 1.0f);  // 1 MB = 262144 floats (each float is 4 bytes)
    for (auto _ : state) {
        redis.addSingleList("benchmark_1mb_list_key", large_list, {.trim = 100});
    }
}
BENCHMARK(Benchmark_Add1MBList_Domain);

// Benchmark for retrieving a 1 MB list from Redis with domain socket connection
static void Benchmark_Get1MBList_Domain(benchmark::State& state) {
    RedisAdapter redis("TEST", { .host = "/tmp/redis.sock" });

    std::vector<float> large_list(262144, 1.0f);
    redis.addSingleList("benchmark_1mb_list_key", large_list, {.trim = 100});  // Pre-add data

    for (auto _ : state) {
        std::vector<float> result;
        redis.getSingleList("benchmark_1mb_list_key", result);
    }
}
BENCHMARK(Benchmark_Get1MBList_Domain);



BENCHMARK_MAIN();
