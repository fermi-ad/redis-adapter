//
//  Baseline-Subtract Adapter
//
//  output[i] = A[i] - B[i]
//
//  Trigger: InputKeyA; InputKeyB cached
//  Usage: baseline-subtract <config.yml>
//

#include "waveform_utils.hpp"
#include <yaml-cpp/yaml.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

static std::atomic<bool> g_running{true};
static void sigHandler(int) { g_running = false; }

struct Config
{
    std::string deviceName;
    std::string redisHost = "127.0.0.1";
    std::string inputKeyA;
    std::string inputKeyB;
    std::string outputKey;
    DataType    dataType = DataType::Float32;
};

static bool parseConfig(const std::string& path, Config& cfg)
{
    YAML::Node root;
    try { root = YAML::LoadFile(path); }
    catch (const std::exception& e)
    {
        fprintf(stderr, "Failed to load config %s: %s\n", path.c_str(), e.what());
        return false;
    }

    auto dev = root["Device"];
    if (!dev) { fprintf(stderr, "Config missing 'Device' root key\n"); return false; }

    cfg.deviceName = dev["DeviceName"].as<std::string>("DEVICE:0001");
    cfg.redisHost  = dev["RedisHost"].as<std::string>("127.0.0.1");
    cfg.inputKeyA  = dev["InputKeyA"].as<std::string>("");
    cfg.inputKeyB  = dev["InputKeyB"].as<std::string>("");
    cfg.outputKey  = dev["OutputKey"].as<std::string>("");
    cfg.dataType   = parseDataType(dev["DataType"].as<std::string>("float32"));

    if (cfg.inputKeyA.empty() || cfg.inputKeyB.empty() || cfg.outputKey.empty())
    {
        fprintf(stderr, "Config requires InputKeyA, InputKeyB, and OutputKey\n");
        return false;
    }
    return true;
}

int main(int argc, char* argv[])
{
    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <config.yml>\n", argv[0]);
        return 1;
    }

    Config cfg;
    if (!parseConfig(argv[1], cfg)) return 1;

    printf("[baseline-subtract] Device: %s  Redis: %s  DataType: %s\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str(), dataTypeName(cfg.dataType));
    printf("[baseline-subtract] A: %s  B: %s  Out: %s\n",
           cfg.inputKeyA.c_str(), cfg.inputKeyB.c_str(), cfg.outputKey.c_str());

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = cfg.deviceName + ":BASELINE_SUBTRACT";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[baseline-subtract] Failed to connect to Redis\n");
        return 1;
    }

    std::mutex baselineMtx;
    std::vector<double> cachedBaseline;
    uint64_t processCount = 0;

    redis.setDeferReaders(true);

    redis.addReader(cfg.inputKeyB,
        [&baselineMtx, &cachedBaseline, &cfg]
        (const std::string&, const std::string&, const TimeAttrsList& data)
    {
        for (auto& [time, attrs] : data)
        {
            auto blob = ral_to_blob(attrs);
            if (!blob) continue;
            auto wf = deserializeWaveform(*blob, cfg.dataType);
            std::lock_guard<std::mutex> lk(baselineMtx);
            cachedBaseline = std::move(wf);
        }
    });

    redis.addReader(cfg.inputKeyA,
        [&](const std::string&, const std::string&, const TimeAttrsList& data)
    {
        for (auto& [time, attrs] : data)
        {
            auto blob = ral_to_blob(attrs);
            if (!blob) continue;
            auto a = deserializeWaveform(*blob, cfg.dataType);

            std::vector<double> b;
            { std::lock_guard<std::mutex> lk(baselineMtx); b = cachedBaseline; }

            size_t n = a.size();
            std::vector<double> out(n);
            for (size_t i = 0; i < n; ++i)
                out[i] = a[i] - (i < b.size() ? b[i] : 0.0);

            serializeAndWrite(redis, cfg.outputKey, out, cfg.dataType, time);

            if (++processCount % 200 == 0)
                printf("[baseline-subtract] processed %lu samples\n",
                       (unsigned long)processCount);
        }
    });

    redis.setDeferReaders(false);

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);
    printf("[baseline-subtract] Running, waiting for data...\n");

    while (g_running)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    printf("[baseline-subtract] Shutting down (%lu processed)\n",
           (unsigned long)processCount);
    return 0;
}
