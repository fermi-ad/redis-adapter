//
//  Filter Adapter
//
//  Exponential moving average: state[i] = alpha*A[i] + (1-alpha)*state[i]
//
//  Trigger: InputKeyA only (no B input)
//  Usage: filter <config.yml>
//

#include "waveform_utils.hpp"
#include <yaml-cpp/yaml.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
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
    std::string outputKey;
    DataType    dataType = DataType::Float32;
    double      filterCoeff = 0.1;
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

    cfg.deviceName  = dev["DeviceName"].as<std::string>("DEVICE:0001");
    cfg.redisHost   = dev["RedisHost"].as<std::string>("127.0.0.1");
    cfg.inputKeyA   = dev["InputKeyA"].as<std::string>("");
    cfg.outputKey   = dev["OutputKey"].as<std::string>("");
    cfg.dataType    = parseDataType(dev["DataType"].as<std::string>("float32"));
    cfg.filterCoeff = dev["FilterCoeff"].as<double>(0.1);

    if (cfg.inputKeyA.empty() || cfg.outputKey.empty())
    {
        fprintf(stderr, "Config requires InputKeyA and OutputKey\n");
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

    printf("[filter] Device: %s  Redis: %s  DataType: %s  Alpha: %.3f\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str(),
           dataTypeName(cfg.dataType), cfg.filterCoeff);
    printf("[filter] A: %s  Out: %s\n",
           cfg.inputKeyA.c_str(), cfg.outputKey.c_str());

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = cfg.deviceName + ":FILTER";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[filter] Failed to connect to Redis\n");
        return 1;
    }

    std::vector<double> filterState;
    uint64_t processCount = 0;

    redis.addReader(cfg.inputKeyA,
        [&](const std::string&, const std::string&, const TimeAttrsList& data)
    {
        for (auto& [time, attrs] : data)
        {
            auto blob = ral_to_blob(attrs);
            if (!blob) continue;
            auto a = deserializeWaveform(*blob, cfg.dataType);

            size_t n = a.size();
            if (filterState.size() != n)
                filterState.assign(n, 0.0);

            double alpha = cfg.filterCoeff;
            for (size_t i = 0; i < n; ++i)
                filterState[i] = alpha * a[i] + (1.0 - alpha) * filterState[i];

            serializeAndWrite(redis, cfg.outputKey, filterState, cfg.dataType, time);

            if (++processCount % 200 == 0)
                printf("[filter] processed %lu samples\n",
                       (unsigned long)processCount);
        }
    });

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);
    printf("[filter] Running, waiting for data...\n");

    while (g_running)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    printf("[filter] Shutting down (%lu processed)\n",
           (unsigned long)processCount);
    return 0;
}
