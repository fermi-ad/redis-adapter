//
//  Position-Intensity Adapter
//
//  Two outputs:
//    position[i]  = PositionScale * (A[i] - B[i]) / (A[i] + B[i])   (div-by-zero → 0.0)
//    intensity[i] = IntensityScale * (A[i] + B[i])
//
//  Trigger: InputKeyA; InputKeyB cached
//  Usage: position-intensity <config.yml>
//

#include "waveform_utils.hpp"
#include <yaml-cpp/yaml.h>

#include <atomic>
#include <chrono>
#include <cmath>
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
    std::string outputKeyPosition;
    std::string outputKeyIntensity;
    DataType    dataType = DataType::Float32;
    double      positionScale  = 1.0;
    double      intensityScale = 1.0;
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

    cfg.deviceName          = dev["DeviceName"].as<std::string>("DEVICE:0001");
    cfg.redisHost           = dev["RedisHost"].as<std::string>("127.0.0.1");
    cfg.inputKeyA           = dev["InputKeyA"].as<std::string>("");
    cfg.inputKeyB           = dev["InputKeyB"].as<std::string>("");
    cfg.outputKeyPosition   = dev["OutputKeyPosition"].as<std::string>("");
    cfg.outputKeyIntensity  = dev["OutputKeyIntensity"].as<std::string>("");
    cfg.dataType            = parseDataType(dev["DataType"].as<std::string>("float32"));
    cfg.positionScale       = dev["PositionScale"].as<double>(1.0);
    cfg.intensityScale      = dev["IntensityScale"].as<double>(1.0);

    if (cfg.inputKeyA.empty() || cfg.inputKeyB.empty() ||
        cfg.outputKeyPosition.empty() || cfg.outputKeyIntensity.empty())
    {
        fprintf(stderr, "Config requires InputKeyA, InputKeyB, OutputKeyPosition, and OutputKeyIntensity\n");
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

    printf("[position-intensity] Device: %s  Redis: %s  DataType: %s\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str(), dataTypeName(cfg.dataType));
    printf("[position-intensity] A: %s  B: %s\n",
           cfg.inputKeyA.c_str(), cfg.inputKeyB.c_str());
    printf("[position-intensity] Position: %s (scale %.4g)  Intensity: %s (scale %.4g)\n",
           cfg.outputKeyPosition.c_str(), cfg.positionScale,
           cfg.outputKeyIntensity.c_str(), cfg.intensityScale);

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = cfg.deviceName + ":POSITION_INTENSITY";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[position-intensity] Failed to connect to Redis\n");
        return 1;
    }

    std::mutex bMtx;
    std::vector<double> cachedB;
    uint64_t processCount = 0;

    redis.setDeferReaders(true);

    redis.addReader(cfg.inputKeyB,
        [&bMtx, &cachedB, &cfg]
        (const std::string&, const std::string&, const TimeAttrsList& data)
    {
        for (auto& [time, attrs] : data)
        {
            auto blob = ral_to_blob(attrs);
            if (!blob) continue;
            auto wf = deserializeWaveform(*blob, cfg.dataType);
            std::lock_guard<std::mutex> lk(bMtx);
            cachedB = std::move(wf);
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
            { std::lock_guard<std::mutex> lk(bMtx); b = cachedB; }

            size_t n = a.size();
            std::vector<double> position(n);
            std::vector<double> intensity(n);
            for (size_t i = 0; i < n; ++i)
            {
                double av = a[i];
                double bv = (i < b.size()) ? b[i] : 0.0;
                double sum = av + bv;
                position[i]  = (sum != 0.0) ? cfg.positionScale * (av - bv) / sum : 0.0;
                intensity[i] = cfg.intensityScale * sum;
            }

            serializeAndWrite(redis, cfg.outputKeyPosition, position, cfg.dataType, time);
            serializeAndWrite(redis, cfg.outputKeyIntensity, intensity, cfg.dataType, time);

            if (++processCount % 200 == 0)
                printf("[position-intensity] processed %lu samples\n",
                       (unsigned long)processCount);
        }
    });

    redis.setDeferReaders(false);

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);
    printf("[position-intensity] Running, waiting for data...\n");

    while (g_running)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    printf("[position-intensity] Shutting down (%lu processed)\n",
           (unsigned long)processCount);
    return 0;
}
