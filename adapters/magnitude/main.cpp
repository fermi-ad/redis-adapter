//
//  Magnitude Adapter
//
//  Two outputs:
//    magnitude[i] = sqrt(I[i]^2 + Q[i]^2)
//    phase[i]     = atan2(Q[i], I[i])
//
//  Trigger: InputKeyI; InputKeyQ cached
//  Usage: magnitude <config.yml>
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
    std::string inputKeyI;
    std::string inputKeyQ;
    std::string outputKeyMag;
    std::string outputKeyPhase;
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

    cfg.deviceName     = dev["DeviceName"].as<std::string>("DEVICE:0001");
    cfg.redisHost      = dev["RedisHost"].as<std::string>("127.0.0.1");
    cfg.inputKeyI      = dev["InputKeyI"].as<std::string>("");
    cfg.inputKeyQ      = dev["InputKeyQ"].as<std::string>("");
    cfg.outputKeyMag   = dev["OutputKeyMag"].as<std::string>("");
    cfg.outputKeyPhase = dev["OutputKeyPhase"].as<std::string>("");
    cfg.dataType       = parseDataType(dev["DataType"].as<std::string>("float32"));

    if (cfg.inputKeyI.empty() || cfg.inputKeyQ.empty() ||
        cfg.outputKeyMag.empty() || cfg.outputKeyPhase.empty())
    {
        fprintf(stderr, "Config requires InputKeyI, InputKeyQ, OutputKeyMag, and OutputKeyPhase\n");
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

    printf("[magnitude] Device: %s  Redis: %s  DataType: %s\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str(), dataTypeName(cfg.dataType));
    printf("[magnitude] I: %s  Q: %s\n",
           cfg.inputKeyI.c_str(), cfg.inputKeyQ.c_str());
    printf("[magnitude] Mag: %s  Phase: %s\n",
           cfg.outputKeyMag.c_str(), cfg.outputKeyPhase.c_str());

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = cfg.deviceName + ":MAGNITUDE";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[magnitude] Failed to connect to Redis\n");
        return 1;
    }

    std::mutex qMtx;
    std::vector<double> cachedQ;
    uint64_t processCount = 0;

    redis.setDeferReaders(true);

    redis.addReader(cfg.inputKeyQ,
        [&qMtx, &cachedQ, &cfg]
        (const std::string&, const std::string&, const TimeAttrsList& data)
    {
        for (auto& [time, attrs] : data)
        {
            auto blob = ral_to_blob(attrs);
            if (!blob) continue;
            auto wf = deserializeWaveform(*blob, cfg.dataType);
            std::lock_guard<std::mutex> lk(qMtx);
            cachedQ = std::move(wf);
        }
    });

    redis.addReader(cfg.inputKeyI,
        [&](const std::string&, const std::string&, const TimeAttrsList& data)
    {
        for (auto& [time, attrs] : data)
        {
            auto blob = ral_to_blob(attrs);
            if (!blob) continue;
            auto iVec = deserializeWaveform(*blob, cfg.dataType);

            std::vector<double> q;
            { std::lock_guard<std::mutex> lk(qMtx); q = cachedQ; }

            size_t n = iVec.size();
            std::vector<double> mag(n);
            std::vector<double> phase(n);
            for (size_t i = 0; i < n; ++i)
            {
                double iv = iVec[i];
                double qv = (i < q.size()) ? q[i] : 0.0;
                mag[i]   = std::sqrt(iv * iv + qv * qv);
                phase[i] = std::atan2(qv, iv);
            }

            serializeAndWrite(redis, cfg.outputKeyMag, mag, cfg.dataType, time);
            serializeAndWrite(redis, cfg.outputKeyPhase, phase, cfg.dataType, time);

            if (++processCount % 200 == 0)
                printf("[magnitude] processed %lu samples\n",
                       (unsigned long)processCount);
        }
    });

    redis.setDeferReaders(false);

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);
    printf("[magnitude] Running, waiting for data...\n");

    while (g_running)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    printf("[magnitude] Shutting down (%lu processed)\n",
           (unsigned long)processCount);
    return 0;
}
