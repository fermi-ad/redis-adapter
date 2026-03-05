//
//  Integrate Adapter
//
//  Baseline-subtract A-B, then sum samples within each configured window.
//  Each window produces a scalar double (always addDouble).
//  Multiple windows allow integration over different regions.
//
//  Each window can optionally produce a time-averaged output: when
//  AvgKey and AvgSeconds are set, the adapter accumulates window results
//  and writes the running average every AvgSeconds.
//
//  Trigger: InputKeyA; InputKeyB cached
//  Usage: integrate <config.yml>
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

struct IntegWindow
{
    std::string outputKey;
    double      start = 0.0;
    double      width = 1.0;

    // averaging
    std::string avgKey;            // empty = no averaging
    double      avgSeconds = 300;  // default 5 minutes

    // runtime state
    double   avgSum   = 0.0;
    uint64_t avgCount = 0;
    std::chrono::steady_clock::time_point avgStart;
};

struct Config
{
    std::string deviceName;
    std::string redisHost = "127.0.0.1";
    std::string inputKeyA;
    std::string inputKeyB;
    DataType    dataType = DataType::Float32;
    std::vector<IntegWindow> windows;
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
    cfg.dataType   = parseDataType(dev["DataType"].as<std::string>("float32"));

    if (cfg.inputKeyA.empty())
    {
        fprintf(stderr, "Config requires InputKeyA\n");
        return false;
    }

    auto windowsNode = dev["Windows"];
    if (windowsNode && windowsNode.IsSequence())
    {
        for (auto wn : windowsNode)
        {
            IntegWindow w;
            w.outputKey  = wn["OutputKey"].as<std::string>("");
            w.start      = wn["Start"].as<double>(0.0);
            w.width      = wn["Width"].as<double>(1.0);
            w.avgKey     = wn["AvgKey"].as<std::string>("");
            w.avgSeconds = wn["AvgSeconds"].as<double>(300.0);
            if (w.outputKey.empty())
            {
                fprintf(stderr, "Each window requires an OutputKey\n");
                return false;
            }
            cfg.windows.push_back(std::move(w));
        }
    }

    if (cfg.windows.empty())
    {
        fprintf(stderr, "Config requires at least one window in Windows list\n");
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

    printf("[integrate] Device: %s  Redis: %s  DataType: %s\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str(), dataTypeName(cfg.dataType));
    printf("[integrate] A: %s  B: %s  Windows: %zu\n",
           cfg.inputKeyA.c_str(), cfg.inputKeyB.c_str(), cfg.windows.size());
    for (size_t w = 0; w < cfg.windows.size(); ++w)
    {
        printf("[integrate]   [%zu] %s  [%.2f, %.2f]", w,
               cfg.windows[w].outputKey.c_str(),
               cfg.windows[w].start,
               cfg.windows[w].start + cfg.windows[w].width);
        if (!cfg.windows[w].avgKey.empty())
            printf("  avg -> %s (%.0fs)",
                   cfg.windows[w].avgKey.c_str(), cfg.windows[w].avgSeconds);
        printf("\n");
    }

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = cfg.deviceName + ":INTEGRATE";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[integrate] Failed to connect to Redis\n");
        return 1;
    }

    // initialize averaging clocks
    auto now = std::chrono::steady_clock::now();
    for (auto& win : cfg.windows)
        win.avgStart = now;

    std::mutex baselineMtx;
    std::vector<double> cachedBaseline;
    uint64_t processCount = 0;

    redis.setDeferReaders(true);

    if (!cfg.inputKeyB.empty())
    {
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
    }

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
            if (n == 0) continue;

            auto tNow = std::chrono::steady_clock::now();

            for (auto& win : cfg.windows)
            {
                size_t iStart = static_cast<size_t>(win.start * n);
                size_t iEnd   = static_cast<size_t>((win.start + win.width) * n);
                if (iStart >= n) iStart = n - 1;
                if (iEnd > n)    iEnd = n;

                double sum = 0.0;
                for (size_t i = iStart; i < iEnd; ++i)
                    sum += a[i] - (i < b.size() ? b[i] : 0.0);

                RAL_AddArgs tArgs;
                tArgs.time = time;
                redis.addDouble(win.outputKey, sum, tArgs);

                // accumulate for averaging
                if (!win.avgKey.empty())
                {
                    win.avgSum += sum;
                    win.avgCount++;

                    auto elapsed = std::chrono::duration<double>(
                        tNow - win.avgStart).count();
                    if (elapsed >= win.avgSeconds)
                    {
                        double avg = win.avgSum /
                            static_cast<double>(win.avgCount);
                        redis.addDouble(win.avgKey, avg);
                        win.avgSum   = 0.0;
                        win.avgCount = 0;
                        win.avgStart = tNow;
                    }
                }
            }

            if (++processCount % 200 == 0)
                printf("[integrate] processed %lu samples\n",
                       (unsigned long)processCount);
        }
    });

    redis.setDeferReaders(false);

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);
    printf("[integrate] Running, waiting for data...\n");

    while (g_running)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    printf("[integrate] Shutting down (%lu processed)\n",
           (unsigned long)processCount);
    return 0;
}
