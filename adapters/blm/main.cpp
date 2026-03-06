//
//  BLM (Beam Loss Monitor) Composite Adapter
//
//  Single-process pipeline that takes one interleaved 8-channel waveform
//  and produces all derived quantities:
//
//    RAW_BLM_WF ──► demux (8 channels)
//                     │
//               per-channel raw waveforms ──► RAW_CHn
//                     │
//          ┌──────────┼──────────────┐
//          │          │              │
//    10ms integration    50ms integration    filter (EMA, optional)
//    (binned waveform)   (total scalar)
//    INTEG_10MS_CHn      INTEG_50MS_CHn      FILTERED_CHn
//          │                   │
//    running average     running average
//    INTEG_10MS_AVG_CHn  INTEG_50MS_AVG_CHn
//
//  Sample rate: 1 MHz (1 sample per microsecond)
//    10ms integration = 10000 samples per bin
//    50ms integration = 50000 samples total
//
//  Everything runs in-process inside the XREAD callback.
//
//  Usage: blm <config.yml>
//

#include "waveform_utils.hpp"
#include <yaml-cpp/yaml.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

static std::atomic<bool> g_running{true};
static void sigHandler(int) { g_running = false; }

// ---- Config structures ----

struct IntegWindow
{
    std::string name;           // for logging
    int         binSamples;     // samples per integration bin (e.g., 10000 for 10ms)
    // per-channel output keys
    std::vector<std::string> outputKeys;    // binned waveform (or scalar if 1 bin)
    std::vector<std::string> avgKeys;       // running average keys
    double      avgSeconds = 300.0;

    // per-channel runtime state
    std::vector<double>   avgSum;
    std::vector<uint64_t> avgCount;
    std::chrono::steady_clock::time_point avgStart;
};

struct ChannelOpts
{
    std::string rawKey;         // per-channel raw waveform output
    std::string filterKey;      // EMA-filtered waveform (empty = disabled)
    double      filterCoeff = 0.1;

    // runtime
    std::vector<double> filterState;
};

struct Config
{
    std::string deviceName;
    std::string redisHost = "127.0.0.1";

    std::string inputKey;
    DataType    dataTypeIn  = DataType::Float32;
    DataType    dataTypeOut = DataType::Float32;
    size_t      numChannels = 8;

    std::vector<ChannelOpts> channelOpts;
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

    cfg.deviceName  = dev["DeviceName"].as<std::string>("BLM:0001");
    cfg.redisHost   = dev["RedisHost"].as<std::string>("127.0.0.1");
    cfg.inputKey    = dev["InputKey"].as<std::string>("");
    cfg.dataTypeIn  = parseDataType(dev["DataTypeIn"].as<std::string>("float32"));
    cfg.dataTypeOut = parseDataType(dev["DataTypeOut"].as<std::string>("float32"));
    cfg.numChannels = dev["NumChannels"].as<size_t>(8);

    if (cfg.inputKey.empty() || cfg.numChannels == 0)
    {
        fprintf(stderr, "Config requires InputKey and NumChannels > 0\n");
        return false;
    }

    // Per-channel options
    auto chNode = dev["Channels"];
    if (!chNode || !chNode.IsSequence() ||
        chNode.size() != cfg.numChannels)
    {
        fprintf(stderr, "Config requires Channels list with %zu entries\n",
                cfg.numChannels);
        return false;
    }

    cfg.channelOpts.resize(cfg.numChannels);
    for (size_t i = 0; i < cfg.numChannels; ++i)
    {
        auto cn = chNode[i];
        auto& co = cfg.channelOpts[i];
        co.rawKey      = cn["RawKey"].as<std::string>("");
        co.filterKey   = cn["FilterKey"].as<std::string>("");
        co.filterCoeff = cn["FilterCoeff"].as<double>(0.1);

        if (co.rawKey.empty())
        {
            fprintf(stderr, "Channel %zu requires RawKey\n", i);
            return false;
        }
    }

    // Integration windows
    auto integNode = dev["Integrate"];
    if (integNode && integNode.IsSequence())
    {
        for (auto wn : integNode)
        {
            IntegWindow w;
            w.name       = wn["Name"].as<std::string>("unnamed");
            w.binSamples = wn["BinSamples"].as<int>(10000);
            w.avgSeconds = wn["AvgSeconds"].as<double>(300.0);

            auto outNode = wn["OutputKeys"];
            auto avgNode = wn["AvgKeys"];

            if (!outNode || !outNode.IsSequence() ||
                outNode.size() != cfg.numChannels)
            {
                fprintf(stderr, "Integrate '%s' requires OutputKeys with %zu entries\n",
                        w.name.c_str(), cfg.numChannels);
                return false;
            }

            w.outputKeys.resize(cfg.numChannels);
            w.avgKeys.resize(cfg.numChannels);
            w.avgSum.resize(cfg.numChannels, 0.0);
            w.avgCount.resize(cfg.numChannels, 0);

            for (size_t i = 0; i < cfg.numChannels; ++i)
            {
                w.outputKeys[i] = outNode[i].as<std::string>("");
                if (avgNode && avgNode.IsSequence() && i < avgNode.size())
                    w.avgKeys[i] = avgNode[i].as<std::string>("");
            }

            cfg.windows.push_back(std::move(w));
        }
    }

    return true;
}

// ---- Main ----

int main(int argc, char* argv[])
{
    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <config.yml>\n", argv[0]);
        return 1;
    }

    Config cfg;
    if (!parseConfig(argv[1], cfg)) return 1;

    size_t nCh = cfg.numChannels;

    printf("[blm] Device: %s  Redis: %s  Channels: %zu\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str(), nCh);
    printf("[blm] Input: %s (%s → %s)\n",
           cfg.inputKey.c_str(), dataTypeName(cfg.dataTypeIn),
           dataTypeName(cfg.dataTypeOut));
    for (size_t i = 0; i < nCh; ++i)
    {
        printf("[blm]   CH%zu: raw=%s", i, cfg.channelOpts[i].rawKey.c_str());
        if (!cfg.channelOpts[i].filterKey.empty())
            printf("  filter=%s(%.3f)", cfg.channelOpts[i].filterKey.c_str(),
                   cfg.channelOpts[i].filterCoeff);
        printf("\n");
    }
    for (auto& w : cfg.windows)
        printf("[blm]   Integrate '%s': %d samples/bin, avg %.0fs\n",
               w.name.c_str(), w.binSamples, w.avgSeconds);

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = cfg.deviceName + ":BLM";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[blm] Failed to connect to Redis\n");
        return 1;
    }

    // init averaging clocks
    auto tNow = std::chrono::steady_clock::now();
    for (auto& w : cfg.windows)
        w.avgStart = tNow;

    uint64_t processCount = 0;

    redis.addReader(cfg.inputKey,
        [&](const std::string&, const std::string&, const TimeAttrsList& data)
    {
        for (auto& [time, attrs] : data)
        {
            auto blob = ral_to_blob(attrs);
            if (!blob) continue;
            auto samples = deserializeWaveform(*blob, cfg.dataTypeIn);

            size_t perChannel = samples.size() / nCh;
            if (perChannel == 0) continue;

            // ---- Stage 1: Demux ----
            std::vector<std::vector<double>> ch(nCh);
            for (size_t c = 0; c < nCh; ++c)
                ch[c].resize(perChannel);

            for (size_t s = 0; s < perChannel; ++s)
            {
                size_t base = s * nCh;
                for (size_t c = 0; c < nCh; ++c)
                    ch[c][s] = samples[base + c];
            }

            // ---- Stage 2: Per-channel raw waveform output ----
            for (size_t c = 0; c < nCh; ++c)
                serializeAndWrite(redis, cfg.channelOpts[c].rawKey,
                                  ch[c], cfg.dataTypeOut, time);

            // ---- Stage 3: Per-channel optional filter (EMA) ----
            for (size_t c = 0; c < nCh; ++c)
            {
                auto& co = cfg.channelOpts[c];
                if (co.filterKey.empty()) continue;

                if (co.filterState.size() != perChannel)
                    co.filterState.assign(perChannel, 0.0);
                double a = co.filterCoeff;
                for (size_t i = 0; i < perChannel; ++i)
                    co.filterState[i] = a * ch[c][i] + (1.0 - a) * co.filterState[i];
                serializeAndWrite(redis, co.filterKey, co.filterState,
                                  cfg.dataTypeOut, time);
            }

            // ---- Stage 4: Integration windows ----
            auto tNow = std::chrono::steady_clock::now();

            for (auto& win : cfg.windows)
            {
                size_t binSize = static_cast<size_t>(win.binSamples);
                if (binSize == 0) binSize = 1;
                size_t numBins = perChannel / binSize;
                bool isSingleBin = (numBins <= 1);

                for (size_t c = 0; c < nCh; ++c)
                {
                    if (win.outputKeys[c].empty() && win.avgKeys[c].empty())
                        continue;

                    if (isSingleBin)
                    {
                        // Total integration → scalar
                        double total = 0.0;
                        for (size_t i = 0; i < perChannel; ++i)
                            total += ch[c][i];

                        if (!win.outputKeys[c].empty())
                        {
                            RAL_AddArgs tArgs;
                            tArgs.time = time;
                            redis.addDouble(win.outputKeys[c], total, tArgs);
                        }

                        if (!win.avgKeys[c].empty())
                        {
                            win.avgSum[c] += total;
                            win.avgCount[c]++;
                        }
                    }
                    else
                    {
                        // Binned integration → waveform
                        std::vector<double> binned(numBins, 0.0);
                        for (size_t b = 0; b < numBins; ++b)
                        {
                            size_t start = b * binSize;
                            size_t end = start + binSize;
                            if (end > perChannel) end = perChannel;
                            for (size_t i = start; i < end; ++i)
                                binned[b] += ch[c][i];
                        }

                        if (!win.outputKeys[c].empty())
                            serializeAndWrite(redis, win.outputKeys[c], binned,
                                              cfg.dataTypeOut, time);

                        if (!win.avgKeys[c].empty())
                        {
                            double total = 0.0;
                            for (size_t b = 0; b < numBins; ++b)
                                total += binned[b];
                            win.avgSum[c] += total;
                            win.avgCount[c]++;
                        }
                    }
                }

                // Check if averaging period has elapsed
                double elapsed = std::chrono::duration<double>(
                    tNow - win.avgStart).count();
                if (elapsed >= win.avgSeconds)
                {
                    for (size_t c = 0; c < nCh; ++c)
                    {
                        if (!win.avgKeys[c].empty() && win.avgCount[c] > 0)
                        {
                            redis.addDouble(win.avgKeys[c],
                                win.avgSum[c] / static_cast<double>(win.avgCount[c]));
                        }
                        win.avgSum[c]   = 0.0;
                        win.avgCount[c] = 0;
                    }
                    win.avgStart = tNow;
                }
            }

            if (++processCount % 200 == 0)
                printf("[blm] processed %lu triggers\n",
                       (unsigned long)processCount);
        }
    });

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);
    printf("[blm] Running, waiting for data...\n");

    while (g_running)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    printf("[blm] Shutting down (%lu processed)\n",
           (unsigned long)processCount);
    return 0;
}
