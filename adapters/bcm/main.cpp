//
//  BCM (Beam Current Monitor) Composite Adapter
//
//  Single-process pipeline that takes a raw signal waveform and a
//  background waveform (4 channels each, interleaved) and produces:
//
//    RAW_BCM_WF ──► demux (4 channels)
//    BKG_BCM_WF ──► demux (4 channels)
//                     │
//               background subtraction ──► SUB_CHn
//                     │
//          ┌──────────┼──────────────────┐
//          │          │                  │
//    gated integration    ungated integration    filter (EMA)
//    (gate window only)   (full waveform)
//    INTEG_GATED_CHn      INTEG_UNGATED_CHn      FILTERED_CHn
//          │                    │
//    1-min / 5-min avg    1-min / 5-min avg
//
//  Gated integration: sums only within [GateStart, GateStart+GateWidth]
//  Ungated integration: sums the entire waveform
//  Both operate on background-subtracted data.
//
//  Usage: bcm <config.yml>
//

#include "waveform_utils.hpp"
#include "metadata.hpp"
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

struct AvgState
{
    double   sum   = 0.0;
    uint64_t count = 0;
    std::chrono::steady_clock::time_point start;
};

struct ChannelOpts
{
    std::string subKey;         // background-subtracted waveform
    std::string filterKey;      // EMA-filtered (empty = disabled)
    double      filterCoeff = 0.1;

    std::string gatedKey;       // gated integration scalar
    std::string gatedAvg1mKey;
    std::string gatedAvg5mKey;

    std::string ungatedKey;     // ungated integration scalar
    std::string ungatedAvg1mKey;
    std::string ungatedAvg5mKey;

    // runtime
    std::vector<double> filterState;
    AvgState gatedAvg1m, gatedAvg5m;
    AvgState ungatedAvg1m, ungatedAvg5m;
};

struct Config
{
    std::string deviceName;
    std::string redisHost = "127.0.0.1";

    std::string inputKeyRaw;
    std::string inputKeyBkg;
    DataType    dataTypeIn  = DataType::Float32;
    DataType    dataTypeOut = DataType::Float32;
    size_t      numChannels = 4;

    double      gateStart = 0.2;
    double      gateWidth = 0.4;

    std::vector<ChannelOpts> channelOpts;
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

    cfg.deviceName  = dev["DeviceName"].as<std::string>("BCM:0001");
    cfg.redisHost   = dev["RedisHost"].as<std::string>("127.0.0.1");
    cfg.inputKeyRaw = dev["InputKeyRaw"].as<std::string>("");
    cfg.inputKeyBkg = dev["InputKeyBkg"].as<std::string>("");
    cfg.dataTypeIn  = parseDataType(dev["DataTypeIn"].as<std::string>("float32"));
    cfg.dataTypeOut = parseDataType(dev["DataTypeOut"].as<std::string>("float32"));
    cfg.numChannels = dev["NumChannels"].as<size_t>(4);
    cfg.gateStart   = dev["GateStart"].as<double>(0.2);
    cfg.gateWidth   = dev["GateWidth"].as<double>(0.4);

    if (cfg.inputKeyRaw.empty() || cfg.inputKeyBkg.empty() || cfg.numChannels == 0)
    {
        fprintf(stderr, "Config requires InputKeyRaw, InputKeyBkg, and NumChannels > 0\n");
        return false;
    }

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
        co.subKey         = cn["SubKey"].as<std::string>("");
        co.filterKey      = cn["FilterKey"].as<std::string>("");
        co.filterCoeff    = cn["FilterCoeff"].as<double>(0.1);
        co.gatedKey       = cn["GatedKey"].as<std::string>("");
        co.gatedAvg1mKey  = cn["GatedAvg1mKey"].as<std::string>("");
        co.gatedAvg5mKey  = cn["GatedAvg5mKey"].as<std::string>("");
        co.ungatedKey     = cn["UngatedKey"].as<std::string>("");
        co.ungatedAvg1mKey = cn["UngatedAvg1mKey"].as<std::string>("");
        co.ungatedAvg5mKey = cn["UngatedAvg5mKey"].as<std::string>("");

        if (co.subKey.empty())
        {
            fprintf(stderr, "Channel %zu requires SubKey\n", i);
            return false;
        }
    }

    return true;
}

// ---- Helper: accumulate and emit average ----

static void updateAvg(RedisAdapterLite& redis, AvgState& avg,
                       const std::string& key, double value,
                       double periodSeconds,
                       std::chrono::steady_clock::time_point tNow)
{
    if (key.empty()) return;
    avg.sum += value;
    avg.count++;
    double elapsed = std::chrono::duration<double>(tNow - avg.start).count();
    if (elapsed >= periodSeconds)
    {
        redis.addDouble(key, avg.sum / static_cast<double>(avg.count));
        avg.sum   = 0.0;
        avg.count = 0;
        avg.start = tNow;
    }
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

    printf("[bcm] Device: %s  Redis: %s  Channels: %zu\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str(), nCh);
    printf("[bcm] Raw: %s  Bkg: %s (%s → %s)\n",
           cfg.inputKeyRaw.c_str(), cfg.inputKeyBkg.c_str(),
           dataTypeName(cfg.dataTypeIn), dataTypeName(cfg.dataTypeOut));
    printf("[bcm] Gate: start=%.3f  width=%.3f\n", cfg.gateStart, cfg.gateWidth);
    for (size_t i = 0; i < nCh; ++i)
    {
        printf("[bcm]   CH%zu: sub=%s", i, cfg.channelOpts[i].subKey.c_str());
        if (!cfg.channelOpts[i].filterKey.empty())
            printf("  filter=%s(%.3f)", cfg.channelOpts[i].filterKey.c_str(),
                   cfg.channelOpts[i].filterCoeff);
        if (!cfg.channelOpts[i].gatedKey.empty())
            printf("  gated=%s", cfg.channelOpts[i].gatedKey.c_str());
        if (!cfg.channelOpts[i].ungatedKey.empty())
            printf("  ungated=%s", cfg.channelOpts[i].ungatedKey.c_str());
        printf("\n");
    }

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 2;      // two input streams
    opts.dogname = cfg.deviceName + ":BCM";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[bcm] Failed to connect to Redis\n");
        return 1;
    }

    // init averaging clocks
    auto tNow = std::chrono::steady_clock::now();
    for (auto& co : cfg.channelOpts)
    {
        co.gatedAvg1m.start   = tNow;
        co.gatedAvg5m.start   = tNow;
        co.ungatedAvg1m.start = tNow;
        co.ungatedAvg5m.start = tNow;
    }

    // ---- Publish metadata ----
    {
        std::vector<ChannelMetaEntry> metaCh;
        for (size_t i = 0; i < nCh; ++i)
        {
            metaCh.push_back({cfg.channelOpts[i].subKey, "waveform",
                              "Ch" + std::to_string(i) + " Subtracted", ""});
            if (!cfg.channelOpts[i].filterKey.empty())
                metaCh.push_back({cfg.channelOpts[i].filterKey, "waveform",
                                  "Ch" + std::to_string(i) + " Filtered", ""});
            if (!cfg.channelOpts[i].gatedKey.empty())
                metaCh.push_back({cfg.channelOpts[i].gatedKey, "scalar",
                                  "Ch" + std::to_string(i) + " Gated", ""});
            if (!cfg.channelOpts[i].gatedAvg1mKey.empty())
                metaCh.push_back({cfg.channelOpts[i].gatedAvg1mKey, "scalar",
                                  "Ch" + std::to_string(i) + " Gated 1m Avg", ""});
            if (!cfg.channelOpts[i].gatedAvg5mKey.empty())
                metaCh.push_back({cfg.channelOpts[i].gatedAvg5mKey, "scalar",
                                  "Ch" + std::to_string(i) + " Gated 5m Avg", ""});
            if (!cfg.channelOpts[i].ungatedKey.empty())
                metaCh.push_back({cfg.channelOpts[i].ungatedKey, "scalar",
                                  "Ch" + std::to_string(i) + " Ungated", ""});
            if (!cfg.channelOpts[i].ungatedAvg1mKey.empty())
                metaCh.push_back({cfg.channelOpts[i].ungatedAvg1mKey, "scalar",
                                  "Ch" + std::to_string(i) + " Ungated 1m Avg", ""});
            if (!cfg.channelOpts[i].ungatedAvg5mKey.empty())
                metaCh.push_back({cfg.channelOpts[i].ungatedAvg5mKey, "scalar",
                                  "Ch" + std::to_string(i) + " Ungated 5m Avg", ""});
        }

        std::vector<ControlMetaEntry> ctrls = {
            {"BEAM_CURRENT_S", "Beam Current (mA)", 100.0},
            {"GATE_ENABLE_S",  "Gate Enable",       1.0},
        };
        for (size_t i = 0; i < nCh; ++i)
            ctrls.push_back({"CH" + std::to_string(i) + "_GAIN_S",
                             "Ch" + std::to_string(i) + " Gain", 1.0});

        publishMetadata(redis, "bcm", cfg.deviceName,
                        dataTypeName(cfg.dataTypeOut), metaCh, ctrls);
    }

    uint64_t processCount = 0;

    // Latest background waveform (updated asynchronously)
    std::mutex bkgMtx;
    std::vector<std::vector<double>> bkgCh(nCh);

    // Background reader: demux and store latest
    redis.addReader(cfg.inputKeyBkg,
        [&](const std::string&, const std::string&, const TimeAttrsList& data)
    {
        for (auto& [time, attrs] : data)
        {
            auto blob = ral_to_blob(attrs);
            if (!blob) continue;
            auto samples = deserializeWaveform(*blob, cfg.dataTypeIn);

            size_t perChannel = samples.size() / nCh;
            if (perChannel == 0) continue;

            std::vector<std::vector<double>> tmp(nCh);
            for (size_t c = 0; c < nCh; ++c)
                tmp[c].resize(perChannel);
            for (size_t s = 0; s < perChannel; ++s)
            {
                size_t base = s * nCh;
                for (size_t c = 0; c < nCh; ++c)
                    tmp[c][s] = samples[base + c];
            }

            std::lock_guard<std::mutex> lk(bkgMtx);
            bkgCh = std::move(tmp);
        }
    });

    // Raw signal reader: demux, subtract background, integrate
    redis.addReader(cfg.inputKeyRaw,
        [&](const std::string&, const std::string&, const TimeAttrsList& data)
    {
        for (auto& [time, attrs] : data)
        {
            auto blob = ral_to_blob(attrs);
            if (!blob) continue;
            auto samples = deserializeWaveform(*blob, cfg.dataTypeIn);

            size_t perChannel = samples.size() / nCh;
            if (perChannel == 0) continue;

            // ---- Stage 1: Demux raw ----
            std::vector<std::vector<double>> rawCh(nCh);
            for (size_t c = 0; c < nCh; ++c)
                rawCh[c].resize(perChannel);
            for (size_t s = 0; s < perChannel; ++s)
            {
                size_t base = s * nCh;
                for (size_t c = 0; c < nCh; ++c)
                    rawCh[c][s] = samples[base + c];
            }

            // Snapshot current background
            std::vector<std::vector<double>> bkg;
            {
                std::lock_guard<std::mutex> lk(bkgMtx);
                bkg = bkgCh;
            }

            // ---- Stage 2: Background subtraction ----
            std::vector<std::vector<double>> sub(nCh);
            for (size_t c = 0; c < nCh; ++c)
            {
                sub[c].resize(perChannel);
                bool hasBkg = (c < bkg.size() && bkg[c].size() == perChannel);
                for (size_t i = 0; i < perChannel; ++i)
                    sub[c][i] = rawCh[c][i] - (hasBkg ? bkg[c][i] : 0.0);

                serializeAndWrite(redis, cfg.channelOpts[c].subKey,
                                  sub[c], cfg.dataTypeOut, time);
            }

            // ---- Stage 3: Per-channel filter (EMA) ----
            for (size_t c = 0; c < nCh; ++c)
            {
                auto& co = cfg.channelOpts[c];
                if (co.filterKey.empty()) continue;

                if (co.filterState.size() != perChannel)
                    co.filterState.assign(perChannel, 0.0);
                double a = co.filterCoeff;
                for (size_t i = 0; i < perChannel; ++i)
                    co.filterState[i] = a * sub[c][i] + (1.0 - a) * co.filterState[i];
                serializeAndWrite(redis, co.filterKey, co.filterState,
                                  cfg.dataTypeOut, time);
            }

            // ---- Stage 4: Gated and ungated integration ----
            size_t gStart = static_cast<size_t>(cfg.gateStart * perChannel);
            size_t gEnd   = static_cast<size_t>((cfg.gateStart + cfg.gateWidth) * perChannel);
            if (gStart >= perChannel) gStart = perChannel - 1;
            if (gEnd > perChannel)    gEnd = perChannel;

            auto tNow = std::chrono::steady_clock::now();

            for (size_t c = 0; c < nCh; ++c)
            {
                auto& co = cfg.channelOpts[c];

                // Gated integration (gate window only)
                if (!co.gatedKey.empty() ||
                    !co.gatedAvg1mKey.empty() || !co.gatedAvg5mKey.empty())
                {
                    double gatedSum = 0.0;
                    for (size_t i = gStart; i < gEnd; ++i)
                        gatedSum += sub[c][i];

                    if (!co.gatedKey.empty())
                    {
                        RAL_AddArgs tArgs;
                        tArgs.time = time;
                        redis.addDouble(co.gatedKey, gatedSum, tArgs);
                    }

                    updateAvg(redis, co.gatedAvg1m, co.gatedAvg1mKey,
                              gatedSum, 60.0, tNow);
                    updateAvg(redis, co.gatedAvg5m, co.gatedAvg5mKey,
                              gatedSum, 300.0, tNow);
                }

                // Ungated integration (full waveform)
                if (!co.ungatedKey.empty() ||
                    !co.ungatedAvg1mKey.empty() || !co.ungatedAvg5mKey.empty())
                {
                    double ungatedSum = 0.0;
                    for (size_t i = 0; i < perChannel; ++i)
                        ungatedSum += sub[c][i];

                    if (!co.ungatedKey.empty())
                    {
                        RAL_AddArgs tArgs;
                        tArgs.time = time;
                        redis.addDouble(co.ungatedKey, ungatedSum, tArgs);
                    }

                    updateAvg(redis, co.ungatedAvg1m, co.ungatedAvg1mKey,
                              ungatedSum, 60.0, tNow);
                    updateAvg(redis, co.ungatedAvg5m, co.ungatedAvg5mKey,
                              ungatedSum, 300.0, tNow);
                }
            }

            if (++processCount % 200 == 0)
                printf("[bcm] processed %lu triggers\n",
                       (unsigned long)processCount);
        }
    });

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);
    printf("[bcm] Running, waiting for data...\n");

    while (g_running)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    printf("[bcm] Shutting down (%lu processed)\n",
           (unsigned long)processCount);
    return 0;
}
