//
//  Demux Adapter
//
//  Takes a single interleaved input waveform and splits it into N output
//  channels.  Samples are de-interleaved in the order the channels are
//  listed: for N channels, sample[0] → ch0, sample[1] → ch1, …,
//  sample[N] → ch0, sample[N+1] → ch1, etc.
//
//  Input and output data types are independently configurable so you can
//  e.g. read int32 ADC data and emit float32 waveforms.
//
//  Trigger: InputKey
//  Usage: demux <config.yml>
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
    std::string              deviceName;
    std::string              redisHost = "127.0.0.1";
    std::string              inputKey;
    DataType                 dataTypeIn  = DataType::Float32;
    DataType                 dataTypeOut = DataType::Float32;
    std::vector<std::string> channels;       // output keys in interleave order
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
    cfg.inputKey   = dev["InputKey"].as<std::string>("");
    cfg.dataTypeIn = parseDataType(dev["DataTypeIn"].as<std::string>("float32"));
    cfg.dataTypeOut = parseDataType(dev["DataTypeOut"].as<std::string>("float32"));

    auto chNode = dev["Channels"];
    if (chNode && chNode.IsSequence())
    {
        for (auto c : chNode)
            cfg.channels.push_back(c.as<std::string>());
    }

    if (cfg.inputKey.empty())
    {
        fprintf(stderr, "Config requires InputKey\n");
        return false;
    }
    if (cfg.channels.size() < 2)
    {
        fprintf(stderr, "Config requires at least 2 entries in Channels\n");
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

    size_t nCh = cfg.channels.size();

    printf("[demux] Device: %s  Redis: %s  In: %s  Out: %s  Channels: %zu\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str(),
           dataTypeName(cfg.dataTypeIn), dataTypeName(cfg.dataTypeOut), nCh);
    printf("[demux] Input: %s\n", cfg.inputKey.c_str());
    for (size_t i = 0; i < nCh; ++i)
        printf("[demux]   [%zu] %s\n", i, cfg.channels[i].c_str());

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = cfg.deviceName + ":DEMUX";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[demux] Failed to connect to Redis\n");
        return 1;
    }

    uint64_t processCount = 0;

    redis.addReader(cfg.inputKey,
        [&](const std::string&, const std::string&, const TimeAttrsList& data)
    {
        for (auto& [time, attrs] : data)
        {
            auto blob = ral_to_blob(attrs);
            if (!blob) continue;
            auto samples = deserializeWaveform(*blob, cfg.dataTypeIn);

            size_t totalSamples = samples.size();
            size_t perChannel   = totalSamples / nCh;

            std::vector<std::vector<double>> out(nCh);
            for (size_t ch = 0; ch < nCh; ++ch)
                out[ch].reserve(perChannel);

            for (size_t i = 0; i < perChannel * nCh; ++i)
                out[i % nCh].push_back(samples[i]);

            for (size_t ch = 0; ch < nCh; ++ch)
                serializeAndWrite(redis, cfg.channels[ch], out[ch], cfg.dataTypeOut, time);

            if (++processCount % 200 == 0)
                printf("[demux] processed %lu samples\n",
                       (unsigned long)processCount);
        }
    });

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);
    printf("[demux] Running, waiting for data...\n");

    while (g_running)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    printf("[demux] Shutting down (%lu processed)\n",
           (unsigned long)processCount);
    return 0;
}
