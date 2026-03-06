//
//  BLM Digital Twin
//
//  Simulates an 8-channel Beam Loss Monitor digitizer.
//  Each channel represents an ionization chamber at a different
//  accelerator location, sampled at 1 MHz (1 sample per microsecond).
//
//  Output: RAW_BLM_WF — interleaved [CH0,CH1,...,CH7, CH0,CH1,...,CH7, ...]
//    float32:  8 × SamplesPerChannel floats  → OutputKey
//
//  Signal model per channel:
//    baseline + noise + loss_pulse(Gaussian shape)
//    Loss pulse amplitude is scaled per-channel to simulate non-uniform
//    beam loss distribution around the ring.
//
//  All parameters are device registers: readable via GetKey, settable via PutKey.
//
//  Usage: blm-twin <config.yml>
//

#include "RedisAdapterLite.hpp"
#include "RAL_Helpers.hpp"
#include <yaml-cpp/yaml.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

static std::atomic<bool> g_running{true};
static void sigHandler(int) { g_running = false; }

static constexpr int MAX_CHANNELS = 8;

// ---- Device register definitions ----

struct BLMParams
{
    int    updateIntervalMs  = 50;      // output rate (ms)
    int    samplesPerChannel = 50000;   // 50ms at 1 MHz
    int    numChannels       = 8;       // 1..8 channels per digitizer

    double baseline      = 0.001;       // DC baseline (all channels)
    double noise         = 0.0005;      // Gaussian noise amplitude

    // Simulated loss pulse (Gaussian shape in time)
    int    lossEnable    = 1;
    double lossAmplitude = 1.0;         // peak loss signal
    double lossPosition  = 0.5;         // fractional position in waveform
    double lossWidth     = 0.02;        // fractional sigma

    // Per-channel amplitude scaling (simulates non-uniform loss distribution)
    // Channels at different ring locations see different fractions of loss
    double channelScale[MAX_CHANNELS] = {1.0, 0.8, 0.6, 0.4, 0.3, 0.2, 0.15, 0.1};
};

enum class RegType { Double, Int };

struct Register
{
    const char* subKey;
    const char* putKey;     // nullptr = read-only
    RegType     type;
    size_t      offset;     // byte offset into BLMParams
    int         clampMin;
    int         clampMax;
};

#define POFF(field) offsetof(BLMParams, field)

static const Register g_registers[] = {
    {"UPDATE_INTERVAL",  "UPDATE_INTERVAL_S",  RegType::Int,    POFF(updateIntervalMs),  10, 10000},
    {"SAMPLES_PER_CH",   "SAMPLES_PER_CH_S",   RegType::Int,    POFF(samplesPerChannel), 1000, 100000},
    {"BASELINE",         "BASELINE_S",         RegType::Double, POFF(baseline),          0, 0},
    {"NOISE",            "NOISE_S",            RegType::Double, POFF(noise),             0, 0},
    {"LOSS_ENABLE",      "LOSS_ENABLE_S",      RegType::Int,    POFF(lossEnable),        0, 1},
    {"LOSS_AMPLITUDE",   "LOSS_AMPLITUDE_S",   RegType::Double, POFF(lossAmplitude),     0, 0},
    {"LOSS_POSITION",    "LOSS_POSITION_S",    RegType::Double, POFF(lossPosition),      0, 0},
    {"LOSS_WIDTH",       "LOSS_WIDTH_S",       RegType::Double, POFF(lossWidth),         0, 0},
    {"CH0_SCALE",        "CH0_SCALE_S",        RegType::Double, POFF(channelScale[0]),   0, 0},
    {"CH1_SCALE",        "CH1_SCALE_S",        RegType::Double, POFF(channelScale[1]),   0, 0},
    {"CH2_SCALE",        "CH2_SCALE_S",        RegType::Double, POFF(channelScale[2]),   0, 0},
    {"CH3_SCALE",        "CH3_SCALE_S",        RegType::Double, POFF(channelScale[3]),   0, 0},
    {"CH4_SCALE",        "CH4_SCALE_S",        RegType::Double, POFF(channelScale[4]),   0, 0},
    {"CH5_SCALE",        "CH5_SCALE_S",        RegType::Double, POFF(channelScale[5]),   0, 0},
    {"CH6_SCALE",        "CH6_SCALE_S",        RegType::Double, POFF(channelScale[6]),   0, 0},
    {"CH7_SCALE",        "CH7_SCALE_S",        RegType::Double, POFF(channelScale[7]),   0, 0},
};

static constexpr size_t NUM_REGISTERS = sizeof(g_registers) / sizeof(g_registers[0]);

// ---- Typed accessors into param block ----

static double getRegDouble(const BLMParams& p, size_t offset)
{
    double v;
    std::memcpy(&v, reinterpret_cast<const char*>(&p) + offset, sizeof(double));
    return v;
}

static int getRegInt(const BLMParams& p, size_t offset)
{
    int v;
    std::memcpy(&v, reinterpret_cast<const char*>(&p) + offset, sizeof(int));
    return v;
}

static void setRegDouble(BLMParams& p, size_t offset, double v)
{
    std::memcpy(reinterpret_cast<char*>(&p) + offset, &v, sizeof(double));
}

static void setRegInt(BLMParams& p, size_t offset, int v)
{
    std::memcpy(reinterpret_cast<char*>(&p) + offset, &v, sizeof(int));
}

// ---- Config ----

struct Config
{
    std::string deviceName;
    std::string redisHost = "127.0.0.1";
    std::string outputKey;
    BLMParams   defaults;
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

    cfg.deviceName = dev["DeviceName"].as<std::string>("BLM:0001");
    cfg.redisHost  = dev["RedisHost"].as<std::string>("127.0.0.1");
    cfg.outputKey  = dev["OutputKey"].as<std::string>("RAW_BLM_WF");

    auto& p = cfg.defaults;
    p.updateIntervalMs  = dev["UpdateIntervalMs"].as<int>(50);
    p.samplesPerChannel = dev["SamplesPerChannel"].as<int>(50000);
    p.baseline          = dev["Baseline"].as<double>(0.001);
    p.noise             = dev["Noise"].as<double>(0.0005);
    p.lossEnable        = dev["LossEnable"].as<int>(1);
    p.lossAmplitude     = dev["LossAmplitude"].as<double>(1.0);
    p.lossPosition      = dev["LossPosition"].as<double>(0.5);
    p.lossWidth         = dev["LossWidth"].as<double>(0.02);

    p.numChannels = dev["NumChannels"].as<int>(8);
    if (p.numChannels < 1) p.numChannels = 1;
    if (p.numChannels > MAX_CHANNELS) p.numChannels = MAX_CHANNELS;

    auto scaleNode = dev["ChannelScales"];
    if (scaleNode && scaleNode.IsSequence())
    {
        for (size_t i = 0; i < MAX_CHANNELS && i < scaleNode.size(); ++i)
            p.channelScale[i] = scaleNode[i].as<double>(1.0);
    }

    return true;
}

// ---- Publish a register's current value ----

static void publishRegister(RedisAdapterLite& redis, const Register& reg,
                             const BLMParams& params)
{
    if (reg.type == RegType::Double)
        redis.addDouble(reg.subKey, getRegDouble(params, reg.offset));
    else
        redis.addInt(reg.subKey, getRegInt(params, reg.offset));
}

// ---- Register all device registers ----

static void initRegisters(RedisAdapterLite& redis, std::mutex& mtx,
                           BLMParams& params)
{
    redis.setDeferReaders(true);

    for (size_t ri = 0; ri < NUM_REGISTERS; ++ri)
    {
        auto& reg = g_registers[ri];

        publishRegister(redis, reg, params);

        if (reg.putKey)
        {
            redis.addReader(reg.putKey,
                [&redis, &mtx, &params, ri]
                (const std::string&, const std::string&, const TimeAttrsList& data)
            {
                auto& reg = g_registers[ri];
                for (auto& [time, attrs] : data)
                {
                    std::lock_guard<std::mutex> lk(mtx);
                    if (reg.type == RegType::Double)
                    {
                        auto v = ral_to_double(attrs);
                        if (!v) continue;
                        setRegDouble(params, reg.offset, *v);
                        redis.addDouble(reg.subKey, *v);
                        printf("[blm-twin] SET %s = %f\n", reg.subKey, *v);
                    }
                    else
                    {
                        auto v = ral_to_int(attrs);
                        if (!v) continue;
                        int ival = static_cast<int>(*v);
                        if (reg.clampMin != reg.clampMax)
                            ival = std::max(reg.clampMin, std::min(reg.clampMax, ival));
                        setRegInt(params, reg.offset, ival);
                        redis.addInt(reg.subKey, ival);
                        printf("[blm-twin] SET %s = %d\n", reg.subKey, ival);
                    }
                }
            });
        }

        printf("[blm-twin]   REG %-20s = ", reg.subKey);
        if (reg.type == RegType::Double)
            printf("%.6f", getRegDouble(params, reg.offset));
        else
            printf("%d", getRegInt(params, reg.offset));
        if (reg.putKey)
            printf("  (set via %s)", reg.putKey);
        printf("\n");
    }

    redis.setDeferReaders(false);
}

// ---- Generate BLM waveform (8ch × N, float32, interleaved) ----

static void generateBLM(RedisAdapterLite& redis, const std::string& key,
                          const BLMParams& p, std::mt19937& rng)
{
    int N   = p.samplesPerChannel;
    int nCh = p.numChannels;
    std::normal_distribution<float> nd(0.0f, static_cast<float>(p.noise));

    std::vector<float> buf(N * nCh);

    for (int s = 0; s < N; ++s)
    {
        double t = static_cast<double>(s) / N;

        // Loss pulse: Gaussian centered at lossPosition with sigma = lossWidth
        double lossPulse = 0.0;
        if (p.lossEnable)
        {
            double dt = t - p.lossPosition;
            double sigma = p.lossWidth;
            if (sigma > 0.0)
                lossPulse = p.lossAmplitude * std::exp(-0.5 * dt * dt / (sigma * sigma));
        }

        int base = s * nCh;
        for (int ch = 0; ch < nCh; ++ch)
        {
            float signal = static_cast<float>(
                p.baseline + p.channelScale[ch] * lossPulse) + nd(rng);
            buf[base + ch] = signal;
        }
    }

    redis.addBlob(key, buf.data(), buf.size() * sizeof(float));
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

    printf("[blm-twin] Device: %s  Redis: %s\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str());
    printf("[blm-twin] Output: %s  %d samples/ch (%d ch)  %.0f us waveform\n",
           cfg.outputKey.c_str(), cfg.defaults.samplesPerChannel,
           cfg.defaults.numChannels,
           static_cast<double>(cfg.defaults.samplesPerChannel));
    printf("[blm-twin] Loss: amp=%.3f  pos=%.3f  width=%.3f  enable=%d\n",
           cfg.defaults.lossAmplitude, cfg.defaults.lossPosition,
           cfg.defaults.lossWidth, cfg.defaults.lossEnable);

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = cfg.deviceName + ":BLM_TWIN";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[blm-twin] Failed to connect to Redis\n");
        return 1;
    }

    std::mutex paramMtx;
    BLMParams params = cfg.defaults;
    initRegisters(redis, paramMtx, params);

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);

    printf("[blm-twin] Running at %d ms interval\n", params.updateIntervalMs);

    std::mt19937 rng(42);
    uint64_t tick = 0;

    while (g_running)
    {
        auto loopStart = std::chrono::steady_clock::now();

        BLMParams p;
        {
            std::lock_guard<std::mutex> lk(paramMtx);
            p = params;
        }

        generateBLM(redis, cfg.outputKey, p, rng);

        tick++;
        if (tick % 200 == 0)
            printf("[blm-twin] tick %lu  loss=%s  amp=%.3f  %d samp/ch\n",
                   (unsigned long)tick,
                   p.lossEnable ? "ON" : "OFF",
                   p.lossAmplitude,
                   p.samplesPerChannel);

        auto elapsed = std::chrono::steady_clock::now() - loopStart;
        auto sleepTime = std::chrono::milliseconds(p.updateIntervalMs) - elapsed;
        if (sleepTime > std::chrono::milliseconds(0))
            std::this_thread::sleep_for(sleepTime);
    }

    printf("[blm-twin] Shutting down\n");
    return 0;
}
