//
//  BCM Digital Twin
//
//  Simulates a 4-channel Beam Current Monitor digitizer at 5 MHz.
//  Each trigger produces two interleaved waveforms:
//
//    RAW_BCM_WF — signal waveform (beam present in gate region)
//      [CH0,CH1,CH2,CH3, CH0,CH1,CH2,CH3, ...] × SamplesPerChannel
//
//    BKG_BCM_WF — background waveform (baseline + noise, no beam)
//      Same layout, captured with beam off for subtraction
//
//  Signal model per channel:
//    Within gate:  baseline + gain[ch] × beamCurrent × sin(carrierFreq) + noise
//    Outside gate: baseline + noise
//    Background:   baseline + noise (always, no beam component)
//
//  4 channels per board, each with independent gain scaling.
//  All parameters are device registers: readable via GetKey, settable via PutKey.
//
//  Usage: bcm-twin <config.yml>
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

static constexpr int NUM_CHANNELS = 4;

// ---- Device register definitions ----

struct BCMParams
{
    int    updateIntervalMs  = 50;      // 20 Hz output rate
    int    samplesPerChannel = 50000;   // 10ms at 5 MHz

    double beamCurrent   = 100.0;       // mA, simulated beam current
    double baseline      = 0.01;        // DC baseline offset
    double noise         = 0.005;       // Gaussian noise amplitude

    double carrierFreq   = 50.0;        // cycles per waveform (RF carrier)
    double carrierPhase  = 0.0;

    int    gateEnable    = 1;
    double gateStart     = 0.2;         // fractional start of beam window
    double gateWidth     = 0.4;         // fractional width of beam window

    // Per-channel gain (sensitivity scaling)
    double gain[NUM_CHANNELS] = {1.0, 0.95, 1.05, 0.98};
};

enum class RegType { Double, Int };

struct Register
{
    const char* subKey;
    const char* putKey;
    RegType     type;
    size_t      offset;
    int         clampMin;
    int         clampMax;
};

#define POFF(field) offsetof(BCMParams, field)

static const Register g_registers[] = {
    {"UPDATE_INTERVAL",  "UPDATE_INTERVAL_S",  RegType::Int,    POFF(updateIntervalMs),  10, 10000},
    {"SAMPLES_PER_CH",   "SAMPLES_PER_CH_S",   RegType::Int,    POFF(samplesPerChannel), 1000, 500000},
    {"BEAM_CURRENT",     "BEAM_CURRENT_S",     RegType::Double, POFF(beamCurrent),       0, 0},
    {"BASELINE",         "BASELINE_S",         RegType::Double, POFF(baseline),          0, 0},
    {"NOISE",            "NOISE_S",            RegType::Double, POFF(noise),             0, 0},
    {"CARRIER_FREQ",     "CARRIER_FREQ_S",     RegType::Double, POFF(carrierFreq),       0, 0},
    {"CARRIER_PHASE",    "CARRIER_PHASE_S",    RegType::Double, POFF(carrierPhase),      0, 0},
    {"GATE_ENABLE",      "GATE_ENABLE_S",      RegType::Int,    POFF(gateEnable),        0, 1},
    {"GATE_START",       "GATE_START_S",       RegType::Double, POFF(gateStart),         0, 0},
    {"GATE_WIDTH",       "GATE_WIDTH_S",       RegType::Double, POFF(gateWidth),         0, 0},
    {"CH0_GAIN",         "CH0_GAIN_S",         RegType::Double, POFF(gain[0]),           0, 0},
    {"CH1_GAIN",         "CH1_GAIN_S",         RegType::Double, POFF(gain[1]),           0, 0},
    {"CH2_GAIN",         "CH2_GAIN_S",         RegType::Double, POFF(gain[2]),           0, 0},
    {"CH3_GAIN",         "CH3_GAIN_S",         RegType::Double, POFF(gain[3]),           0, 0},
};

static constexpr size_t NUM_REGISTERS = sizeof(g_registers) / sizeof(g_registers[0]);

// ---- Typed accessors into param block ----

static double getRegDouble(const BCMParams& p, size_t offset)
{
    double v;
    std::memcpy(&v, reinterpret_cast<const char*>(&p) + offset, sizeof(double));
    return v;
}

static int getRegInt(const BCMParams& p, size_t offset)
{
    int v;
    std::memcpy(&v, reinterpret_cast<const char*>(&p) + offset, sizeof(int));
    return v;
}

static void setRegDouble(BCMParams& p, size_t offset, double v)
{
    std::memcpy(reinterpret_cast<char*>(&p) + offset, &v, sizeof(double));
}

static void setRegInt(BCMParams& p, size_t offset, int v)
{
    std::memcpy(reinterpret_cast<char*>(&p) + offset, &v, sizeof(int));
}

// ---- Config ----

struct Config
{
    std::string deviceName;
    std::string redisHost = "127.0.0.1";
    std::string outputKeyRaw;
    std::string outputKeyBkg;
    BCMParams   defaults;
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

    cfg.deviceName   = dev["DeviceName"].as<std::string>("BCM:0001");
    cfg.redisHost    = dev["RedisHost"].as<std::string>("127.0.0.1");
    cfg.outputKeyRaw = dev["OutputKeyRaw"].as<std::string>("RAW_BCM_WF");
    cfg.outputKeyBkg = dev["OutputKeyBkg"].as<std::string>("BKG_BCM_WF");

    auto& p = cfg.defaults;
    p.updateIntervalMs  = dev["UpdateIntervalMs"].as<int>(50);
    p.samplesPerChannel = dev["SamplesPerChannel"].as<int>(50000);
    p.beamCurrent       = dev["BeamCurrent"].as<double>(100.0);
    p.baseline          = dev["Baseline"].as<double>(0.01);
    p.noise             = dev["Noise"].as<double>(0.005);
    p.carrierFreq       = dev["CarrierFreq"].as<double>(50.0);
    p.carrierPhase      = dev["CarrierPhase"].as<double>(0.0);
    p.gateEnable        = dev["GateEnable"].as<int>(1);
    p.gateStart         = dev["GateStart"].as<double>(0.2);
    p.gateWidth         = dev["GateWidth"].as<double>(0.4);

    auto gainNode = dev["ChannelGains"];
    if (gainNode && gainNode.IsSequence())
    {
        for (size_t i = 0; i < NUM_CHANNELS && i < gainNode.size(); ++i)
            p.gain[i] = gainNode[i].as<double>(1.0);
    }

    return true;
}

// ---- Publish a register's current value ----

static void publishRegister(RedisAdapterLite& redis, const Register& reg,
                             const BCMParams& params)
{
    if (reg.type == RegType::Double)
        redis.addDouble(reg.subKey, getRegDouble(params, reg.offset));
    else
        redis.addInt(reg.subKey, getRegInt(params, reg.offset));
}

// ---- Register all device registers ----

static void initRegisters(RedisAdapterLite& redis, std::mutex& mtx,
                           BCMParams& params)
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
                        printf("[bcm-twin] SET %s = %f\n", reg.subKey, *v);
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
                        printf("[bcm-twin] SET %s = %d\n", reg.subKey, ival);
                    }
                }
            });
        }

        printf("[bcm-twin]   REG %-20s = ", reg.subKey);
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

// ---- Generate BCM waveforms (raw + background) ----

static void generateBCM(RedisAdapterLite& redis,
                          const std::string& rawKey,
                          const std::string& bkgKey,
                          const BCMParams& p, double runningPhase,
                          std::mt19937& rng)
{
    int N = p.samplesPerChannel;
    std::normal_distribution<float> nd(0.0f, static_cast<float>(p.noise));
    double step = 2.0 * M_PI * p.carrierFreq / N;

    std::vector<float> rawBuf(N * NUM_CHANNELS);
    std::vector<float> bkgBuf(N * NUM_CHANNELS);

    for (int s = 0; s < N; ++s)
    {
        double t     = static_cast<double>(s) / N;
        double theta = step * s + p.carrierPhase + runningPhase;
        double carrier = std::sin(theta);

        bool inGate = !p.gateEnable ||
            (t >= p.gateStart && t < p.gateStart + p.gateWidth);

        int base = s * NUM_CHANNELS;
        for (int ch = 0; ch < NUM_CHANNELS; ++ch)
        {
            float nRaw = nd(rng);
            float nBkg = nd(rng);

            // Background: baseline + noise only (no beam)
            bkgBuf[base + ch] = static_cast<float>(p.baseline) + nBkg;

            // Raw: baseline + beam signal (within gate) + noise
            if (inGate)
            {
                double signal = p.gain[ch] * p.beamCurrent * carrier;
                rawBuf[base + ch] = static_cast<float>(p.baseline + signal) + nRaw;
            }
            else
            {
                rawBuf[base + ch] = static_cast<float>(p.baseline) + nRaw;
            }
        }
    }

    redis.addBlob(rawKey, rawBuf.data(), rawBuf.size() * sizeof(float));
    redis.addBlob(bkgKey, bkgBuf.data(), bkgBuf.size() * sizeof(float));
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

    printf("[bcm-twin] Device: %s  Redis: %s\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str());
    printf("[bcm-twin] Raw → %s  Bkg → %s\n",
           cfg.outputKeyRaw.c_str(), cfg.outputKeyBkg.c_str());
    printf("[bcm-twin] %d samples/ch (%d ch)  %.1f MHz  BeamI=%.1f mA\n",
           cfg.defaults.samplesPerChannel, NUM_CHANNELS,
           cfg.defaults.carrierFreq * cfg.defaults.samplesPerChannel / 1e6,
           cfg.defaults.beamCurrent);
    printf("[bcm-twin] Gate: %s  start=%.3f  width=%.3f\n",
           cfg.defaults.gateEnable ? "ON" : "OFF",
           cfg.defaults.gateStart, cfg.defaults.gateWidth);

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = cfg.deviceName + ":BCM_TWIN";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[bcm-twin] Failed to connect to Redis\n");
        return 1;
    }

    std::mutex paramMtx;
    BCMParams params = cfg.defaults;
    initRegisters(redis, paramMtx, params);

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);

    printf("[bcm-twin] Running at %d ms interval (%.0f Hz)\n",
           params.updateIntervalMs,
           1000.0 / params.updateIntervalMs);

    std::mt19937 rng(42);
    double runningPhase = 0.0;
    uint64_t tick = 0;

    while (g_running)
    {
        auto loopStart = std::chrono::steady_clock::now();

        BCMParams p;
        {
            std::lock_guard<std::mutex> lk(paramMtx);
            p = params;
        }

        runningPhase += 2.0 * M_PI * p.carrierFreq * p.updateIntervalMs / 1000.0;
        if (runningPhase > 2.0 * M_PI) runningPhase -= 2.0 * M_PI;

        generateBCM(redis, cfg.outputKeyRaw, cfg.outputKeyBkg, p, runningPhase, rng);

        tick++;
        if (tick % 200 == 0)
            printf("[bcm-twin] tick %lu  beamI=%.1f mA  gate=%s  %d samp/ch\n",
                   (unsigned long)tick, p.beamCurrent,
                   p.gateEnable ? "ON" : "OFF",
                   p.samplesPerChannel);

        auto elapsed = std::chrono::steady_clock::now() - loopStart;
        auto sleepTime = std::chrono::milliseconds(p.updateIntervalMs) - elapsed;
        if (sleepTime > std::chrono::milliseconds(0))
            std::this_thread::sleep_for(sleepTime);
    }

    printf("[bcm-twin] Shutting down\n");
    return 0;
}
