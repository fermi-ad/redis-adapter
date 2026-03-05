//
//  BPM Digital Twin
//
//  Defines device registers via RedisAdapterLite and writes all outputs
//  through the standard RAL API (stream field "_").
//
//  Two output modes controlled by MODE_CHANGE register:
//
//  Mode 0 — IQ:
//    4ch IQ, float32, interleaved [I0,Q0,I1,Q1,I2,Q2,I3,Q3, ...]
//    10000 samples/ch × 8 = 80000 floats → OutputKeyIQ
//    Channel amps: H+ ∝ (1+x/r), H- ∝ (1-x/r), V+ ∝ (1+y/r), V- ∝ (1-y/r)
//
//  Mode 1 — RAW_ADC:
//    8ch raw digitizer, uint16, interleaved [A0..A7, A0..A7, ...]
//    10000 samples/ch × 8 = 80000 uint16s → OutputKeyADC
//    8 electrodes at 45° intervals, mid-scale 32768
//
//  All parameters are device registers: readable via GetKey, settable via PutKey.
//
//  Usage: bpm-twin <config.yml>
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

// ---- Device register definitions ----
//
//  Each register has a subKey (written via redis.addDouble / addInt),
//  an optional PutKey for runtime setpoints, a type, and a pointer
//  into the shared parameter block.

struct BPMParams
{
    int    updateIntervalMs  = 50;
    int    samplesPerChannel = 10000;
    int    mode              = 0;      // 0 = IQ, 1 = RAW_ADC

    double beamX         = 0.0;
    double beamY         = 0.0;
    double beamIntensity = 1.0;
    double aperture      = 10.0;

    double carrierFreq   = 5.0;
    double carrierPhase  = 0.0;

    double noise         = 0.02;
    int    gateEnable    = 0;
    double gateStart     = 0.2;
    double gateWidth     = 0.4;

    double adcScale      = 16000.0;
};

enum class RegType { Double, Int };

struct Register
{
    const char* subKey;      // GetKey (read channel)
    const char* putKey;      // PutKey (set channel), nullptr = read-only
    RegType     type;
    size_t      offset;      // byte offset into BPMParams
    int         clampMin;    // int clamp min (only for Int type)
    int         clampMax;    // int clamp max (only for Int type)
};

#define POFF(field) offsetof(BPMParams, field)

static const Register g_registers[] = {
    {"UPDATE_INTERVAL",  "UPDATE_INTERVAL_S",  RegType::Int,    POFF(updateIntervalMs),  10, 10000},
    {"SAMPLES_PER_CH",   "SAMPLES_PER_CH_S",   RegType::Int,    POFF(samplesPerChannel), 16, 65536},
    {"MODE_CHANGE",      "MODE_CHANGE_S",       RegType::Int,    POFF(mode),              0,  1},
    {"BEAM_X",           "BEAM_X_S",            RegType::Double, POFF(beamX),             0,  0},
    {"BEAM_Y",           "BEAM_Y_S",            RegType::Double, POFF(beamY),             0,  0},
    {"BEAM_INTENSITY",   "BEAM_INTENSITY_S",    RegType::Double, POFF(beamIntensity),     0,  0},
    {"APERTURE",         "APERTURE_S",          RegType::Double, POFF(aperture),          0,  0},
    {"CARRIER_FREQ",     "CARRIER_FREQ_S",      RegType::Double, POFF(carrierFreq),       0,  0},
    {"CARRIER_PHASE",    "CARRIER_PHASE_S",     RegType::Double, POFF(carrierPhase),      0,  0},
    {"NOISE",            "NOISE_S",             RegType::Double, POFF(noise),             0,  0},
    {"GATE_ENABLE",      "GATE_ENABLE_S",       RegType::Int,    POFF(gateEnable),        0,  1},
    {"GATE_START",       "GATE_START_S",        RegType::Double, POFF(gateStart),         0,  0},
    {"GATE_WIDTH",       "GATE_WIDTH_S",        RegType::Double, POFF(gateWidth),         0,  0},
    {"ADC_SCALE",        "ADC_SCALE_S",         RegType::Double, POFF(adcScale),          0,  0},
};

static constexpr size_t NUM_REGISTERS = sizeof(g_registers) / sizeof(g_registers[0]);

// ---- Typed accessors into param block ----

static double getRegDouble(const BPMParams& p, size_t offset)
{
    double v;
    std::memcpy(&v, reinterpret_cast<const char*>(&p) + offset, sizeof(double));
    return v;
}

static int getRegInt(const BPMParams& p, size_t offset)
{
    int v;
    std::memcpy(&v, reinterpret_cast<const char*>(&p) + offset, sizeof(int));
    return v;
}

static void setRegDouble(BPMParams& p, size_t offset, double v)
{
    std::memcpy(reinterpret_cast<char*>(&p) + offset, &v, sizeof(double));
}

static void setRegInt(BPMParams& p, size_t offset, int v)
{
    std::memcpy(reinterpret_cast<char*>(&p) + offset, &v, sizeof(int));
}

// ---- Config ----

struct Config
{
    std::string deviceName;
    std::string redisHost = "127.0.0.1";
    std::string outputKeyIQ;
    std::string outputKeyADC;
    BPMParams   defaults;
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

    cfg.deviceName   = dev["DeviceName"].as<std::string>("BPM:0001");
    cfg.redisHost    = dev["RedisHost"].as<std::string>("127.0.0.1");
    cfg.outputKeyIQ  = dev["OutputKeyIQ"].as<std::string>("RAW_MUX_WF");
    cfg.outputKeyADC = dev["OutputKeyADC"].as<std::string>("RAW_ADC");

    auto& p = cfg.defaults;
    p.updateIntervalMs  = dev["UpdateIntervalMs"].as<int>(50);
    p.samplesPerChannel = dev["SamplesPerChannel"].as<int>(10000);
    p.mode              = dev["Mode"].as<int>(0);
    p.beamX             = dev["BeamX"].as<double>(0.0);
    p.beamY             = dev["BeamY"].as<double>(0.0);
    p.beamIntensity     = dev["BeamIntensity"].as<double>(1.0);
    p.aperture          = dev["Aperture"].as<double>(10.0);
    p.carrierFreq       = dev["CarrierFreq"].as<double>(5.0);
    p.carrierPhase      = dev["CarrierPhase"].as<double>(0.0);
    p.noise             = dev["Noise"].as<double>(0.02);
    p.gateEnable        = dev["GateEnable"].as<int>(0);
    p.gateStart         = dev["GateStart"].as<double>(0.2);
    p.gateWidth         = dev["GateWidth"].as<double>(0.4);
    p.adcScale          = dev["ADCScale"].as<double>(16000.0);

    return true;
}

// ---- Publish a register's current value ----

static void publishRegister(RedisAdapterLite& redis, const Register& reg,
                             const BPMParams& params)
{
    if (reg.type == RegType::Double)
        redis.addDouble(reg.subKey, getRegDouble(params, reg.offset));
    else
        redis.addInt(reg.subKey, getRegInt(params, reg.offset));
}

// ---- Register all device registers: publish initial + wire setpoint readers ----

static void initRegisters(RedisAdapterLite& redis, std::mutex& mtx,
                           BPMParams& params)
{
    redis.setDeferReaders(true);

    for (size_t ri = 0; ri < NUM_REGISTERS; ++ri)
    {
        auto& reg = g_registers[ri];

        // publish initial value
        publishRegister(redis, reg, params);

        // wire PutKey reader → update param + echo to GetKey
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
                        printf("[bpm-twin] SET %s = %f\n", reg.subKey, *v);
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
                        printf("[bpm-twin] SET %s = %d\n", reg.subKey, ival);
                    }
                }
            });
        }

        // log
        printf("[bpm-twin]   REG %-20s = ", reg.subKey);
        if (reg.type == RegType::Double)
            printf("%.4f", getRegDouble(params, reg.offset));
        else
            printf("%d", getRegInt(params, reg.offset));
        if (reg.putKey)
            printf("  (set via %s)", reg.putKey);
        printf("\n");
    }

    redis.setDeferReaders(false);
}

// ---- Generate Mode 0: IQ (4ch × IQ × N, float32) ----

static void generateIQ(RedisAdapterLite& redis, const std::string& key,
                        const BPMParams& p, double runningPhase,
                        std::mt19937& rng)
{
    int N = p.samplesPerChannel;

    double clampX = std::max(-p.aperture, std::min(p.aperture, p.beamX));
    double clampY = std::max(-p.aperture, std::min(p.aperture, p.beamY));

    double ampCh[4];
    ampCh[0] = p.beamIntensity * (1.0 + clampX / p.aperture);
    ampCh[1] = p.beamIntensity * (1.0 - clampX / p.aperture);
    ampCh[2] = p.beamIntensity * (1.0 + clampY / p.aperture);
    ampCh[3] = p.beamIntensity * (1.0 - clampY / p.aperture);

    std::normal_distribution<float> nd(0.0f, static_cast<float>(p.noise));
    double step = 2.0 * M_PI * p.carrierFreq / N;

    std::vector<float> buf(N * 8);

    for (int s = 0; s < N; ++s)
    {
        double t     = static_cast<double>(s) / N;
        double theta = step * s + p.carrierPhase + runningPhase;

        bool gated = p.gateEnable &&
            (t < p.gateStart || t >= p.gateStart + p.gateWidth);

        int base = s * 8;
        for (int ch = 0; ch < 4; ++ch)
        {
            float ni = nd(rng);
            float nq = nd(rng);
            if (gated)
            {
                buf[base + ch * 2]     = ni;
                buf[base + ch * 2 + 1] = nq;
            }
            else
            {
                float a = static_cast<float>(ampCh[ch]);
                buf[base + ch * 2]     = a * static_cast<float>(std::cos(theta)) + ni;
                buf[base + ch * 2 + 1] = a * static_cast<float>(std::sin(theta)) + nq;
            }
        }
    }

    redis.addBlob(key, buf.data(), buf.size() * sizeof(float));
}

// ---- Generate Mode 1: RAW_ADC (8ch × N, uint16) ----

static void generateRAW(RedisAdapterLite& redis, const std::string& key,
                         const BPMParams& p, double runningPhase,
                         std::mt19937& rng)
{
    int N = p.samplesPerChannel;

    double clampX = std::max(-p.aperture, std::min(p.aperture, p.beamX));
    double clampY = std::max(-p.aperture, std::min(p.aperture, p.beamY));

    double ampCh[8];
    for (int ch = 0; ch < 8; ++ch)
    {
        double angle = ch * M_PI / 4.0;
        double proj  = clampX * std::cos(angle) + clampY * std::sin(angle);
        ampCh[ch]    = p.beamIntensity * (1.0 + proj / p.aperture);
    }

    std::normal_distribution<double> nd(0.0, p.noise);
    double step = 2.0 * M_PI * p.carrierFreq / N;

    std::vector<uint16_t> buf(N * 8);

    for (int s = 0; s < N; ++s)
    {
        double t     = static_cast<double>(s) / N;
        double theta = step * s + p.carrierPhase + runningPhase;

        bool gated = p.gateEnable &&
            (t < p.gateStart || t >= p.gateStart + p.gateWidth);

        int base = s * 8;
        for (int ch = 0; ch < 8; ++ch)
        {
            double signal = gated ? nd(rng)
                                  : ampCh[ch] * std::sin(theta) + nd(rng);

            double counts = 32768.0 + signal * p.adcScale;
            if (counts < 0.0)     counts = 0.0;
            if (counts > 65535.0) counts = 65535.0;
            buf[base + ch] = static_cast<uint16_t>(counts);
        }
    }

    redis.addBlob(key, buf.data(), buf.size() * sizeof(uint16_t));
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

    const char* modeNames[] = {"IQ (4ch float32)", "RAW_ADC (8ch uint16)"};

    printf("[bpm-twin] Device: %s  Redis: %s\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str());
    printf("[bpm-twin] IQ → %s  ADC → %s  Mode: %s\n",
           cfg.outputKeyIQ.c_str(), cfg.outputKeyADC.c_str(),
           modeNames[cfg.defaults.mode & 1]);
    printf("[bpm-twin] %d samples/ch  Beam: (%.2f, %.2f)  Intensity: %.2f\n",
           cfg.defaults.samplesPerChannel,
           cfg.defaults.beamX, cfg.defaults.beamY, cfg.defaults.beamIntensity);

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = cfg.deviceName + ":BPM_TWIN";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[bpm-twin] Failed to connect to Redis\n");
        return 1;
    }

    // Register all device registers (publish initial values + setpoint listeners)
    std::mutex paramMtx;
    BPMParams params = cfg.defaults;
    initRegisters(redis, paramMtx, params);

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);

    printf("[bpm-twin] Running at %d ms interval\n", params.updateIntervalMs);

    std::mt19937 rng(42);
    double runningPhase = 0.0;
    uint64_t tick = 0;
    int lastMode = -1;

    while (g_running)
    {
        auto loopStart = std::chrono::steady_clock::now();

        BPMParams p;
        {
            std::lock_guard<std::mutex> lk(paramMtx);
            p = params;
        }

        runningPhase += 2.0 * M_PI * p.carrierFreq * p.updateIntervalMs / 1000.0;
        if (runningPhase > 2.0 * M_PI) runningPhase -= 2.0 * M_PI;

        if (p.mode != lastMode)
        {
            printf("[bpm-twin] Mode → %s\n", modeNames[p.mode & 1]);
            lastMode = p.mode;
        }

        if (p.mode == 0)
            generateIQ(redis, cfg.outputKeyIQ, p, runningPhase, rng);
        else
            generateRAW(redis, cfg.outputKeyADC, p, runningPhase, rng);

        tick++;
        if (tick % 200 == 0)
            printf("[bpm-twin] tick %lu  mode=%d  beam=(%.2f,%.2f)  %d samp/ch\n",
                   (unsigned long)tick, p.mode, p.beamX, p.beamY,
                   p.samplesPerChannel);

        auto elapsed = std::chrono::steady_clock::now() - loopStart;
        auto sleepTime = std::chrono::milliseconds(p.updateIntervalMs) - elapsed;
        if (sleepTime > std::chrono::milliseconds(0))
            std::this_thread::sleep_for(sleepTime);
    }

    printf("[bpm-twin] Shutting down\n");
    return 0;
}
