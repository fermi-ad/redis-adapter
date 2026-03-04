//
//  Device Digital Twin
//
//  Generic digital twin that reads a YAML device config and:
//    - Connects to Redis via RedisAdapterLite
//    - Listens for setting writes (PutKey / _S channels)
//    - Echoes settings back on the reading channel (GetKey)
//    - Generates dummy data: sinewaves for array PVs, oscillating scalars
//    - Supports reserved control PVs for frequency, amplitude, phase, noise, gate
//    - Updates at configurable rate (DEVICE_UPDATE_INTERVAL, default 50ms = 20Hz)
//
//  Usage: device-twin <config.yml>
//

#include "RedisAdapterLite.hpp"
#include "RAL_Helpers.hpp"
#include <yaml-cpp/yaml.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// ---- PV descriptor parsed from YAML ----

enum class ValType { Float32, Int32 };

struct PVDesc
{
    std::string pvName;
    std::string getKey;
    std::string putKey;   // empty = read-only (no setting channel)
    ValType     valType   = ValType::Float32;
    int         valCount  = 0;    // >0 = waveform/array, 0 = scalar
    double      initVal   = 0.0;
};

// ---- Runtime state for each PV ----

struct PVState
{
    PVDesc  desc;
    double  currentVal = 0.0;   // current scalar value (or unused for arrays)
    int64_t currentInt = 0;     // current int value for Int32 scalars
};

// ---- Signal generation parameters (reserved PVs) ----

struct TwinParams
{
    int    updateIntervalMs = 50;
    double frequency        = 1.0;
    double amplitude        = 1.0;
    double phase            = 0.0;
    double noise            = 0.02;
    int    gateEnable       = 0;
    double gateStart        = 0.1;
    double gateWidth        = 0.3;
};

// ---- Reserved control PV definitions ----

struct ControlPVDef
{
    const char* pvName;
    const char* getKey;
    const char* putKey;
    ValType     valType;
    double      initVal;
};

static const ControlPVDef g_controlPVs[] = {
    {"DEVICE_UPDATE_INTERVAL", "DEVICE_UPDATE_INTERVAL", "DEVICE_UPDATE_INTERVAL_S", ValType::Int32,   50.0},
    {"WAVEFORM_FREQUENCY",     "WAVEFORM_FREQUENCY",     "WAVEFORM_FREQUENCY_S",     ValType::Float32, 1.0},
    {"WAVEFORM_AMPLITUDE",     "WAVEFORM_AMPLITUDE",     "WAVEFORM_AMPLITUDE_S",     ValType::Float32, 1.0},
    {"WAVEFORM_PHASE",         "WAVEFORM_PHASE",         "WAVEFORM_PHASE_S",         ValType::Float32, 0.0},
    {"WAVEFORM_NOISE",         "WAVEFORM_NOISE",         "WAVEFORM_NOISE_S",         ValType::Float32, 0.02},
    {"GATE_ENABLE",            "GATE_ENABLE",            "GATE_ENABLE_S",            ValType::Int32,   0.0},
    {"GATE_START",             "GATE_START",             "GATE_START_S",             ValType::Float32, 0.1},
    {"GATE_WIDTH",             "GATE_WIDTH",             "GATE_WIDTH_S",             ValType::Float32, 0.3},
};

static bool isReservedPV(const std::string& pvName)
{
    for (auto& def : g_controlPVs)
        if (pvName == def.pvName)
            return true;
    return false;
}

// ---- Globals ----

static std::atomic<bool> g_running{true};

static void sigHandler(int) { g_running = false; }

// ---- Parse YAML config ----

static bool parseConfig(const std::string& path,
                        std::string& deviceName,
                        std::string& redisHost,
                        std::vector<PVDesc>& pvList)
{
    YAML::Node root;
    try { root = YAML::LoadFile(path); }
    catch (const std::exception& e)
    {
        fprintf(stderr, "Failed to load config %s: %s\n", path.c_str(), e.what());
        return false;
    }

    auto device = root["Device"];
    if (!device)
    {
        fprintf(stderr, "Config missing 'Device' root key\n");
        return false;
    }

    deviceName = device["DeviceName"].as<std::string>("DEVICE:0001");
    redisHost  = device["RedisHost"].as<std::string>("127.0.0.1");

    auto pvListNode = device["PVList"];
    if (!pvListNode || !pvListNode.IsSequence())
    {
        fprintf(stderr, "Config missing 'PVList' sequence\n");
        return false;
    }

    for (auto&& pv : pvListNode)
    {
        PVDesc d;
        d.pvName  = pv["PVName"].as<std::string>("");
        d.getKey  = pv["GetKey"].as<std::string>(d.pvName);
        d.putKey  = pv["PutKey"].as<std::string>("");
        d.initVal = pv["InitVal"].as<double>(0.0);

        auto vt = pv["ValType"].as<std::string>("Float32");
        d.valType = (vt == "Int32") ? ValType::Int32 : ValType::Float32;

        d.valCount = pv["ValCount"].as<int>(0);

        if (d.pvName.empty())
        {
            fprintf(stderr, "Skipping PV entry with no PVName\n");
            continue;
        }
        if (isReservedPV(d.pvName))
        {
            printf("[twin] Ignoring reserved PV '%s' in config (handled internally)\n",
                   d.pvName.c_str());
            continue;
        }
        pvList.push_back(std::move(d));
    }

    return !pvList.empty();
}

// ---- Generate signal with gate/pulse support ----

static void generateSignal(std::vector<float>& buf, int count,
                            double frequency, double runningPhase,
                            double amplitude, double phaseOffset,
                            double noiseSigma,
                            bool gateEnable, double gateStart, double gateWidth,
                            std::mt19937& rng)
{
    buf.resize(count);
    std::normal_distribution<float> noiseDist(0.0f, static_cast<float>(noiseSigma));
    double step = 2.0 * M_PI * frequency / count;
    for (int i = 0; i < count; ++i)
    {
        float n = noiseDist(rng);
        if (gateEnable)
        {
            double t = static_cast<double>(i) / count;
            if (t >= gateStart && t < gateStart + gateWidth)
                buf[i] = static_cast<float>(amplitude * std::sin(step * i + phaseOffset + runningPhase)) + n;
            else
                buf[i] = n;
        }
        else
        {
            buf[i] = static_cast<float>(amplitude * std::sin(step * i + phaseOffset + runningPhase)) + n;
        }
    }
}

// ---- Build table mapping reserved GetKey names to TwinParams fields ----

struct ParamBinding
{
    enum Kind { Double, Int } kind;
    double* dptr = nullptr;
    int*    iptr = nullptr;
};

static std::unordered_map<std::string, ParamBinding>
buildParamTable(TwinParams& p)
{
    std::unordered_map<std::string, ParamBinding> table;
    table["DEVICE_UPDATE_INTERVAL"] = {ParamBinding::Int,    nullptr, &p.updateIntervalMs};
    table["WAVEFORM_FREQUENCY"]     = {ParamBinding::Double, &p.frequency, nullptr};
    table["WAVEFORM_AMPLITUDE"]     = {ParamBinding::Double, &p.amplitude, nullptr};
    table["WAVEFORM_PHASE"]         = {ParamBinding::Double, &p.phase, nullptr};
    table["WAVEFORM_NOISE"]         = {ParamBinding::Double, &p.noise, nullptr};
    table["GATE_ENABLE"]            = {ParamBinding::Int,    nullptr, &p.gateEnable};
    table["GATE_START"]             = {ParamBinding::Double, &p.gateStart, nullptr};
    table["GATE_WIDTH"]             = {ParamBinding::Double, &p.gateWidth, nullptr};
    return table;
}

// ---- Main ----

int main(int argc, char* argv[])
{
    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <config.yml>\n", argv[0]);
        return 1;
    }

    // Parse config
    std::string deviceName, redisHost;
    std::vector<PVDesc> pvDescs;
    if (!parseConfig(argv[1], deviceName, redisHost, pvDescs))
        return 1;

    printf("[twin] Device: %s  Redis: %s  PVs: %zu\n",
           deviceName.c_str(), redisHost.c_str(), pvDescs.size());

    // Connect to Redis
    RAL_Options opts;
    opts.host    = redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = deviceName + ":TWIN";

    RedisAdapterLite redis(deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[twin] Failed to connect to Redis at %s\n", redisHost.c_str());
        return 1;
    }
    printf("[twin] Connected to Redis\n");

    // Build PV state map from user-defined PVs
    std::mutex pvMutex;
    std::unordered_map<std::string, PVState> pvMap;
    TwinParams params;
    auto paramTable = buildParamTable(params);

    for (auto& d : pvDescs)
    {
        PVState st;
        st.desc       = d;
        st.currentVal = d.initVal;
        st.currentInt = static_cast<int64_t>(d.initVal);
        pvMap[d.getKey] = std::move(st);
    }

    // Write initial values for user-defined PVs
    for (auto& [key, st] : pvMap)
    {
        if (st.desc.valCount > 0)
            continue; // arrays start generating on first tick

        if (st.desc.valType == ValType::Int32)
            redis.addInt(st.desc.getKey, st.currentInt);
        else
            redis.addDouble(st.desc.getKey, st.currentVal);

        printf("[twin]   init %s = %g\n", st.desc.pvName.c_str(), st.currentVal);
    }

    // Set up readers for user-defined settable PVs (those with PutKey)
    for (auto& [key, st] : pvMap)
    {
        if (st.desc.putKey.empty())
            continue;

        const std::string putKey = st.desc.putKey;
        const std::string getKey = st.desc.getKey;
        const ValType     vt     = st.desc.valType;

        redis.addReader(putKey,
            [&redis, &pvMap, &pvMutex, getKey, putKey, vt]
            (const std::string& /*base*/, const std::string& /*sub*/,
             const TimeAttrsList& data)
        {
            for (auto& [time, attrs] : data)
            {
                if (vt == ValType::Int32)
                {
                    auto v = ral_to_int(attrs);
                    if (!v) continue;
                    int64_t val = *v;
                    redis.addInt(getKey, val);
                    {
                        std::lock_guard<std::mutex> lk(pvMutex);
                        if (pvMap.count(getKey))
                        {
                            pvMap[getKey].currentInt = val;
                            pvMap[getKey].currentVal = static_cast<double>(val);
                        }
                    }
                    printf("[twin] SET %s -> %s = %ld\n",
                           putKey.c_str(), getKey.c_str(), (long)val);
                }
                else
                {
                    auto v = ral_to_double(attrs);
                    if (!v) continue;
                    double val = *v;
                    redis.addDouble(getKey, val);
                    {
                        std::lock_guard<std::mutex> lk(pvMutex);
                        if (pvMap.count(getKey))
                            pvMap[getKey].currentVal = val;
                    }
                    printf("[twin] SET %s -> %s = %f\n",
                           putKey.c_str(), getKey.c_str(), val);
                }
            }
        });

        printf("[twin]   listening on %s -> echoes to %s\n",
               putKey.c_str(), getKey.c_str());
    }

    // Register reserved control PVs (always present, not from config)
    for (auto& def : g_controlPVs)
    {
        // Publish initial value
        if (def.valType == ValType::Int32)
            redis.addInt(def.getKey, static_cast<int64_t>(def.initVal));
        else
            redis.addDouble(def.getKey, def.initVal);

        printf("[twin]   init control %s = %g\n", def.pvName, def.initVal);

        // Wire up reader for the setting channel
        const std::string putKey = def.putKey;
        const std::string getKey = def.getKey;
        const ValType     vt     = def.valType;

        redis.addReader(putKey,
            [&redis, &pvMutex, &params, &paramTable, getKey, putKey, vt]
            (const std::string& /*base*/, const std::string& /*sub*/,
             const TimeAttrsList& data)
        {
            for (auto& [time, attrs] : data)
            {
                if (vt == ValType::Int32)
                {
                    auto v = ral_to_int(attrs);
                    if (!v) continue;
                    int64_t val = *v;
                    redis.addInt(getKey, val);
                    {
                        std::lock_guard<std::mutex> lk(pvMutex);
                        auto pit = paramTable.find(getKey);
                        if (pit != paramTable.end() && pit->second.kind == ParamBinding::Int)
                        {
                            int ival = static_cast<int>(val);
                            if (getKey == "DEVICE_UPDATE_INTERVAL")
                            {
                                if (ival >= 10 && ival <= 10000)
                                    *pit->second.iptr = ival;
                            }
                            else
                            {
                                *pit->second.iptr = ival;
                            }
                        }
                    }
                    printf("[twin] SET %s -> %s = %ld\n",
                           putKey.c_str(), getKey.c_str(), (long)val);
                }
                else
                {
                    auto v = ral_to_double(attrs);
                    if (!v) continue;
                    double val = *v;
                    redis.addDouble(getKey, val);
                    {
                        std::lock_guard<std::mutex> lk(pvMutex);
                        auto pit = paramTable.find(getKey);
                        if (pit != paramTable.end() && pit->second.kind == ParamBinding::Double)
                            *pit->second.dptr = val;
                    }
                    printf("[twin] SET %s -> %s = %f\n",
                           putKey.c_str(), getKey.c_str(), val);
                }
            }
        });

        printf("[twin]   listening on %s -> echoes to %s\n", def.putKey, def.getKey);
    }

    // Signal handling
    signal(SIGINT,  sigHandler);
    signal(SIGTERM, sigHandler);

    printf("[twin] Running at %d ms update interval (%.1f Hz)\n",
           params.updateIntervalMs, 1000.0 / params.updateIntervalMs);

    // Main data generation loop
    std::mt19937 rng(42);
    double runningPhase = 0.0;
    uint64_t tick = 0;

    while (g_running)
    {
        auto loopStart = std::chrono::steady_clock::now();

        // Snapshot params under lock
        TwinParams p;
        {
            std::lock_guard<std::mutex> lk(pvMutex);
            p = params;
        }

        // Advance running phase
        runningPhase += 2.0 * M_PI * p.frequency * p.updateIntervalMs / 1000.0;
        if (runningPhase > 2.0 * M_PI) runningPhase -= 2.0 * M_PI;

        std::normal_distribution<double> scalarNoise(0.0, p.noise);

        std::lock_guard<std::mutex> lk(pvMutex);

        for (auto& [key, st] : pvMap)
        {
            // Skip settable-only PVs (they update via echo)
            if (!st.desc.putKey.empty() && st.desc.valCount == 0)
                continue;

            if (st.desc.valCount > 0)
            {
                // Array PV: generate signal with gate support
                std::vector<float> wf;
                generateSignal(wf, st.desc.valCount,
                               p.frequency, runningPhase,
                               p.amplitude, p.phase, p.noise,
                               p.gateEnable != 0, p.gateStart, p.gateWidth,
                               rng);
                redis.addBlob(st.desc.getKey, wf.data(),
                              wf.size() * sizeof(float));
            }
            else if (st.desc.valType == ValType::Float32)
            {
                // Scalar float: oscillate around amplitude-scaled sine
                double base = p.amplitude * std::sin(runningPhase);
                st.currentVal = base + scalarNoise(rng) * 10.0;
                redis.addDouble(st.desc.getKey, st.currentVal);
            }
        }

        tick++;
        if (tick % 200 == 0)
            printf("[twin] tick %lu  interval=%dms  freq=%.3f  amp=%.3f  gate=%d\n",
                   (unsigned long)tick, p.updateIntervalMs, p.frequency,
                   p.amplitude, p.gateEnable);

        // Sleep for remainder of interval
        auto elapsed = std::chrono::steady_clock::now() - loopStart;
        auto sleepTime = std::chrono::milliseconds(p.updateIntervalMs) - elapsed;
        if (sleepTime > std::chrono::milliseconds(0))
            std::this_thread::sleep_for(sleepTime);
    }

    printf("[twin] Shutting down\n");
    return 0;
}
