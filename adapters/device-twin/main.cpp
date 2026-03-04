//
//  Device Digital Twin
//
//  Generic digital twin that reads a YAML device config and:
//    - Connects to Redis via RedisAdapterLite
//    - Listens for setting writes (PutKey / _S channels)
//    - Echoes settings back on the reading channel (GetKey)
//    - Generates dummy data: sinewaves for array PVs, random scalars
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
        pvList.push_back(std::move(d));
    }

    return !pvList.empty();
}

// ---- Generate a sinewave with some noise ----

static void generateSinewave(std::vector<float>& buf, int count,
                              double frequency, double phase,
                              std::mt19937& rng)
{
    buf.resize(count);
    std::normal_distribution<float> noise(0.0f, 0.02f);
    double step = 2.0 * M_PI * frequency / count;
    for (int i = 0; i < count; ++i)
        buf[i] = static_cast<float>(std::sin(step * i + phase)) + noise(rng);
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

    // Build PV state map
    std::mutex pvMutex;
    std::unordered_map<std::string, PVState> pvMap;

    // Track update interval and waveform frequency (settable PVs)
    std::atomic<int>    updateIntervalMs{50};
    std::atomic<double> waveformFrequency{0.1};

    for (auto& d : pvDescs)
    {
        PVState st;
        st.desc       = d;
        st.currentVal = d.initVal;
        st.currentInt = static_cast<int64_t>(d.initVal);
        pvMap[d.getKey] = std::move(st);
    }

    // Write initial values for all PVs
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

    // Pick up special PV initial values
    if (pvMap.count("DEVICE_UPDATE_INTERVAL"))
        updateIntervalMs = static_cast<int>(pvMap["DEVICE_UPDATE_INTERVAL"].currentVal);
    if (pvMap.count("WAVEFORM_FREQUENCY"))
        waveformFrequency = pvMap["WAVEFORM_FREQUENCY"].currentVal;

    // Set up readers for all settable PVs (those with PutKey)
    // When a value is written to the PutKey (_S), echo it back on GetKey
    for (auto& [key, st] : pvMap)
    {
        if (st.desc.putKey.empty())
            continue;

        const std::string putKey = st.desc.putKey;
        const std::string getKey = st.desc.getKey;
        const ValType     vt     = st.desc.valType;

        redis.addReader(putKey,
            [&redis, &pvMap, &pvMutex, &updateIntervalMs, &waveformFrequency,
             getKey, putKey, vt]
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

                    // Echo back on reading channel
                    redis.addInt(getKey, val);

                    // Update local state
                    {
                        std::lock_guard<std::mutex> lk(pvMutex);
                        if (pvMap.count(getKey))
                        {
                            pvMap[getKey].currentInt = val;
                            pvMap[getKey].currentVal = static_cast<double>(val);
                        }
                    }

                    // Handle special PVs
                    if (getKey == "DEVICE_UPDATE_INTERVAL")
                    {
                        int ms = static_cast<int>(val);
                        if (ms >= 10 && ms <= 10000)
                            updateIntervalMs = ms;
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

                    if (getKey == "WAVEFORM_FREQUENCY")
                        waveformFrequency = val;

                    printf("[twin] SET %s -> %s = %f\n",
                           putKey.c_str(), getKey.c_str(), val);
                }
            }
        });

        printf("[twin]   listening on %s -> echoes to %s\n",
               putKey.c_str(), getKey.c_str());
    }

    // Signal handling
    signal(SIGINT,  sigHandler);
    signal(SIGTERM, sigHandler);

    printf("[twin] Running at %d ms update interval (%.1f Hz)\n",
           updateIntervalMs.load(), 1000.0 / updateIntervalMs.load());

    // Main data generation loop
    std::mt19937 rng(42);
    std::normal_distribution<double> scalarNoise(0.0, 0.01);
    double phase = 0.0;
    uint64_t tick = 0;

    while (g_running)
    {
        auto loopStart = std::chrono::steady_clock::now();
        int intervalMs = updateIntervalMs.load();
        double freq    = waveformFrequency.load();

        // Advance phase
        phase += 2.0 * M_PI * freq * intervalMs / 1000.0;
        if (phase > 2.0 * M_PI) phase -= 2.0 * M_PI;

        std::lock_guard<std::mutex> lk(pvMutex);

        for (auto& [key, st] : pvMap)
        {
            // Skip settable-only PVs (they update via echo)
            if (!st.desc.putKey.empty() && st.desc.valCount == 0)
                continue;

            if (st.desc.valCount > 0)
            {
                // Array PV: generate sinewave data
                std::vector<float> wf;
                generateSinewave(wf, st.desc.valCount, freq, phase, rng);
                redis.addBlob(st.desc.getKey, wf.data(),
                              wf.size() * sizeof(float));
            }
            else if (st.desc.valType == ValType::Float32)
            {
                // Scalar float: slow random walk around a base
                double base = std::sin(phase) * 50.0 + 100.0;
                st.currentVal = base + scalarNoise(rng) * 10.0;
                redis.addDouble(st.desc.getKey, st.currentVal);
            }
            // Int32 read-only scalars with no PutKey would go here if needed
        }

        tick++;
        if (tick % 200 == 0)
            printf("[twin] tick %lu  interval=%dms  freq=%.3f\n",
                   (unsigned long)tick, intervalMs, freq);

        // Sleep for remainder of interval
        auto elapsed = std::chrono::steady_clock::now() - loopStart;
        auto sleepTime = std::chrono::milliseconds(intervalMs) - elapsed;
        if (sleepTime > std::chrono::milliseconds(0))
            std::this_thread::sleep_for(sleepTime);
    }

    printf("[twin] Shutting down\n");
    return 0;
}
