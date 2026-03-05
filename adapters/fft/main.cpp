//
//  FFT Adapter
//
//  Takes a real-valued input waveform, computes its FFT, and writes
//  magnitude and phase spectra.  Input is zero-padded to the next
//  power of two.  Only the first N/2+1 bins (DC … Nyquist) are output.
//
//  Trigger: InputKey
//  Usage: fft <config.yml>
//

#include "waveform_utils.hpp"
#include <yaml-cpp/yaml.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <complex>
#include <csignal>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

static std::atomic<bool> g_running{true};
static void sigHandler(int) { g_running = false; }

// ---- Radix-2 Cooley-Tukey FFT (in-place, iterative) ----

static void fft(std::vector<std::complex<double>>& x)
{
    size_t N = x.size();
    if (N <= 1) return;

    // bit-reversal permutation
    for (size_t i = 1, j = 0; i < N; ++i)
    {
        size_t bit = N >> 1;
        for (; j & bit; bit >>= 1)
            j ^= bit;
        j ^= bit;
        if (i < j) std::swap(x[i], x[j]);
    }

    // Cooley-Tukey butterfly
    for (size_t len = 2; len <= N; len <<= 1)
    {
        double angle = -2.0 * M_PI / static_cast<double>(len);
        std::complex<double> wlen(std::cos(angle), std::sin(angle));
        for (size_t i = 0; i < N; i += len)
        {
            std::complex<double> w(1.0, 0.0);
            for (size_t j = 0; j < len / 2; ++j)
            {
                auto u = x[i + j];
                auto v = x[i + j + len / 2] * w;
                x[i + j]           = u + v;
                x[i + j + len / 2] = u - v;
                w *= wlen;
            }
        }
    }
}

static size_t nextPow2(size_t n)
{
    size_t p = 1;
    while (p < n) p <<= 1;
    return p;
}

// ---- Config ----

struct Config
{
    std::string deviceName;
    std::string redisHost = "127.0.0.1";
    std::string inputKey;
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
    cfg.inputKey       = dev["InputKey"].as<std::string>("");
    cfg.outputKeyMag   = dev["OutputKeyMag"].as<std::string>("");
    cfg.outputKeyPhase = dev["OutputKeyPhase"].as<std::string>("");
    cfg.dataType       = parseDataType(dev["DataType"].as<std::string>("float32"));

    if (cfg.inputKey.empty() || cfg.outputKeyMag.empty() || cfg.outputKeyPhase.empty())
    {
        fprintf(stderr, "Config requires InputKey, OutputKeyMag, and OutputKeyPhase\n");
        return false;
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

    printf("[fft] Device: %s  Redis: %s  DataType: %s\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str(), dataTypeName(cfg.dataType));
    printf("[fft] Input: %s\n", cfg.inputKey.c_str());
    printf("[fft] Mag: %s  Phase: %s\n",
           cfg.outputKeyMag.c_str(), cfg.outputKeyPhase.c_str());

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = cfg.deviceName + ":FFT";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[fft] Failed to connect to Redis\n");
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
            auto samples = deserializeWaveform(*blob, cfg.dataType);

            size_t nOrig = samples.size();
            if (nOrig == 0) continue;

            // zero-pad to next power of two
            size_t N = nextPow2(nOrig);
            std::vector<std::complex<double>> buf(N, {0.0, 0.0});
            for (size_t i = 0; i < nOrig; ++i)
                buf[i] = {samples[i], 0.0};

            fft(buf);

            // output DC … Nyquist (N/2+1 bins)
            size_t nBins = N / 2 + 1;
            std::vector<double> mag(nBins);
            std::vector<double> phase(nBins);
            for (size_t i = 0; i < nBins; ++i)
            {
                mag[i]   = std::abs(buf[i]);
                phase[i] = std::arg(buf[i]);
            }

            serializeAndWrite(redis, cfg.outputKeyMag, mag, cfg.dataType, time);
            serializeAndWrite(redis, cfg.outputKeyPhase, phase, cfg.dataType, time);

            if (++processCount % 200 == 0)
                printf("[fft] processed %lu samples\n",
                       (unsigned long)processCount);
        }
    });

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);
    printf("[fft] Running, waiting for data...\n");

    while (g_running)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    printf("[fft] Shutting down (%lu processed)\n",
           (unsigned long)processCount);
    return 0;
}
