//
//  BPM (Beam Position Monitor) Composite Adapter
//
//  Single-process pipeline that takes one interleaved muxed waveform
//  and produces all derived quantities:
//
//    RAW_MUX_WF ──► demux (N IQ pairs)
//                     │
//                magnitude per channel ──► MAG_CHn, PHASE_CHn
//                     │
//          ┌──────────┼───────────────────────┐
//          │          │                       │
//    position-intensity (H/V)       fft (per channel, optional)
//    baseline-subtract (optional)   filter (per channel, optional)
//    integrate (per channel, optional, multi-window + averaging)
//
//  Everything runs in-process inside the XREAD callback — no
//  inter-service Redis hops, no extra containers.
//
//  Usage: bpm <config.yml>
//

#include "waveform_utils.hpp"
#include "metadata.hpp"
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

// ---- FFT (radix-2 Cooley-Tukey, in-place) ----

static void fftTransform(std::vector<std::complex<double>>& x)
{
    size_t N = x.size();
    if (N <= 1) return;

    for (size_t i = 1, j = 0; i < N; ++i)
    {
        size_t bit = N >> 1;
        for (; j & bit; bit >>= 1) j ^= bit;
        j ^= bit;
        if (i < j) std::swap(x[i], x[j]);
    }

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

// ---- Config structures ----

struct IntegWindow
{
    std::string outputKey;
    double      start = 0.0;
    double      width = 1.0;
    std::string avgKey;
    double      avgSeconds = 300.0;

    // runtime
    double   avgSum   = 0.0;
    uint64_t avgCount = 0;
    std::chrono::steady_clock::time_point avgStart;
};

struct PositionPair
{
    size_t      chA;                // channel index for plate A
    size_t      chB;                // channel index for plate B
    std::string posKey;
    std::string intKey;
    double      positionScale  = 1.0;
    double      intensityScale = 1.0;
};

struct ChannelOpts
{
    // per-channel optional processing (empty key = disabled)
    std::string filterKey;
    double      filterCoeff = 0.1;
    std::string fftMagKey;
    std::string fftPhaseKey;
    std::string baselineSubKey;     // subtract against which channel?
    size_t      baselineChIdx = 0;

    // runtime
    std::vector<double> filterState;
};

struct Config
{
    std::string deviceName;
    std::string redisHost = "127.0.0.1";

    // input
    std::string inputKey;
    DataType    dataTypeIn  = DataType::Float32;
    DataType    dataTypeOut = DataType::Float32;
    size_t      numChannels = 4;    // IQ pairs, so 2*numChannels interleaved

    // per-channel output keys
    std::vector<std::string> magKeys;
    std::vector<std::string> phaseKeys;
    std::vector<ChannelOpts> channelOpts;

    // position-intensity pairs
    std::vector<PositionPair> positions;

    // integrate (operates on a specific channel's magnitude)
    size_t      integChannel = 0;
    size_t      integBaselineChannel = 0;
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

    cfg.deviceName  = dev["DeviceName"].as<std::string>("DEVICE:0001");
    cfg.redisHost   = dev["RedisHost"].as<std::string>("127.0.0.1");
    cfg.inputKey    = dev["InputKey"].as<std::string>("");
    cfg.dataTypeIn  = parseDataType(dev["DataTypeIn"].as<std::string>("float32"));
    cfg.dataTypeOut = parseDataType(dev["DataTypeOut"].as<std::string>("float32"));
    cfg.numChannels = dev["NumChannels"].as<size_t>(4);

    if (cfg.inputKey.empty() || cfg.numChannels == 0)
    {
        fprintf(stderr, "Config requires InputKey and NumChannels > 0\n");
        return false;
    }

    // Per-channel output keys and options
    auto chNode = dev["Channels"];
    if (!chNode || !chNode.IsSequence() ||
        chNode.size() != cfg.numChannels)
    {
        fprintf(stderr, "Config requires Channels list with %zu entries\n",
                cfg.numChannels);
        return false;
    }

    cfg.magKeys.resize(cfg.numChannels);
    cfg.phaseKeys.resize(cfg.numChannels);
    cfg.channelOpts.resize(cfg.numChannels);

    for (size_t i = 0; i < cfg.numChannels; ++i)
    {
        auto cn = chNode[i];
        cfg.magKeys[i]   = cn["MagKey"].as<std::string>("");
        cfg.phaseKeys[i] = cn["PhaseKey"].as<std::string>("");

        if (cfg.magKeys[i].empty())
        {
            fprintf(stderr, "Channel %zu requires MagKey\n", i);
            return false;
        }

        auto& co = cfg.channelOpts[i];
        co.filterKey      = cn["FilterKey"].as<std::string>("");
        co.filterCoeff    = cn["FilterCoeff"].as<double>(0.1);
        co.fftMagKey      = cn["FFTMagKey"].as<std::string>("");
        co.fftPhaseKey    = cn["FFTPhaseKey"].as<std::string>("");
        co.baselineSubKey = cn["BaselineSubKey"].as<std::string>("");
        co.baselineChIdx  = cn["BaselineChIdx"].as<size_t>(0);
    }

    // Position-intensity pairs
    auto posNode = dev["Positions"];
    if (posNode && posNode.IsSequence())
    {
        for (auto pn : posNode)
        {
            PositionPair pp;
            pp.chA            = pn["ChannelA"].as<size_t>(0);
            pp.chB            = pn["ChannelB"].as<size_t>(1);
            pp.posKey         = pn["PositionKey"].as<std::string>("");
            pp.intKey         = pn["IntensityKey"].as<std::string>("");
            pp.positionScale  = pn["PositionScale"].as<double>(1.0);
            pp.intensityScale = pn["IntensityScale"].as<double>(1.0);

            if (pp.posKey.empty() || pp.intKey.empty())
            {
                fprintf(stderr, "Each Position pair requires PositionKey and IntensityKey\n");
                return false;
            }
            if (pp.chA >= cfg.numChannels || pp.chB >= cfg.numChannels)
            {
                fprintf(stderr, "Position channel indices out of range\n");
                return false;
            }
            cfg.positions.push_back(std::move(pp));
        }
    }

    // Integrate
    auto integNode = dev["Integrate"];
    if (integNode)
    {
        cfg.integChannel         = integNode["Channel"].as<size_t>(0);
        cfg.integBaselineChannel = integNode["BaselineChannel"].as<size_t>(0);

        auto winNode = integNode["Windows"];
        if (winNode && winNode.IsSequence())
        {
            for (auto wn : winNode)
            {
                IntegWindow w;
                w.outputKey  = wn["OutputKey"].as<std::string>("");
                w.start      = wn["Start"].as<double>(0.0);
                w.width      = wn["Width"].as<double>(1.0);
                w.avgKey     = wn["AvgKey"].as<std::string>("");
                w.avgSeconds = wn["AvgSeconds"].as<double>(300.0);
                if (w.outputKey.empty())
                {
                    fprintf(stderr, "Each integrate window requires OutputKey\n");
                    return false;
                }
                cfg.windows.push_back(std::move(w));
            }
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

    printf("[bpm] Device: %s  Redis: %s  Channels: %zu\n",
           cfg.deviceName.c_str(), cfg.redisHost.c_str(), nCh);
    printf("[bpm] Input: %s (%s → %s)  Mux width: %zu\n",
           cfg.inputKey.c_str(), dataTypeName(cfg.dataTypeIn),
           dataTypeName(cfg.dataTypeOut), nCh * 2);
    for (size_t i = 0; i < nCh; ++i)
    {
        printf("[bpm]   CH%zu: mag=%s", i, cfg.magKeys[i].c_str());
        if (!cfg.phaseKeys[i].empty())   printf("  phase=%s", cfg.phaseKeys[i].c_str());
        if (!cfg.channelOpts[i].filterKey.empty())
            printf("  filter=%s(%.3f)", cfg.channelOpts[i].filterKey.c_str(),
                   cfg.channelOpts[i].filterCoeff);
        if (!cfg.channelOpts[i].fftMagKey.empty())
            printf("  fft=%s", cfg.channelOpts[i].fftMagKey.c_str());
        if (!cfg.channelOpts[i].baselineSubKey.empty())
            printf("  bsub=%s(ch%zu)", cfg.channelOpts[i].baselineSubKey.c_str(),
                   cfg.channelOpts[i].baselineChIdx);
        printf("\n");
    }
    for (size_t i = 0; i < cfg.positions.size(); ++i)
        printf("[bpm]   POS[%zu]: ch%zu vs ch%zu → %s / %s  (scale %.4g / %.4g)\n",
               i, cfg.positions[i].chA, cfg.positions[i].chB,
               cfg.positions[i].posKey.c_str(), cfg.positions[i].intKey.c_str(),
               cfg.positions[i].positionScale, cfg.positions[i].intensityScale);
    if (!cfg.windows.empty())
        printf("[bpm]   Integrate: ch%zu (baseline ch%zu), %zu windows\n",
               cfg.integChannel, cfg.integBaselineChannel, cfg.windows.size());

    RAL_Options opts;
    opts.host    = cfg.redisHost;
    opts.workers = 2;
    opts.readers = 1;
    opts.dogname = cfg.deviceName + ":BPM";

    RedisAdapterLite redis(cfg.deviceName, opts);
    if (!redis.connected())
    {
        fprintf(stderr, "[bpm] Failed to connect to Redis\n");
        return 1;
    }

    // init averaging clocks
    auto tNow = std::chrono::steady_clock::now();
    for (auto& w : cfg.windows)
        w.avgStart = tNow;

    // ---- Publish metadata ----
    {
        std::vector<ChannelMetaEntry> metaCh;
        for (size_t i = 0; i < nCh; ++i)
        {
            metaCh.push_back({cfg.magKeys[i], "waveform",
                              "Ch" + std::to_string(i) + " Magnitude", ""});
            if (!cfg.phaseKeys[i].empty())
                metaCh.push_back({cfg.phaseKeys[i], "waveform",
                                  "Ch" + std::to_string(i) + " Phase", "rad"});
            if (!cfg.channelOpts[i].filterKey.empty())
                metaCh.push_back({cfg.channelOpts[i].filterKey, "waveform",
                                  "Ch" + std::to_string(i) + " Filtered", ""});
            if (!cfg.channelOpts[i].fftMagKey.empty())
                metaCh.push_back({cfg.channelOpts[i].fftMagKey, "waveform",
                                  "FFT Magnitude", ""});
            if (!cfg.channelOpts[i].fftPhaseKey.empty())
                metaCh.push_back({cfg.channelOpts[i].fftPhaseKey, "waveform",
                                  "FFT Phase", "rad"});
            if (!cfg.channelOpts[i].baselineSubKey.empty())
                metaCh.push_back({cfg.channelOpts[i].baselineSubKey, "waveform",
                                  "Baseline Subtracted", ""});
        }
        for (auto& pp : cfg.positions)
        {
            metaCh.push_back({pp.posKey, "waveform", pp.posKey + " Position", "mm"});
            metaCh.push_back({pp.intKey, "waveform", pp.intKey + " Intensity", ""});
        }
        for (auto& win : cfg.windows)
        {
            metaCh.push_back({win.outputKey, "scalar", win.outputKey + " Integral", ""});
            if (!win.avgKey.empty())
                metaCh.push_back({win.avgKey, "scalar", win.avgKey + " Avg", ""});
        }

        std::vector<ControlMetaEntry> ctrls = {
            {"BEAM_X_S",        "Beam X (mm)",      2.0},
            {"BEAM_Y_S",        "Beam Y (mm)",     -1.0},
            {"BEAM_INTENSITY_S","Beam Intensity",    1.0},
            {"NOISE_S",         "Noise",             0.02},
            {"GATE_ENABLE_S",   "Gate Enable",       1.0},
            {"GATE_START_S",    "Gate Start",        0.2},
            {"GATE_WIDTH_S",    "Gate Width",        0.4},
            {"MODE_CHANGE_S",   "Mode (0=IQ,1=ADC)",0.0},
        };

        publishMetadata(redis, "bpm", cfg.deviceName,
                        dataTypeName(cfg.dataTypeOut), metaCh, ctrls);
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

            size_t muxWidth    = nCh * 2;  // IQ pairs
            size_t perChannel  = samples.size() / muxWidth;
            if (perChannel == 0) continue;

            // ---- Stage 1: Demux ----
            std::vector<std::vector<double>> iCh(nCh), qCh(nCh);
            for (size_t ch = 0; ch < nCh; ++ch)
            {
                iCh[ch].resize(perChannel);
                qCh[ch].resize(perChannel);
            }
            for (size_t s = 0; s < perChannel; ++s)
            {
                size_t base = s * muxWidth;
                for (size_t ch = 0; ch < nCh; ++ch)
                {
                    iCh[ch][s] = samples[base + ch * 2];
                    qCh[ch][s] = samples[base + ch * 2 + 1];
                }
            }

            // ---- Stage 2: Magnitude + Phase per channel ----
            std::vector<std::vector<double>> mag(nCh), phase(nCh);
            for (size_t ch = 0; ch < nCh; ++ch)
            {
                mag[ch].resize(perChannel);
                phase[ch].resize(perChannel);
                for (size_t i = 0; i < perChannel; ++i)
                {
                    double iv = iCh[ch][i];
                    double qv = qCh[ch][i];
                    mag[ch][i]   = std::sqrt(iv * iv + qv * qv);
                    phase[ch][i] = std::atan2(qv, iv);
                }

                serializeAndWrite(redis, cfg.magKeys[ch], mag[ch], cfg.dataTypeOut, time);
                if (!cfg.phaseKeys[ch].empty())
                    serializeAndWrite(redis, cfg.phaseKeys[ch], phase[ch], cfg.dataTypeOut, time);
            }

            // ---- Stage 3a: Per-channel optional processing ----
            for (size_t ch = 0; ch < nCh; ++ch)
            {
                auto& co = cfg.channelOpts[ch];

                // Filter (EMA)
                if (!co.filterKey.empty())
                {
                    if (co.filterState.size() != perChannel)
                        co.filterState.assign(perChannel, 0.0);
                    double a = co.filterCoeff;
                    for (size_t i = 0; i < perChannel; ++i)
                        co.filterState[i] = a * mag[ch][i] + (1.0 - a) * co.filterState[i];
                    serializeAndWrite(redis, co.filterKey, co.filterState, cfg.dataTypeOut, time);
                }

                // FFT
                if (!co.fftMagKey.empty())
                {
                    size_t N = nextPow2(perChannel);
                    std::vector<std::complex<double>> buf(N, {0.0, 0.0});
                    for (size_t i = 0; i < perChannel; ++i)
                        buf[i] = {mag[ch][i], 0.0};
                    fftTransform(buf);

                    size_t nBins = N / 2 + 1;
                    std::vector<double> fMag(nBins), fPh(nBins);
                    for (size_t i = 0; i < nBins; ++i)
                    {
                        fMag[i] = std::abs(buf[i]);
                        fPh[i]  = std::arg(buf[i]);
                    }
                    serializeAndWrite(redis, co.fftMagKey, fMag, cfg.dataTypeOut, time);
                    if (!co.fftPhaseKey.empty())
                        serializeAndWrite(redis, co.fftPhaseKey, fPh, cfg.dataTypeOut, time);
                }

                // Baseline subtract
                if (!co.baselineSubKey.empty() && co.baselineChIdx < nCh)
                {
                    auto& ref = mag[co.baselineChIdx];
                    std::vector<double> diff(perChannel);
                    for (size_t i = 0; i < perChannel; ++i)
                        diff[i] = mag[ch][i] - (i < ref.size() ? ref[i] : 0.0);
                    serializeAndWrite(redis, co.baselineSubKey, diff, cfg.dataTypeOut, time);
                }
            }

            // ---- Stage 3b: Position-intensity pairs ----
            for (auto& pp : cfg.positions)
            {
                auto& a = mag[pp.chA];
                auto& b = mag[pp.chB];
                std::vector<double> pos(perChannel), inten(perChannel);
                for (size_t i = 0; i < perChannel; ++i)
                {
                    double av = a[i];
                    double bv = (i < b.size()) ? b[i] : 0.0;
                    double sum = av + bv;
                    pos[i]   = (sum != 0.0) ? pp.positionScale * (av - bv) / sum : 0.0;
                    inten[i] = pp.intensityScale * sum;
                }
                serializeAndWrite(redis, pp.posKey, pos, cfg.dataTypeOut, time);
                serializeAndWrite(redis, pp.intKey, inten, cfg.dataTypeOut, time);
            }

            // ---- Stage 3c: Integrate ----
            if (!cfg.windows.empty() &&
                cfg.integChannel < nCh &&
                cfg.integBaselineChannel < nCh)
            {
                auto& a = mag[cfg.integChannel];
                auto& b = mag[cfg.integBaselineChannel];
                auto tNow = std::chrono::steady_clock::now();

                for (auto& win : cfg.windows)
                {
                    size_t iStart = static_cast<size_t>(win.start * perChannel);
                    size_t iEnd   = static_cast<size_t>((win.start + win.width) * perChannel);
                    if (iStart >= perChannel) iStart = perChannel - 1;
                    if (iEnd > perChannel)    iEnd = perChannel;

                    double sum = 0.0;
                    for (size_t i = iStart; i < iEnd; ++i)
                        sum += a[i] - (i < b.size() ? b[i] : 0.0);

                    RAL_AddArgs tArgs;
                    tArgs.time = time;
                    redis.addDouble(win.outputKey, sum, tArgs);

                    if (!win.avgKey.empty())
                    {
                        win.avgSum += sum;
                        win.avgCount++;
                        double elapsed = std::chrono::duration<double>(
                            tNow - win.avgStart).count();
                        if (elapsed >= win.avgSeconds)
                        {
                            redis.addDouble(win.avgKey,
                                win.avgSum / static_cast<double>(win.avgCount));
                            win.avgSum   = 0.0;
                            win.avgCount = 0;
                            win.avgStart = tNow;
                        }
                    }
                }
            }

            if (++processCount % 200 == 0)
                printf("[bpm] processed %lu triggers\n",
                       (unsigned long)processCount);
        }
    });

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);
    printf("[bpm] Running, waiting for data...\n");

    while (g_running)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    printf("[bpm] Shutting down (%lu processed)\n",
           (unsigned long)processCount);
    return 0;
}
