#pragma once
//
//  Metadata publishing for instrument adapters
//
//  Publishes a JSON metadata blob to {DeviceName}:META so that
//  inst-tui and other tools can discover devices and their channels.
//

#include "RedisAdapterLite.hpp"

#include <string>
#include <vector>

struct ChannelMetaEntry
{
    std::string key;
    std::string kind;   // "waveform" or "scalar"
    std::string label;
    std::string unit;
};

struct ControlMetaEntry
{
    std::string key;
    std::string label;
    double      defaultValue;
};

inline void publishMetadata(
    RedisAdapterLite& redis,
    const std::string& deviceType,
    const std::string& deviceName,
    const std::string& dataType,
    const std::vector<ChannelMetaEntry>& channels,
    const std::vector<ControlMetaEntry>& controls = {})
{
    // Build JSON manually (no dependency on a JSON library)
    std::string json = "{";
    json += "\"type\":\"" + deviceType + "\",";
    json += "\"device\":\"" + deviceName + "\",";
    json += "\"data_type\":\"" + dataType + "\",";

    // channels array
    json += "\"channels\":[";
    for (size_t i = 0; i < channels.size(); ++i)
    {
        if (i > 0) json += ",";
        json += "{\"key\":\"" + channels[i].key + "\",";
        json += "\"kind\":\"" + channels[i].kind + "\",";
        json += "\"label\":\"" + channels[i].label + "\"";
        if (!channels[i].unit.empty())
            json += ",\"unit\":\"" + channels[i].unit + "\"";
        json += "}";
    }
    json += "],";

    // controls array
    json += "\"controls\":[";
    for (size_t i = 0; i < controls.size(); ++i)
    {
        if (i > 0) json += ",";
        char buf[64];
        snprintf(buf, sizeof(buf), "%.6g", controls[i].defaultValue);
        json += "{\"key\":\"" + controls[i].key + "\",";
        json += "\"label\":\"" + controls[i].label + "\",";
        json += "\"default_value\":" + std::string(buf) + "}";
    }
    json += "]}";

    redis.addString("META", json);
    printf("[meta] Published metadata for %s (%zu channels, %zu controls)\n",
           deviceName.c_str(), channels.size(), controls.size());
}
