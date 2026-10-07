#pragma once

#include <charconv>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

// Connection-free wire containers and validation shared by real and mock builds.
class RedisStreamData {
public:
  using Attrs = std::unordered_map<std::string, std::string>;
  using StreamEntry = std::pair<std::string, Attrs>;
  using StreamBatch = std::vector<StreamEntry>;
  using StreamCallback = std::function<void(const std::string&, const std::string&, const StreamBatch&)>;

  static int compareStreamIds(const std::string& lhs, const std::string& rhs) {
    const auto parse = [](const std::string& id) {
      const auto dash = id.find('-');
      const auto firstEnd = dash == std::string::npos ? id.size() : dash;
      std::pair<uint64_t, uint64_t> parts{};
      const auto first = std::from_chars(id.data(), id.data() + firstEnd, parts.first);
      std::from_chars_result second{id.data() + id.size(), {}};
      if (dash != std::string::npos) second = std::from_chars(id.data() + dash + 1, id.data() + id.size(), parts.second);
      if (first.ec != std::errc{} || first.ptr != id.data() + firstEnd ||
          second.ec != std::errc{} || second.ptr != id.data() + id.size())
        throw std::invalid_argument("invalid Redis stream ID: " + id);
      return parts;
    };
    const auto a = parse(lhs), b = parse(rhs);
    return a < b ? -1 : b < a ? 1 : 0;
  }

  template<typename T>
  [[nodiscard]] static bool decodeScalar(const Attrs& fields, T& output,
                           size_t maxBytes = std::numeric_limits<size_t>::max(),
                           const std::string& field = DEFAULT_FIELD) {
    const auto found = fields.find(field);
    if (found == fields.end() || found->second.size() > maxBytes) return false;
    const auto& bytes = found->second;
    if constexpr (std::is_same_v<T, std::string>) {
      output = bytes;
    } else {
      static_assert(std::is_trivial_v<T>, "scalar must be trivial or string");
      if (bytes.size() != sizeof(T)) return false;
      if constexpr (std::is_same_v<T, bool>) return decodeBoolean(bytes.data(), output);
      else std::memcpy(&output, bytes.data(), sizeof(T));
    }
    return true;
  }

  template<typename T>
  [[nodiscard]] static bool decodeArray(const Attrs& fields, std::vector<T>& output,
                          size_t maxBytes = std::numeric_limits<size_t>::max(),
                           const std::string& field = DEFAULT_FIELD) {
    static_assert(std::is_trivial_v<T>, "array element must be trivial");
    const auto found = fields.find(field);
    if (found == fields.end()) return false;
    const auto& bytes = found->second;
    if (bytes.size() > maxBytes || bytes.size() % sizeof(T) != 0u) return false;
    if constexpr (!std::is_same_v<T, bool>) {
      // Validation is complete; trivial resize has the strong exception guarantee.
      output.resize(bytes.size() / sizeof(T));
      if (!bytes.empty()) std::memcpy(output.data(), bytes.data(), bytes.size());
      return true;
    } else {
      std::vector<T> decoded(bytes.size() / sizeof(T));
      for (size_t index = 0; index < decoded.size(); ++index) {
        bool value;
        if (!decodeBoolean(bytes.data() + index * sizeof(bool), value)) return false;
        decoded[index] = value;
      }
      output.swap(decoded);
      return true;
    }
  }

protected:
  inline static const std::string DEFAULT_FIELD = "_";

private:
  static bool decodeBoolean(const char* bytes, bool& output) {
    const bool falseValue = false, trueValue = true;
    if (std::memcmp(bytes, &falseValue, sizeof(bool)) == 0) output = false;
    else if (std::memcmp(bytes, &trueValue, sizeof(bool)) == 0) output = true;
    else return false;
    return true;
  }
};
