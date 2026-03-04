//
//  RedisAdapter.hpp
//
//  Backward-compatible wrapper around RedisAdapterLite.
//  Provides the old template-based API for drop-in replacement.
//
//  Include this header instead of RedisAdapterLite.hpp to use
//  the original RedisAdapter API with the lite backend.
//

#pragma once

#include "RedisAdapterLite.hpp"
#include <cstring>
#include <type_traits>

//--- Old type aliases ---

using RA_Time = RAL_Time;
inline constexpr RA_Time RA_NOT_CONNECTED(-1);

#ifndef RA_VERSION
#define RA_VERSION RAL_VERSION
#endif

//--- Old argument structs ---

struct RA_ArgsGet
{ std::string baseKey; RA_Time minTime; RA_Time maxTime; uint32_t count = 1; };

struct RA_ArgsAdd
{ RA_Time time; uint32_t trim = 1; };

//--- Old options layout (with nested connection struct) ---

struct RA_Options
{
  struct Connection
  {
    std::string path;
    std::string host = "127.0.0.1";
    std::string user = "default";
    std::string password;
    uint32_t timeout = 500;
    uint16_t port = 6379;
    uint16_t size = 5;          // pool size — ignored by lite
  } cxn;

  std::string dogname;
  uint16_t workers = 1;
  uint16_t readers = 1;
};

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  class RedisAdapter
//
//  Template-based compatibility wrapper delegating to RedisAdapterLite.
//
class RedisAdapter
{
public:
  using Attrs = ::Attrs;

  template<typename T> using TimeVal     = std::pair<RA_Time, T>;
  template<typename T> using TimeValList = std::vector<TimeVal<T>>;

  using ListenSubFn = SubCallback;

  template<typename T>
  using ReaderSubFn = std::function<void(const std::string& baseKey,
                                          const std::string& subKey,
                                          const TimeValList<T>& data)>;

  //--- Construction / Destruction ---

  RedisAdapter(const std::string& baseKey, const RA_Options& options = {})
    : _lite(baseKey, to_ral_options(options)) {}

  RedisAdapter(const RedisAdapter&) = delete;
  RedisAdapter& operator=(const RedisAdapter&) = delete;

  virtual ~RedisAdapter() = default;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getValues / getLists  — forward range (count ignored)
  //
  template<typename T>
  TimeValList<T> getValues(const std::string& subKey, const RA_ArgsGet& args = {})
  { return get_range_dispatch<T>(subKey, {args.baseKey, args.minTime, args.maxTime, 0}); }

  template<typename T>
  TimeValList<std::vector<T>> getLists(const std::string& subKey, const RA_ArgsGet& args = {})
  {
    static_assert(std::is_trivial_v<T>, "T must be trivial for list operations");
    return get_list_forward<T>(subKey, {args.baseKey, args.minTime, args.maxTime, 0});
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getValuesBefore / getListsBefore  — reverse (minTime ignored)
  //
  template<typename T>
  TimeValList<T> getValuesBefore(const std::string& subKey, const RA_ArgsGet& args = {})
  { return get_reverse_dispatch<T>(subKey, {args.baseKey, {}, args.maxTime, args.count}); }

  template<typename T>
  TimeValList<std::vector<T>> getListsBefore(const std::string& subKey, const RA_ArgsGet& args = {})
  {
    static_assert(std::is_trivial_v<T>, "T must be trivial for list operations");
    return get_list_reverse<T>(subKey, {args.baseKey, {}, args.maxTime, args.count});
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getValuesAfter / getListsAfter  — forward (maxTime ignored)
  //
  template<typename T>
  TimeValList<T> getValuesAfter(const std::string& subKey, const RA_ArgsGet& args = {})
  { return get_range_dispatch<T>(subKey, {args.baseKey, args.minTime, {}, args.count}); }

  template<typename T>
  TimeValList<std::vector<T>> getListsAfter(const std::string& subKey, const RA_ArgsGet& args = {})
  {
    static_assert(std::is_trivial_v<T>, "T must be trivial for list operations");
    return get_list_forward<T>(subKey, {args.baseKey, args.minTime, {}, args.count});
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  getSingleValue / getSingleList
  //
  template<typename T>
  RA_Time getSingleValue(const std::string& subKey, T& dest, const RA_ArgsGet& args = {})
  {
    RAL_GetArgs ra{args.baseKey, {}, args.maxTime};
    if constexpr (std::is_same_v<T, std::string>) {
      return _lite.getString(subKey, dest, ra);
    } else if constexpr (std::is_same_v<T, Attrs>) {
      return _lite.getAttrs(subKey, dest, ra);
    } else {
      static_assert(std::is_trivial_v<T>, "T must be trivial, string, or Attrs");
      std::vector<uint8_t> blob;
      RAL_Time t = _lite.getBlob(subKey, blob, ra);
      if (t.ok() && blob.size() == sizeof(T))
        std::memcpy(&dest, blob.data(), sizeof(T));
      return t;
    }
  }

  template<typename T>
  RA_Time getSingleList(const std::string& subKey, std::vector<T>& dest, const RA_ArgsGet& args = {})
  {
    static_assert(std::is_trivial_v<T>, "T must be trivial for list operations");
    std::vector<uint8_t> blob;
    RAL_Time t = _lite.getBlob(subKey, blob, {args.baseKey, {}, args.maxTime});
    if (t.ok() && blob.size() >= sizeof(T)) {
      size_t n = blob.size() / sizeof(T);
      dest.resize(n);
      std::memcpy(dest.data(), blob.data(), n * sizeof(T));
    }
    return t;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addSingleValue / addSingleDouble / addSingleList
  //
  template<typename T>
  RA_Time addSingleValue(const std::string& subKey, const T& data, const RA_ArgsAdd& args = {})
  {
    static_assert(!std::is_same_v<T, double>,
                  "use addSingleDouble for double or 'f' suffix for float literal");
    RAL_AddArgs ra{args.time, args.trim};
    if constexpr (std::is_same_v<T, std::string>) {
      return _lite.addString(subKey, data, ra);
    } else if constexpr (std::is_same_v<T, Attrs>) {
      return _lite.addAttrs(subKey, data, ra);
    } else {
      static_assert(std::is_trivial_v<T>, "T must be trivial, string, or Attrs");
      return _lite.addBlob(subKey, &data, sizeof(T), ra);
    }
  }

  RA_Time addSingleDouble(const std::string& subKey, double data, const RA_ArgsAdd& args = {})
  { return _lite.addDouble(subKey, data, {args.time, args.trim}); }

  template<typename T>
  RA_Time addSingleList(const std::string& subKey, const std::vector<T>& data,
                         const RA_ArgsAdd& args = {})
  {
    static_assert(std::is_trivial_v<T>, "T must be trivial for list operations");
    return _lite.addBlob(subKey, data.data(), data.size() * sizeof(T), {args.time, args.trim});
  }

  template<template<typename, size_t> class C, typename T, size_t S>
  RA_Time addSingleList(const std::string& subKey, const C<T, S>& data,
                         const RA_ArgsAdd& args = {})
  {
    static_assert(std::is_trivial_v<T>, "T must be trivial for list operations");
    return _lite.addBlob(subKey, data.data(), data.size() * sizeof(T), {args.time, args.trim});
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  addValues / addLists  — bulk
  //
  template<typename T>
  std::vector<RA_Time> addValues(const std::string& subKey, const TimeValList<T>& data,
                                  uint32_t trim = 1)
  {
    if constexpr (std::is_same_v<T, std::string>) {
      return _lite.addStrings(subKey, data, trim);
    } else if constexpr (std::is_same_v<T, Attrs>) {
      return _lite.addAttrsBatch(subKey, data, trim);
    } else if constexpr (std::is_same_v<T, double>) {
      return _lite.addDoubles(subKey, data, trim);
    } else if constexpr (std::is_same_v<T, int64_t>) {
      return _lite.addInts(subKey, data, trim);
    } else {
      static_assert(std::is_trivial_v<T>, "T must be trivial, string, or Attrs");
      TimeBlobList blobs;
      blobs.reserve(data.size());
      for (const auto& [time, val] : data) {
        std::vector<uint8_t> blob(sizeof(T));
        std::memcpy(blob.data(), &val, sizeof(T));
        blobs.push_back({time, std::move(blob)});
      }
      return _lite.addBlobs(subKey, blobs, trim);
    }
  }

  template<typename T>
  std::vector<RA_Time> addLists(const std::string& subKey,
                                 const TimeValList<std::vector<T>>& data, uint32_t trim = 1)
  {
    static_assert(std::is_trivial_v<T>, "T must be trivial for list operations");
    TimeBlobList blobs;
    blobs.reserve(data.size());
    for (const auto& [time, vec] : data) {
      std::vector<uint8_t> blob(vec.size() * sizeof(T));
      std::memcpy(blob.data(), vec.data(), blob.size());
      blobs.push_back({time, std::move(blob)});
    }
    return _lite.addBlobs(subKey, blobs, trim);
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  connected
  //
  bool connected() { return _lite.connected(); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Watchdog
  //
  bool addWatchdog(const std::string& dogname, uint32_t expiration)
  { return _lite.addWatchdog(dogname, expiration); }

  bool petWatchdog(const std::string& dogname, uint32_t expiration)
  { return _lite.petWatchdog(dogname, expiration); }

  std::vector<std::string> getWatchdogs() { return _lite.getWatchdogs(); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Key management
  //
  bool copy(const std::string& srcSubKey, const std::string& dstSubKey,
            const std::string& baseKey = "")
  { return _lite.copy(srcSubKey, dstSubKey, baseKey); }

  bool rename(const std::string& subKeySrc, const std::string& subKeyDst)
  { return _lite.rename(subKeySrc, subKeyDst); }

  bool del(const std::string& subKey) { return _lite.del(subKey); }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Pub/Sub
  //
  bool publish(const std::string& subKey, const std::string& message,
               const std::string& baseKey = "")
  { return _lite.publish(subKey, message, baseKey); }

  bool subscribe(const std::string& subKey, ListenSubFn func, const std::string& baseKey = "")
  { return _lite.subscribe(subKey, std::move(func), baseKey); }

  bool unsubscribe(const std::string& subKey, const std::string& baseKey = "")
  { return _lite.unsubscribe(subKey, baseKey); }

  // psubscribe is not supported by the lite backend
  bool psubscribe(const std::string&, ListenSubFn, const std::string& = "")
  { return false; }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  Stream Readers
  //
  bool setDeferReaders(bool defer) { return _lite.setDeferReaders(defer); }

  template<typename T>
  bool addValuesReader(const std::string& subKey, ReaderSubFn<T> func,
                        const std::string& baseKey = "")
  { return _lite.addReader(subKey, make_values_reader<T>(std::move(func)), baseKey); }

  template<typename T>
  bool addListsReader(const std::string& subKey, ReaderSubFn<std::vector<T>> func,
                       const std::string& baseKey = "")
  { return _lite.addReader(subKey, make_lists_reader<T>(std::move(func)), baseKey); }

  bool removeReader(const std::string& subKey, const std::string& baseKey = "")
  { return _lite.removeReader(subKey, baseKey); }

  // Generic readers — approximate via lite's schema-based reader
  bool addGenericReader(const std::string& key, ReaderSubFn<Attrs> func)
  {
    return _lite.addReader(key,
      [f = std::move(func)](const std::string& b, const std::string& s, const TimeAttrsList& raw) {
        TimeValList<Attrs> typed;
        typed.reserve(raw.size());
        for (const auto& entry : raw) typed.push_back(entry);
        f(b, s, typed);
      });
  }

  bool removeGenericReader(const std::string& key)
  { return _lite.removeReader(key); }

private:
  RedisAdapterLite _lite;

  //--- Options conversion ---
  static RAL_Options to_ral_options(const RA_Options& o)
  {
    return { o.cxn.path, o.cxn.host, o.cxn.user, o.cxn.password,
             o.cxn.timeout, o.cxn.port, o.dogname, o.workers, o.readers };
  }

  //--- Forward range dispatch ---
  template<typename T>
  TimeValList<T> get_range_dispatch(const std::string& subKey, const RAL_GetArgs& ra)
  {
    if constexpr (std::is_same_v<T, std::string>)  return _lite.getStrings(subKey, ra);
    else if constexpr (std::is_same_v<T, Attrs>)   return _lite.getAttrsRange(subKey, ra);
    else if constexpr (std::is_same_v<T, double>)   return _lite.getDoubles(subKey, ra);
    else if constexpr (std::is_same_v<T, int64_t>)  return _lite.getInts(subKey, ra);
    else {
      static_assert(std::is_trivial_v<T>, "T must be trivial, string, or Attrs");
      return blobs_to_vals<T>(_lite.getBlobs(subKey, ra));
    }
  }

  //--- Reverse range dispatch ---
  template<typename T>
  TimeValList<T> get_reverse_dispatch(const std::string& subKey, const RAL_GetArgs& ra)
  {
    if constexpr (std::is_same_v<T, std::string>)  return _lite.getStringsBefore(subKey, ra);
    else if constexpr (std::is_same_v<T, Attrs>)   return _lite.getAttrsBefore(subKey, ra);
    else if constexpr (std::is_same_v<T, double>)   return _lite.getDoublesBefore(subKey, ra);
    else if constexpr (std::is_same_v<T, int64_t>)  return _lite.getIntsBefore(subKey, ra);
    else {
      static_assert(std::is_trivial_v<T>, "T must be trivial, string, or Attrs");
      return blobs_to_vals<T>(_lite.getBlobsBefore(subKey, ra));
    }
  }

  //--- List range helpers ---
  template<typename T>
  TimeValList<std::vector<T>> get_list_forward(const std::string& subKey, const RAL_GetArgs& ra)
  { return blobs_to_lists<T>(_lite.getBlobs(subKey, ra)); }

  template<typename T>
  TimeValList<std::vector<T>> get_list_reverse(const std::string& subKey, const RAL_GetArgs& ra)
  { return blobs_to_lists<T>(_lite.getBlobsBefore(subKey, ra)); }

  //--- Blob → typed conversion ---
  template<typename T>
  static TimeValList<T> blobs_to_vals(const TimeBlobList& blobs)
  {
    TimeValList<T> out;
    out.reserve(blobs.size());
    for (const auto& [t, b] : blobs) {
      if (b.size() == sizeof(T)) {
        T v; std::memcpy(&v, b.data(), sizeof(T));
        out.push_back({t, v});
      }
    }
    return out;
  }

  template<typename T>
  static TimeValList<std::vector<T>> blobs_to_lists(const TimeBlobList& blobs)
  {
    TimeValList<std::vector<T>> out;
    out.reserve(blobs.size());
    for (const auto& [t, b] : blobs) {
      if (b.size() >= sizeof(T)) {
        size_t n = b.size() / sizeof(T);
        std::vector<T> v(n);
        std::memcpy(v.data(), b.data(), n * sizeof(T));
        out.push_back({t, std::move(v)});
      }
    }
    return out;
  }

  //--- Reader callback wrappers ---
  template<typename T>
  static ReaderCallback make_values_reader(ReaderSubFn<T> func)
  {
    return [f = std::move(func)](const std::string& base, const std::string& sub,
                                  const TimeAttrsList& raw)
    {
      TimeValList<T> typed;
      typed.reserve(raw.size());
      for (const auto& [time, attrs] : raw)
      {
        if constexpr (std::is_same_v<T, std::string>) {
          auto v = ral_to_string(attrs); if (v) typed.push_back({time, *v});
        } else if constexpr (std::is_same_v<T, Attrs>) {
          typed.push_back({time, attrs});
        } else if constexpr (std::is_same_v<T, double>) {
          auto v = ral_to_double(attrs); if (v) typed.push_back({time, *v});
        } else if constexpr (std::is_same_v<T, int64_t>) {
          auto v = ral_to_int(attrs); if (v) typed.push_back({time, *v});
        } else {
          static_assert(std::is_trivial_v<T>, "T must be trivial, string, or Attrs");
          auto it = attrs.find(DEFAULT_FIELD);
          if (it != attrs.end() && it->second.size() == sizeof(T)) {
            T v; std::memcpy(&v, it->second.data(), sizeof(T));
            typed.push_back({time, v});
          }
        }
      }
      f(base, sub, typed);
    };
  }

  template<typename T>
  static ReaderCallback make_lists_reader(ReaderSubFn<std::vector<T>> func)
  {
    static_assert(std::is_trivial_v<T>, "T must be trivial for list reader");
    return [f = std::move(func)](const std::string& base, const std::string& sub,
                                  const TimeAttrsList& raw)
    {
      TimeValList<std::vector<T>> typed;
      typed.reserve(raw.size());
      for (const auto& [time, attrs] : raw)
      {
        auto it = attrs.find(DEFAULT_FIELD);
        if (it != attrs.end() && it->second.size() >= sizeof(T)) {
          size_t n = it->second.size() / sizeof(T);
          std::vector<T> v(n);
          std::memcpy(v.data(), it->second.data(), n * sizeof(T));
          typed.push_back({time, std::move(v)});
        }
      }
      f(base, sub, typed);
    };
  }
};
