//
//  RedisAdapterTempl.hpp
//
//  This file contains the RedisAdapter method templates

#pragma once

#include "RedisAdapter.hpp"

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  Helper functions for getting DEFAULT_FIELD in Attrs
//
template<typename T> auto RedisAdapter::default_field_value(const Attrs& attrs) const
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  swr::Optional<T> ret;
  if (attrs.count(DEFAULT_FIELD)) ret = *(const T*)attrs.at(DEFAULT_FIELD).data();
  return ret;
}
//  string specialization
template<> inline auto RedisAdapter::default_field_value<std::string>(const Attrs& attrs) const
{
  std::string ret;
  if (attrs.count(DEFAULT_FIELD)) ret = attrs.at(DEFAULT_FIELD);
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  Helper functions for setting DEFAULT_FIELD in Attrs
//
template<typename T> RedisAdapter::Attrs RedisAdapter::default_field_attrs(const T& data) const
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  return {{ DEFAULT_FIELD, std::string((const char*)&data, sizeof(T)) }};
}
//  string specialization
template<> inline RedisAdapter::Attrs RedisAdapter::default_field_attrs(const std::string& data) const
{
  return {{ DEFAULT_FIELD, data }};
}
//  overload for buffer as ptr, size
template<typename T> RedisAdapter::Attrs RedisAdapter::default_field_attrs(const T* data, size_t size) const
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  return {{ DEFAULT_FIELD, data ? std::string((const char*)data, size * sizeof(T)) : "" }};
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_forward_stream_helper : get data as type T (T is trivial, string or Attrs)
//                            that occurred after a specified time
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    minTime : lowest time to get data for
//    maxTime : highest time to get data for
//    count   : max number of items to get
//    return  : TimeValList of TimeVal<T>
//
template<typename T> RedisAdapter::TimeValList<T>
RedisAdapter::get_forward_stream_helper(const std::string& baseKey, const std::string& subKey,
                                        RA_Time minTime, RA_Time maxTime, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  std::string key = build_key(subKey, baseKey);
  std::string minID = minTime.id_or_min();
  std::string maxID = maxTime.id_or_max();
  ItemStream raw;

  if (count) { reconnect(_redis.xrange(key, minID, maxID, count, std::back_inserter(raw))); }
  else       { reconnect(_redis.xrange(key, minID, maxID, std::back_inserter(raw))); }

  TimeValList<T> ret;
  TimeVal<T> retItem;
  for (const auto& rawItem : raw)
  {
    swr::Optional<T> maybe = default_field_value<T>(rawItem.second);
    if (maybe)
    {
      retItem.first = RA_Time(rawItem.first);
      retItem.second = maybe.value();
      ret.push_back(retItem);
    }
  }
  return ret;
}
//  Attrs specialization
template<> inline RedisAdapter::TimeValList<RedisAdapter::Attrs>
RedisAdapter::get_forward_stream_helper(const std::string& baseKey, const std::string& subKey,
                                        RA_Time minTime, RA_Time maxTime, uint32_t count)
{
  std::string key = build_key(subKey, baseKey);
  std::string minID = minTime.id_or_min();
  std::string maxID = maxTime.id_or_max();
  ItemStream raw;

  if (count) { reconnect(_redis.xrange(key, minID, maxID, count, std::back_inserter(raw))); }
  else       { reconnect(_redis.xrange(key, minID, maxID, std::back_inserter(raw))); }

  TimeValList<Attrs> ret;
  TimeVal<Attrs> retItem;
  for (const auto& rawItem : raw)
  {
    retItem.first = RA_Time(rawItem.first);
    retItem.second = rawItem.second;
    ret.push_back(retItem);
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_forward_stream_list_helper : get data as type vector<T> (T is trivial)
//                                 that occurred after a specified time
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    minID   : lowest time to get data for
//    maxID   : highest time to get data for
//    count   : max number of items to get
//    return  : TimeValList of TimeVal<vector<T>>
//
template<typename T> RedisAdapter::TimeValList<std::vector<T>>
RedisAdapter::get_forward_stream_list_helper(const std::string& baseKey, const std::string& subKey,
                                             RA_Time minTime, RA_Time maxTime, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  std::string key = build_key(subKey, baseKey);
  std::string minID = minTime.id_or_min();
  std::string maxID = maxTime.id_or_max();
  ItemStream raw;

  if (count) { reconnect(_redis.xrange(key, minID, maxID, count, std::back_inserter(raw))); }
  else       { reconnect(_redis.xrange(key, minID, maxID, std::back_inserter(raw))); }

  TimeValList<std::vector<T>> ret;
  TimeVal<std::vector<T>> retItem;
  for (const auto& rawItem : raw)
  {
    const std::string str = default_field_value<std::string>(rawItem.second);
    if (str.size())
    {
      retItem.first = RA_Time(rawItem.first);
      retItem.second.assign((T*)str.data(), (T*)(str.data() + str.size()));
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_reverse_stream_helper : get data as type T (T is trivial, string or Attrs)
//                            that occurred before a specified time
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    maxTime : highest time to get data for
//    count   : max number of items to get
//    return  : TimeValList of TimeVal<Attrs>
//
template<typename T> RedisAdapter::TimeValList<T>
RedisAdapter::get_reverse_stream_helper(const std::string& baseKey, const std::string& subKey,
                                        RA_Time maxTime, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  std::string key = build_key(subKey, baseKey);
  std::string maxID = maxTime.id_or_max();
  ItemStream raw;

  if (count) { reconnect(_redis.xrevrange(key, maxID, "-", count, std::back_inserter(raw))); }
  else       { reconnect(_redis.xrevrange(key, maxID, "-", std::back_inserter(raw))); }

  TimeValList<T> ret;
  TimeVal<T> retItem;
  for (auto rawItem = raw.rbegin(); rawItem != raw.rend(); rawItem++)   //  reverse iterate
  {
    swr::Optional<T> maybe = default_field_value<T>(rawItem->second);
    if (maybe)
    {
      retItem.first = RA_Time(rawItem->first);
      retItem.second = maybe.value();
      ret.push_back(retItem);
    }
  }
  return ret;
}
//  Attrs specialization
template<> inline RedisAdapter::TimeValList<RedisAdapter::Attrs>
RedisAdapter::get_reverse_stream_helper(const std::string& baseKey, const std::string& subKey,
                                        RA_Time maxTime, uint32_t count)
{
  std::string key = build_key(subKey, baseKey);
  std::string maxID = maxTime.id_or_max();
  ItemStream raw;

  if (count) { reconnect(_redis.xrevrange(key, maxID, "-", count, std::back_inserter(raw))); }
  else       { reconnect(_redis.xrevrange(key, maxID, "-", std::back_inserter(raw))); }

  TimeValList<Attrs> ret;
  TimeVal<Attrs> retItem;
  for (auto rawItem = raw.rbegin(); rawItem != raw.rend(); rawItem++)   //  reverse iterate
  {
    retItem.first = RA_Time(rawItem->first);
    retItem.second = rawItem->second;
    ret.push_back(retItem);
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_reverse_stream_list_helper : get data as type vector<T> (T is trivial)
//                                 that occurred before a specified time
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    maxTime : highest time to get data for
//    count   : max number of items to get
//    return  : ItemStream of Item<vector<T>>
//
template<typename T> RedisAdapter::TimeValList<std::vector<T>>
RedisAdapter::get_reverse_stream_list_helper(const std::string& baseKey, const std::string& subKey,
                                             RA_Time maxTime, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  std::string key = build_key(subKey, baseKey);
  std::string maxID = maxTime.id_or_max();
  ItemStream raw;

  if (count) { reconnect(_redis.xrevrange(key, maxID, "-", count, std::back_inserter(raw))); }
  else       { reconnect(_redis.xrevrange(key, maxID, "-", std::back_inserter(raw))); }

  TimeValList<std::vector<T>> ret;
  TimeVal<std::vector<T>> retItem;
  for (auto rawItem = raw.rbegin(); rawItem != raw.rend(); rawItem++)   //  reverse iterate
  {
    const std::string str = default_field_value<std::string>(rawItem->second);
    if (str.size())
    {
      retItem.first = RA_Time(rawItem->first);
      retItem.second.assign((T*)str.data(), (T*)(str.data() + str.size()));
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_single_stream_helper : get a single data item of type T (T is trivial, string or Attrs)
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    dest    : destination to copy data to
//    maxTime : time that equals or exceeds the data to get
//    return  : time of the data item if successful, zero on failure
//
template<typename T> RA_Time
RedisAdapter::get_single_stream_helper(const std::string& baseKey, const std::string& subKey,
                                       T& dest, RA_Time maxTime)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  ItemStream raw;
  if ( ! reconnect(_redis.xrevrange(build_key(subKey, baseKey),
                                  maxTime.id_or_max(), "-", 1,
                                  std::back_inserter(raw))))
    { return RA_NOT_CONNECTED; }

  if (raw.size())
  {
    swr::Optional<T> maybe = default_field_value<T>(raw.front().second);
    if (maybe)
    {
      dest = maybe.value();
      return RA_Time(raw.front().first);
    }
  }
  return {};
}
//  Attrs specialization
template<> inline RA_Time
RedisAdapter::get_single_stream_helper(const std::string& baseKey, const std::string& subKey,
                                       Attrs& dest, RA_Time maxTime)
{
  ItemStream raw;
  if ( ! reconnect(_redis.xrevrange(build_key(subKey, baseKey),
                                  maxTime.id_or_max(), "-", 1,
                                  std::back_inserter(raw))))
    { return RA_NOT_CONNECTED; }

  if (raw.size())
  {
    dest = raw.front().second;
    return RA_Time(raw.front().first);
  }
  return {};
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_single_stream_list_helper : get a single data item as vector<T> (T is trivial)
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    dest    : destination to copy data to
//    maxTime : time that equals or exceeds the data to get
//    return  : id of the data item if successful, empty string on failure
//
template<typename T> RA_Time
RedisAdapter::get_single_stream_list_helper(const std::string& baseKey, const std::string& subKey,
                                            std::vector<T>& dest, RA_Time maxTime)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  ItemStream raw;
  if ( ! reconnect(_redis.xrevrange(build_key(subKey, baseKey),
                                  maxTime.id_or_max(), "-", 1,
                                  std::back_inserter(raw))))
    { return RA_NOT_CONNECTED; }

  if (raw.size())
  {
    const std::string str = default_field_value<std::string>(raw.front().second);
    if (str.size())
    {
      dest.assign((T*)str.data(), (T*)(str.data() + str.size()));
      return RA_Time(raw.front().first);
    }
  }
  return {};
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addValues : add multiple data items of type T (T is trivial, string or Attrs)
//
//    subKey : sub key to add data to
//    data   : times and data to add (0 time means host time)
//    trim   : trim to greater of this value or number of data items
//    return : vector of ids of successfully added data items
//
template<typename T> std::vector<RA_Time>
RedisAdapter::addValues(const std::string& subKey, const TimeValList<T>& data, uint32_t trim)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  std::vector<RA_Time> ret;
  std::string key = build_key(subKey);
  for (const auto& item : data)
  {
    Attrs attrs = default_field_attrs(item.second);

    std::string id = _redis.xadd(key, item.first.id_or_now(), attrs.begin(), attrs.end());

    if (id.size()) { ret.push_back(RA_Time(id)); }
  }
  if (trim && ret.size()) { _redis.xtrim(key, std::max(trim, (uint32_t)ret.size())); }

  reconnect(ret.size());
  return ret;
}
//  Attrs specialization
template<> inline std::vector<RA_Time>
RedisAdapter::addValues(const std::string& subKey, const TimeValList<Attrs>& data, uint32_t trim)
{
  std::vector<RA_Time> ret;
  std::string key = build_key(subKey);
  for (const auto& item : data)
  {
    std::string id = _redis.xadd(key, item.first.id_or_now(), item.second.begin(), item.second.end());

    if (id.size()) { ret.push_back(RA_Time(id)); }
  }
  if (trim && ret.size()) { _redis.xtrim(key, std::max(trim, (uint32_t)ret.size())); }

  reconnect(ret.size());
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addLists : add multiple vector<T> as data items (T is trivial)
//
//    subKey : sub key to add data to
//    data   : times and data to add (0 time means host time)
//    trim   : trim to greater of this value or number of data items
//    return : vector of ids of successfully added data items
//
template<typename T> std::vector<RA_Time>
RedisAdapter::addLists(const std::string& subKey, const TimeValList<std::vector<T>>& data, uint32_t trim)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  std::vector<RA_Time> ret;
  std::string key = build_key(subKey);
  for (const auto& item : data)
  {
    Attrs attrs = default_field_attrs(item.second.data(), item.second.size());

    std::string id = _redis.xadd(key, item.first.id_or_now(), attrs.begin(), attrs.end());

    if (id.size()) { ret.push_back(RA_Time(id)); }
  }
  if (trim && ret.size()) { _redis.xtrim(key, std::max(trim, (uint32_t)ret.size())); }

  reconnect(ret.size());
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addSingleValue : add a single data item of type T (T is trivial, string or Attrs)
//
//    subKey : sub key to add data to
//    time   : time to add the data at (0 for current host time)
//    data   : data to add
//    trim   : number of items to trim the stream to
//    return : time of the added data item if successful, zero on failure
//
template<typename T> RA_Time
RedisAdapter::addSingleValue(const std::string& subKey, const T& data, const RA_ArgsAdd& args)
{
  static_assert( ! std::is_same<T, double>(), "use addSingleDouble for double or 'f' suffix for float literal");
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  std::string key = build_key(subKey);
  Attrs attrs = default_field_attrs(data);

  std::string id = args.trim ? _redis.xaddTrim(key, args.time.id_or_now(), attrs.begin(), attrs.end(), args.trim)
                             : _redis.xadd(key, args.time.id_or_now(), attrs.begin(), attrs.end());

  if ( ! reconnect(id.size())) { return RA_NOT_CONNECTED; }

  return RA_Time(id);
}
//  Attrs specialization
template<> inline RA_Time
RedisAdapter::addSingleValue(const std::string& subKey, const Attrs& data, const RA_ArgsAdd& args)
{
  std::string key = build_key(subKey);

  std::string id = args.trim ? _redis.xaddTrim(key, args.time.id_or_now(), data.begin(), data.end(), args.trim)
                             : _redis.xadd(key, args.time.id_or_now(), data.begin(), data.end());

  if ( ! reconnect(id.size())) { return RA_NOT_CONNECTED; }

  return RA_Time(id);
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  add_single_stream_list_helper : add a data item as buffer of type T data (T is trivial)
//
//    subKey : sub key to add data to
//    time   : time to add the data at (0 is current host time)
//    data   : pointer to buffer of type T data to add
//    size   : number of type T elements in buffer
//    trim   : number of items to trim the stream to
//    return : time of the added data item if successful, zero on failure
//
template<typename T> RA_Time
RedisAdapter::add_single_stream_list_helper(const std::string& subKey, RA_Time time, const T* data, size_t size, uint32_t trim)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  std::string key = build_key(subKey);
  Attrs attrs = default_field_attrs(data, size);

  std::string id = trim ? _redis.xaddTrim(key, time.id_or_now(), attrs.begin(), attrs.end(), trim)
                        : _redis.xadd(key, time.id_or_now(), attrs.begin(), attrs.end());

  if ( ! reconnect(id.size())) { return RA_NOT_CONNECTED; }

  return RA_Time(id);
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  make_reader_callback : wrap user callback to convert data to desired type
//
//    func   : user callback that wants data as type T
//    base   : base key of desired data
//    sub    : sub key of desired data
//    raw    : raw data as type Attrs
//    return : closure that reader thread can call upon data arrival
//
template<typename T> RedisAdapter::reader_sub_fn
RedisAdapter::make_reader_callback(ReaderSubFn<T> func) const
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  return [&, func](const std::string& base, const std::string& sub, const ItemStream& raw)
  {
    TimeValList<T> ret;
    TimeVal<T> retItem;
    for (const auto& rawItem : raw)
    {
      swr::Optional<T> maybe = default_field_value<T>(rawItem.second);
      if (maybe)
      {
        retItem.first = RA_Time(rawItem.first);
        retItem.second = maybe.value();
        ret.push_back(retItem);
      }
    }
    func(base, sub, ret);
  };
}
//  Attrs specialization
template<> inline RedisAdapter::reader_sub_fn
RedisAdapter::make_reader_callback(ReaderSubFn<Attrs> func) const
{
  return [&, func](const std::string& base, const std::string& sub, const ItemStream& raw)
  {
    TimeValList<Attrs> ret;
    TimeVal<Attrs> retItem;
    for (const auto& rawItem : raw)
    {
      retItem.first = RA_Time(rawItem.first);
      retItem.second = rawItem.second;
      ret.push_back(retItem);
    }
    func(base, sub, ret);
  };
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  make_list_reader_callback : wrap user callback to convert data to vector of T
//
//    func   : user callback that wants data as vector of T
//    base   : base key of desired data
//    sub    : sub key of desired data
//    raw    : raw data as type Attrs
//    return : closure that reader thread can call upon data arrival
//
template<typename T> RedisAdapter::reader_sub_fn
RedisAdapter::make_list_reader_callback(ReaderSubFn<std::vector<T>> func) const
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  return [&, func](const std::string& base, const std::string& sub, const ItemStream& raw)
  {
    TimeValList<std::vector<T>> ret;
    TimeVal<std::vector<T>> retItem;
    for (const auto& rawItem : raw)
    {
      const std::string str = default_field_value<std::string>(rawItem.second);
      if (str.size())
      {
        retItem.first = RA_Time(rawItem.first);
        retItem.second.assign((T*)str.data(), (T*)(str.data() + str.size()));
        ret.push_back(retItem);
      }
    }
    func(base, sub, ret);
  };
}
