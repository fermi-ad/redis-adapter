//
//  RedisAdapterTempl.hpp
//
//  This file contains the RedisAdapter method templates

#pragma once

#include "RedisAdapter.hpp"

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  Helper functions for getting DEFAULT_FIELD in swr::Attrs
//
template<> inline auto RedisAdapter::default_field_value<std::string>(const swr::Attrs& attrs)
{
  std::string ret;
  if (attrs.count(DEFAULT_FIELD)) ret = attrs.at(DEFAULT_FIELD);
  return ret;
}
template<typename T> auto RedisAdapter::default_field_value(const swr::Attrs& attrs)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  swr::Optional<T> ret;
  if (attrs.count(DEFAULT_FIELD)) ret = *(const T*)attrs.at(DEFAULT_FIELD).data();
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  Helper functions for setting DEFAULT_FIELD in swr::Attrs
//
template<> inline swr::Attrs RedisAdapter::default_field_attrs(const std::string& data)
{
  return {{ DEFAULT_FIELD, data }};
}
template<typename T> swr::Attrs RedisAdapter::default_field_attrs(const T* data, size_t size)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  return {{ DEFAULT_FIELD, data ? std::string((const char*)data, size * sizeof(T)) : "" }};
}
template<typename T> swr::Attrs RedisAdapter::default_field_attrs(const T& data)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  return {{ DEFAULT_FIELD, std::string((const char*)&data, sizeof(T)) }};
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getSetting : get setting for home device as type T (T is trivial, string)
//
//    subKey : sub key to get setting from
//    return : string or Optional with setting value if successful
//             empty string or Optional if unsuccessful
//
template<> inline auto RedisAdapter::getSetting<std::string>(const std::string& subKey, const std::string& baseKey)
{
  swr::ItemStream<swr::Attrs> raw;

  _redis->xrevrange(build_key(baseKey, SETTINGS_STUB, subKey), "+", "-", 1, back_inserter(raw));

  return raw.size() ? default_field_value<std::string>(raw.front().second) : "";
}
template<typename T> auto RedisAdapter::getSetting(const std::string& subKey, const std::string& baseKey)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  swr::ItemStream<swr::Attrs> raw;

  _redis->xrevrange(build_key(baseKey, SETTINGS_STUB, subKey), "+", "-", 1, back_inserter(raw));

  swr::Optional<T> ret;
  if (raw.size()) ret = default_field_value<T>(raw.front().second);
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  getSettingList<T> : get setting for home device as type vector<T> (T is trivial)
//
//    subKey : sub key to get setting from
//    return : setting value as vector<T> or empty on failure
//
template<typename T> std::vector<T> RedisAdapter::getSettingList(const std::string& subKey, const std::string& baseKey)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  swr::ItemStream<swr::Attrs> raw;

  _redis->xrevrange(build_key(baseKey, SETTINGS_STUB, subKey), "+", "-", 1, back_inserter(raw));

  std::vector<T> ret;
  if (raw.size())
  {
    std::string str = default_field_value<std::string>(raw.front().second);
    ret.assign((T*)str.data(), (T*)(str.data() + str.size()));
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  setSetting : set setting for home device as type T (T is trivial, string)
//
//    subKey : sub key to set setting on
//    value  : setting value to set
//    return : true on success, false on failure
//
template<typename T> bool RedisAdapter::setSetting(const std::string& subKey, const T& value)
{
  static_assert( ! std::is_same<T, double>(), "use setSettingDouble for double or 'f' suffix for float literal");
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  swr::Attrs attrs = default_field_attrs(value);

  return _redis->xaddTrim(_baseKey + SETTINGS_STUB + subKey, "*", attrs.begin(), attrs.end(), 1).size();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  setSettingList : set setting for home device as vector<T> (T is trivial)
//
//    subKey : sub key to set setting on
//    value  : vector of setting values to set
//    return : true on success, false on failure
//
template<typename T> bool RedisAdapter::setSettingList(const std::string& subKey, const std::vector<T>& value)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  swr::Attrs attrs = default_field_attrs(value.data(), value.size());

  return _redis->xaddTrim(_baseKey + SETTINGS_STUB + subKey, "*", attrs.begin(), attrs.end(), 1).size();
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_forward_data_helper<T> : get data as type T (T is trivial, string or Attrs)
//                               that occurred after a specified time
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    minID   : lowest time to get data for
//    maxID   : highest time to get data for
//    count   : max number of items to get
//    return  : ItemStream of Item<T>
//
template<> inline swr::ItemStream<swr::Attrs>
RedisAdapter::get_forward_data_helper(const std::string& baseKey, const std::string& subKey,
                                      const std::string& minID, const std::string& maxID, uint32_t count)
{
  std::string key = build_key(baseKey, DATA_STUB, subKey);
  swr::ItemStream<swr::Attrs> ret;

  if (count) { _redis->xrange(key, minID, maxID, count, std::back_inserter(ret)); }
  else       { _redis->xrange(key, minID, maxID, std::back_inserter(ret)); }

  return ret;
}
template<typename T> swr::ItemStream<T>
RedisAdapter::get_forward_data_helper(const std::string& baseKey, const std::string& subKey,
                                      const std::string& minID, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  std::string key = build_key(baseKey, DATA_STUB, subKey);
  swr::ItemStream<swr::Attrs> raw;

  if (count) { _redis->xrange(key, minID, maxID, count, std::back_inserter(raw)); }
  else       { _redis->xrange(key, minID, maxID, std::back_inserter(raw)); }

  swr::ItemStream<T> ret;
  swr::Item<T> retItem;
  for (const auto& rawItem : raw)
  {
    swr::Optional<T> maybe = default_field_value<T>(rawItem.second);
    if (maybe.has_value())
    {
      retItem.first = rawItem.first;
      retItem.second = maybe.value();
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_forward_data_list_helper<T> : get data as type vector<T> (T is trivial)
//                                    that occurred after a specified time
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    minID   : lowest time to get data for
//    maxID   : highest time to get data for
//    count   : max number of items to get
//    return  : ItemStream of Item<vector<T>>
//
template<typename T> swr::ItemStream<std::vector<T>>
RedisAdapter::get_forward_data_list_helper(const std::string& baseKey, const std::string& subKey,
                                           const std::string& minID, const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  std::string key = build_key(baseKey, DATA_STUB, subKey);
  swr::ItemStream<swr::Attrs> raw;

  if (count) { _redis->xrange(key, minID, maxID, count, std::back_inserter(raw)); }
  else       { _redis->xrange(key, minID, maxID, std::back_inserter(raw)); }

  swr::ItemStream<std::vector<T>> ret;
  swr::Item<std::vector<T>> retItem;
  for (const auto& rawItem : raw)
  {
    const std::string str = default_field_value<std::string>(rawItem.second);
    if (str.size())
    {
      retItem.first = rawItem.first;
      retItem.second.assign((T*)str.data(), (T*)(str.data() + str.size()));
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_reverse_data_helper<T> : get data as type T (T is trivial, string or Attrs)
//                               that occurred before a specified time
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    maxID   : highest time to get data for
//    count   : max number of items to get
//    return  : ItemStream of Item<Attrs>
//
template<> inline swr::ItemStream<swr::Attrs>
RedisAdapter::get_reverse_data_helper(const std::string& baseKey, const std::string& subKey,
                                      const std::string& maxID, uint32_t count)
{
  std::string key = build_key(baseKey, DATA_STUB, subKey);
  swr::ItemStream<swr::Attrs> ret;

  if (count) { _redis->xrevrange(key, maxID, "-", count, std::back_inserter(ret)); }
  else       { _redis->xrevrange(key, maxID, "-", std::back_inserter(ret)); }

  std::reverse(ret.begin(), ret.end());   //  reverse in place
  return ret;
}
template<typename T> swr::ItemStream<T>
RedisAdapter::get_reverse_data_helper(const std::string& baseKey, const std::string& subKey,
                                      const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  std::string key = build_key(baseKey, DATA_STUB, subKey);
  swr::ItemStream<swr::Attrs> raw;

  if (count) { _redis->xrevrange(key, maxID, "-", count, std::back_inserter(raw)); }
  else       { _redis->xrevrange(key, maxID, "-", std::back_inserter(raw)); }

  swr::ItemStream<T> ret;
  swr::Item<T> retItem;
  for (auto rawItem = raw.rbegin(); rawItem != raw.rend(); rawItem++)   //  reverse iterate
  {
    swr::Optional<T> maybe = default_field_value<T>(rawItem->second);
    if (maybe.has_value())
    {
      retItem.first = rawItem->first;
      retItem.second = maybe.value();
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_reverse_data_list_helper<T> : get data as type vector<T> (T is trivial)
//                                    that occurred before a specified time
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    maxID   : highest time to get data for
//    count   : max number of items to get
//    return  : ItemStream of Item<vector<T>>
//
template<typename T> swr::ItemStream<std::vector<T>>
RedisAdapter::get_reverse_data_list_helper(const std::string& baseKey, const std::string& subKey,
                                           const std::string& maxID, uint32_t count)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  std::string key = build_key(baseKey, DATA_STUB, subKey);
  swr::ItemStream<swr::Attrs> raw;

  if (count) { _redis->xrevrange(key, maxID, "-", count, std::back_inserter(raw)); }
  else       { _redis->xrevrange(key, maxID, "-", std::back_inserter(raw)); }

  swr::ItemStream<std::vector<T>> ret;
  swr::Item<std::vector<T>> retItem;
  for (auto rawItem = raw.rbegin(); rawItem != raw.rend(); rawItem++)   //  reverse iterate
  {
    const std::string str = default_field_value<std::string>(rawItem->second);
    if (str.size())
    {
      retItem.first = rawItem->first;
      retItem.second.assign((T*)str.data(), (T*)(str.data() + str.size()));
      ret.push_back(retItem);
    }
  }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_single_data_helper<T> : get a single data item of type T (T is trivial, string or Attrs)
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    dest    : destination to copy data to
//    maxID   : time that equals or exceeds the data to get
//    return  : id of the data item if successful
//              empty string on failure
//
template<> inline std::string
RedisAdapter::get_single_data_helper(const std::string& baseKey, const std::string& subKey,
                                     swr::Attrs& dest, const std::string& maxID)
{
  swr::ItemStream<swr::Attrs> raw;

  _redis->xrevrange(build_key(baseKey, DATA_STUB, subKey), maxID, "-", 1, std::back_inserter(raw));

  std::string id;
  if (raw.size())
  {
    id = raw.front().first;
    dest = raw.front().second;
  }
  return id;
}
template<typename T> std::string
RedisAdapter::get_single_data_helper(const std::string& baseKey, const std::string& subKey,
                                     T& dest, const std::string& maxID)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  swr::ItemStream<swr::Attrs> raw;

  _redis->xrevrange(build_key(baseKey, DATA_STUB, subKey), maxID, "-", 1, std::back_inserter(raw));

  std::string id;
  if (raw.size())
  {
    swr::Optional<T> maybe = default_field_value<T>(raw.front().second);
    if (maybe.has_value())
    {
      id = raw.front().first;
      dest = maybe.value();
    }
  }
  return id;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  get_single_data_list_helper<T> : get a single data item as vector<T> (T is trivial)
//
//    baseKey : base key of device
//    subKey  : sub key to get data from
//    dest    : destination to copy data to
//    maxID   : time that equals or exceeds the data to get
//    return  : id of the data item if successful
//              empty string on failure
//
template<typename T> std::string
RedisAdapter::get_single_data_list_helper(const std::string& baseKey, const std::string& subKey,
                                          std::vector<T>& dest, const std::string& maxID)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  swr::ItemStream<swr::Attrs> raw;

  _redis->xrevrange(build_key(baseKey, DATA_STUB, subKey), maxID, "-", 1, std::back_inserter(raw));

  std::string id;
  if (raw.size())
  {
    const std::string str = default_field_value<std::string>(raw.front().second);
    if (str.size())
    {
      id = raw.front().first;
      dest.assign((T*)str.data(), (T*)(str.data() + str.size()));
    }
  }
  return id;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addData<T> : add multiple data items of type T (T is trivial, string or Attrs)
//
//    subKey : sub key to add data to
//    data   : ids and data to add (empty ids treated as "*")
//    trim   : trim to greater of this value or number of data items
//    return : vector of ids of successfully added data items
//
template<> inline std::vector<std::string>
RedisAdapter::addData(const std::string& subKey, const swr::ItemStream<swr::Attrs>& data, uint32_t trim)
{
  std::vector<std::string> ret;
  std::string key = _baseKey + DATA_STUB + subKey;
  for (const auto& item : data)
  {
    std::string id = item.first.size() ? item.first : "*";
    id = _redis->xadd(key, id, item.second.begin(), item.second.end());
    if (id.size()) { ret.push_back(id); }
  }
  if (trim && ret.size()) { _redis->xtrim(key, std::max(trim, (uint32_t)ret.size())); }
  return ret;
}
template<typename T> std::vector<std::string>
RedisAdapter::addData(const std::string& subKey, const swr::ItemStream<T>& data, uint32_t trim)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  std::vector<std::string> ret;
  std::string key = _baseKey + DATA_STUB + subKey;
  for (const auto& item : data)
  {
    std::string id = item.first.size() ? item.first : "*";
    swr::Attrs attrs = default_field_attrs(item.second);
    id = _redis->xadd(key, id, attrs.begin(), attrs.end());
    if (id.size()) { ret.push_back(id); }
  }
  if (trim && ret.size()) { _redis->xtrim(key, std::max(trim, (uint32_t)ret.size())); }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addDataList<T> : add multiple vector<T> as data items (T is trivial)
//
//    subKey : sub key to add data to
//    data   : ids and data to add (empty ids treated as "*")
//    trim   : trim to greater of this value or number of data items
//    return : vector of ids of successfully added data items
//
template<typename T> std::vector<std::string>
RedisAdapter::addDataList(const std::string& subKey, const swr::ItemStream<std::vector<T>>& data, uint32_t trim)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  std::vector<std::string> ret;
  std::string key = _baseKey + DATA_STUB + subKey;
  for (const auto& item : data)
  {
    std::string id = item.first.size() ? item.first : "*";
    swr::Attrs attrs = default_field_attrs(item.second.data(), item.second.size());
    id = _redis->xadd(key, id, attrs.begin(), attrs.end());
    if (id.size()) { ret.push_back(id); }
  }
  if (trim && ret.size()) { _redis->xtrim(key, std::max(trim, (uint32_t)ret.size())); }
  return ret;
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  addDataSingle<T> : add a data item of type T (T is trivial, string or Attrs)
//
//    subKey : sub key to add data to
//    data   : data to add
//    id     : time to add the data at ("*" is current redis time)
//    trim   : number of items to trim the stream to
//    return : id of the added data item if successful
//             empty string on failure
//
template<> inline std::string
RedisAdapter::addDataSingle(const std::string& subKey, const swr::Attrs& data, const std::string& id, uint32_t trim)
{
  std::string key = _baseKey + DATA_STUB + subKey;

  return trim ? _redis->xaddTrim(key, id, data.begin(), data.end(), trim)
              : _redis->xadd(key, id, data.begin(), data.end());
}
template<typename T> std::string
RedisAdapter::addDataSingle(const std::string& subKey, const T& data, const std::string& id, uint32_t trim)
{
  static_assert( ! std::is_same<T, double>(), "use addDataDouble for double or 'f' suffix for float literal");
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  std::string key = _baseKey + DATA_STUB + subKey;
  swr::Attrs attrs = default_field_attrs(data);

  return trim ? _redis->xaddTrim(key, id, attrs.begin(), attrs.end(), trim)
              : _redis->xadd(key, id, attrs.begin(), attrs.end());
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  add_single_data_list_helper<T> : add a data item as buffer of type T data (T is trivial)
//
//    subKey : sub key to add data to
//    data   : pointer to buffer of type T data to add
//    size   : number of type T elements in buffer
//    id     : time to add the data at ("*" is current redis time)
//    trim   : number of items to trim the stream to
//    return : id of the added data item if successful
//             empty string on failure
//
template<typename T> std::string
RedisAdapter::add_single_data_list_helper(const std::string& subKey, const T* data, size_t size, const std::string& id, uint32_t trim)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  std::string key = _baseKey + DATA_STUB + subKey;
  swr::Attrs attrs = default_field_attrs(data, size);

  return trim ? _redis->xaddTrim(key, id, attrs.begin(), attrs.end(), trim)
              : _redis->xadd(key, id, attrs.begin(), attrs.end());
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  make_reader_callback<T> : wrap user callback to convert data to desired type
//
//    func   : user callback that wants data as type T
//    base   : base key of desired data
//    sub    : sub key of desired data
//    raw    : raw data as type swr::Attrs
//    return : closure that reader thread can call upon data arrival
//
template<> inline RedisAdapter::ReaderSubFn<swr::Attrs>
RedisAdapter::make_reader_callback(ReaderSubFn<swr::Attrs> func) { return func; }

template<typename T> RedisAdapter::ReaderSubFn<swr::Attrs>
RedisAdapter::make_reader_callback(ReaderSubFn<T> func)
{
  static_assert(std::is_trivial<T>() || std::is_same<T, std::string>(), "wrong type T");

  return [&, func](const std::string& base, const std::string& sub, const swr::ItemStream<swr::Attrs>& raw)
  {
    swr::ItemStream<T> ret;
    swr::Item<T> retItem;
    for (const auto& rawItem : raw)
    {
      swr::Optional<T> maybe = default_field_value<T>(rawItem.second);
      if (maybe.has_value())
      {
        retItem.first = rawItem.first;
        retItem.second = maybe.value();
        ret.push_back(retItem);
      }
    }
    func(base, sub, ret);
  };
}

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  make_list_reader_callback<T> : wrap user callback to convert data to vector of T
//
//    func   : user callback that wants data as vector of T
//    base   : base key of desired data
//    sub    : sub key of desired data
//    raw    : raw data as type swr::Attrs
//    return : closure that reader thread can call upon data arrival
//
template<typename T> RedisAdapter::ReaderSubFn<swr::Attrs>
RedisAdapter::make_list_reader_callback(ReaderSubFn<std::vector<T>> func)
{
  static_assert(std::is_trivial<T>(), "wrong type T");

  return [&, func](const std::string& base, const std::string& sub, const swr::ItemStream<swr::Attrs>& raw)
  {
    swr::ItemStream<std::vector<T>> ret;
    swr::Item<std::vector<T>> retItem;
    for (const auto& rawItem : raw)
    {
      const std::string str = default_field_value<std::string>(rawItem.second);
      if (str.size())
      {
        retItem.first = rawItem.first;
        retItem.second.assign((T*)str.data(), (T*)(str.data() + str.size()));
        ret.push_back(retItem);
      }
    }
    func(base, sub, ret);
  };
}
