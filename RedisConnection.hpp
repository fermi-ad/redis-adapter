#pragma once

#include "sw/redis++/redis++.h"
#include <syslog.h>

namespace swr = sw::redis;
namespace chr = std::chrono;

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  class RedisConnection
//
//  Provides common interface to either a Redis single server or a Redis server cluster
//  The user will not know which server type is connected
//  If an exception is thrown in a method, the exception is logged and failure is returned
//  If a method is called while not connected, failure is returned but not logged
//
class RedisConnection
{
public:
  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  class RedisConnection::Options
  //
  //    host     : IP address of server "w.x.y.z"
  //    user     : username for connection
  //    password : password for connection
  //    port     : port server is listening on
  //    size     : connection pool size
  //
  struct Options
  {
    std::string host = "127.0.0.1";
    std::string user;
    std::string password;
    uint32_t timeout = 0;   //  milliseconds
    uint16_t port = 0;
    uint16_t size = 5;
  };

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  RedisConnection : store connection options and attempt to connect
  //
  //    options : see RedisConnection::Options above
  //
  RedisConnection(const Options& opts)
  {
    _co.host = opts.host;
    if (opts.user.size()) _co.user = opts.user;   //  dont overwrite redis++ default with ""
    _co.password = opts.password;
    _co.socket_timeout = chr::milliseconds(opts.timeout);
    if (opts.port) _co.port = opts.port;          //  dont overwrite redis++ default with 0
    _cpo.size = opts.size;

    if ( ! connect()) syslog(LOG_ERR, "RedisConnection failed to connect in constructor");
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  RedisConnection : deleted to prevent copy construction
  //
  RedisConnection(const RedisConnection& conn) = delete;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  operator= : deleted to prevent copy by assignment
  //
  RedisConnection& operator=(const RedisConnection&) = delete;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  RedisConnection : move allowed
  //
  RedisConnection(RedisConnection&&) = default;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  connect : attempt to make either a cluster or single server connection
  //
  //    return : true if live server connected
  //             false if not connected
  //
  bool connect()
  {
    try { _cluster = std::make_unique<swr::RedisCluster>(_co, _cpo); }  //  this one throws
    catch (...)
    {
      try
      {
        _singler = std::make_unique<swr::Redis>(_co, _cpo);   //  this one does not
        _singler->ping();                                     //  but this one does
      }
      catch (...) { _singler.reset(); }   //  reset _singler to null since not really connected
    }
    //  a live server is connected, either _cluster OR _singler is valid (but not both)
    if (_cluster || _singler) return true;

    //  neither server type connected, log the failure and return false
    syslog(LOG_ERR, "RedisConnection can't connnect(%zu) to %s:%i", _cpo.size, _co.host.c_str(), _co.port);
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  ping() - test whether or not a live server is connected
  //
  //    return : true if live server connected
  //             false if not connected
  //
  bool ping(const std::string& key = "ping")
  {
    try
    {
      if (_cluster) return _cluster->redis(key, false).ping().compare("PONG") == 0;
      if (_singler) return _singler->ping().compare("PONG") == 0;
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  del() - delete the specified key
  //
  //    key    : key (any type) to delete
  //    return : 1 if key deleted
  //             0 if key not found
  //            -1 if not connected
  //
  int32_t del(const std::string& key)
  {
    try
    {
      if (_cluster) return _cluster->del(key);
      if (_singler) return _singler->del(key);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xrange : read a forward-id-ordered (newest last) list of elements from a stream
  //
  //    key    : the stream to read
  //    beg    : the lowest id to read (subject to cnt)
  //    end    : the highest id to read
  //    cnt    : the max number of elements to read (regardless of beg)
  //    out    : the elements read, typically ItemStream
  //    return : true if connected
  //             false if not connected
  //
  template<typename Output>
  bool xrange(const std::string& key, const std::string& beg,
              const std::string& end, uint32_t cnt, Output out)
  {
    try
    {
      if (_cluster) { _cluster->xrange(key, beg, end, cnt, out); return true; }
      if (_singler) { _singler->xrange(key, beg, end, cnt, out); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xrange : read a forward-id-ordered (newest last) list of elements from a stream
  //
  //    key    : the stream to read
  //    beg    : the lowest id to read
  //    end    : the highest id to read
  //    out    : the elements read, typically ItemStream
  //    return : true if connected
  //             false if not connected
  //
  template<typename Output>
  bool xrange(const std::string& key, const std::string& beg,
              const std::string& end, Output out)
  {
    try
    {
      if (_cluster) { _cluster->xrange(key, beg, end, out); return true; }
      if (_singler) { _singler->xrange(key, beg, end, out); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xrevrange : read a reverse-id-ordered (newest first) list of elements from a stream
  //
  //    key    : the stream to read
  //    end    : the highest id to read
  //    beg    : the lowest id to read (subject to cnt)
  //    cnt    : the max number of elements to read (regardless of beg)
  //    out    : the elements read, typically ItemStream
  //    return : true if connected
  //             false if not connected
  //
  template<typename Output>
  bool xrevrange(const std::string& key, const std::string& end,
                 const std::string& beg, uint32_t cnt, Output out)
  {
    try
    {
      if (_cluster) { _cluster->xrevrange(key, end, beg, cnt, out); return true; }
      if (_singler) { _singler->xrevrange(key, end, beg, cnt, out); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xrevrange : read a reverse-id-ordered (newest first) list of elements from a stream
  //
  //    key    : the stream to read
  //    end    : the highest id to read
  //    beg    : the lowest id to read
  //    out    : the elements read, typically ItemStream
  //    return : true if connected
  //             false if not connected
  //
  template<typename Output>
  bool xrevrange(const std::string& key, const std::string& end,
                 const std::string& beg, Output out)
  {
    try
    {
      if (_cluster) { _cluster->xrevrange(key, end, beg, out); return true; }
      if (_singler) { _singler->xrevrange(key, end, beg, out); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xreadMultiBlock : read from multiple streams, block until new data on one or more streams
  //
  //    fst    : the first element of map<string, string> of: stream key -> most recent element id read
  //    lst    : the last element of map<string, string> of: stream key -> most recent element id read
  //    tmo    : timeout in milliseconds (zero means block indefinitely)
  //    out    : the elements read as Streams per https://github.com/sewenew/redis-plus-plus#examples-4
  //    return : true if connected
  //             false if not connected
  //
  //  Note that this method will fail on a cluster unless the specified keys all hash to the same slot
  //    https://stackoverflow.com/questions/38042629/redis-cross-slot-error
  //    https://redis.io/docs/reference/cluster-spec/
  //
  template<typename Input, typename Output>
  bool xreadMultiBlock(Input fst, Input lst, uint32_t tmo, Output out)
  {
    try
    {
      if (_cluster) { _cluster->xread(fst, lst, chr::milliseconds(tmo), out); return true; }
      if (_singler) { _singler->xread(fst, lst, chr::milliseconds(tmo), out); return true; }
    }
    catch (const swr::TimeoutError&) { return true; }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xadd : add an element to the specified stream
  //
  //    key    : the stream to add an element to
  //    id     : the id for the element, must exceed current highest id, "*" means server will generate
  //    fst    : the first element of map<string, string> of: field -> value
  //    fst    : the last element of map<string, string> of: field -> value
  //    return : the id of the new element if successful
  //             empty string if unsuccsessful or not connected
  //
  template<typename Input>
  std::string xadd(const std::string& key, const std::string& id, Input fst, Input lst)
  {
    try
    {
      if (_cluster) return _cluster->xadd(key, id, fst, lst);
      if (_singler) return _singler->xadd(key, id, fst, lst);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return {};
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xtrim : trim older elements from a stream
  //
  //    key    : the stream to add an element to
  //    thr    : the threshold (number of elements) to trim stream to (zero works but is silly)
  //    apx    : true if thr can be approximate (>=), false if thr should be exact
  //    return : the number of trimmed elements if successful
  //             -1 if unsuccsessful or not connected
  //
  int32_t xtrim(const std::string& key, uint32_t thr, bool apx = true)
  {
    try
    {
      if (_cluster) return _cluster->xtrim(key, thr, apx);
      if (_singler) return _singler->xtrim(key, thr, apx);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  xaddTrim : add an element to the specified stream and trim older elements
  //
  //    key    : the stream to add an element to
  //    id     : the id for the element, must exceed current highest id, "*" means server will generate
  //    fst    : the first element of map<string, string> of: field -> value
  //    fst    : the last element of map<string, string> of: field -> value
  //    thr    : the threshold (number of elements) to trim stream to (zero works but is silly)
  //    apx    : true if thr can be approximate (>=), false if thr should be exact
  //    return : the id of the new element if successful
  //             empty string if unsuccsessful or not connected
  //
  template<typename Input>
  std::string xaddTrim(const std::string& key, const std::string& id,
                       Input fst, Input lst, uint32_t thr, bool apx = true)
  {
    try
    {
      if (_cluster) return _cluster->xadd(key, id, fst, lst, thr, apx);
      if (_singler) return _singler->xadd(key, id, fst, lst, thr, apx);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return {};
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  exists : test if a key exists
  //
  //    key    : the key to look for
  //    return : 1 if key exists
  //             0 if key does not exist
  //            -1 if unsuccsessful or not connected
  //
  int32_t exists(const std::string& key)
  {
    try
    {
      if (_cluster) return _cluster->exists(key);
      if (_singler) return _singler->exists(key);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }
  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  keyslot : find the cluster slot for a key
  //
  //    key    : the key to find a slot for
  //    return : the slot number if successful
  //             0 if connected to a single redis
  //            -1 if unsuccsessful or not connected
  //
  int32_t keyslot(const std::string& key)
  {
    try
    {
      if (_cluster) return _cluster->command<long long>("cluster", "keyslot", key);
      if (_singler) return 0;
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  copy : copy a key to another key
  //
  //    src    : the key to copy from
  //    dst    : the key to copy to
  //    return : 1 if key copied
  //             0 if key not copied
  //            -1 if error or not connected
  //            -2 if CROSSSLOT error
  //
  //  Note that this method will fail on a cluster unless src and dst hash to the same slot
  //    https://stackoverflow.com/questions/38042629/redis-cross-slot-error
  //    https://redis.io/docs/reference/cluster-spec/
  //
  int32_t copy(const std::string& src, const std::string& dst)
  {
    try
    {
      if (_cluster) return _cluster->command<long long>("copy", src, dst);
      if (_singler) return _singler->command<long long>("copy", src, dst);
    }
    catch (const swr::Error& e)
    {
      if (std::string(e.what()).find("CROSSSLOT") != std::string::npos) return -2;
      syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what());
    }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  rename : rename a key to another key
  //
  //    src    : the key to rename from
  //    dst    : the key to rename to
  //    return : true if connected
  //             false if not connected
  //
  //  Note that this method will fail on a cluster unless src and dst hash to the same slot
  //    https://stackoverflow.com/questions/38042629/redis-cross-slot-error
  //    https://redis.io/docs/reference/cluster-spec/
  //
  bool rename(const std::string& src, const std::string& dst)
  {
    try
    {
      if (_cluster) { _cluster->rename(src, dst); return true; }
      if (_singler) { _singler->rename(src, dst); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  time() - gets current server time
  //
  //    return : vector<string> = { seconds, microseconds }
  //
  std::vector<std::string> time(const std::string& key = "time")
  {
    std::vector<std::string> ret;
    try
    {
      if (_cluster) _cluster->redis(key, false).command("time", std::back_inserter(ret));
      if (_singler) _singler->command("time", std::back_inserter(ret));
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return ret;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  subscriber : get a Subscriber object for pub/sub
  //
  //    return : Optional with a Subscriber if successful
  //             empty Optional if unsuccessful or not connected
  //
  swr::Optional<swr::Subscriber> subscriber()
  {
    try
    {
      if (_cluster) { return swr::Optional<swr::Subscriber>(_cluster->subscriber()); }
      if (_singler) { return swr::Optional<swr::Subscriber>(_singler->subscriber()); }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return {};
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  publish : publish a message to a pub/sub channel
  //
  //    chn    : the channel to publish to
  //    msg    : the message to publish
  //    return : >= 0 the number of subscribers notified
  //             -1 if not connected
  //
  int32_t publish(const std::string& chn, const std::string& msg)
  {
    try
    {
      if (_cluster) return _cluster->publish(chn, msg);
      if (_singler) return _singler->publish(chn, msg);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

private:
  swr::ConnectionOptions      _co;
  swr::ConnectionPoolOptions  _cpo;

  std::unique_ptr<swr::RedisCluster> _cluster;
  std::unique_ptr<swr::Redis>        _singler;
};
