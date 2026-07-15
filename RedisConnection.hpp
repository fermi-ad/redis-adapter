#pragma once

#include "sw/redis++/redis++.h"
#include <syslog.h>
#include <mutex>

namespace swr = sw::redis;
namespace chr = std::chrono;

//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  struct RedisConnection::Options
  //
  //    path     : path to unix domain socket file
  //    host     : IP address of server "w.x.y.z"
  //    user     : username for connection
  //    password : password for connection
  //    timeout  : connection and blocking read timeout
  //    port     : port server is listening on
  //    size     : connection pool size
  //
  struct Options
  {
    std::string path;
    std::string host = "127.0.0.1";
    std::string user = "default";
    std::string password;
    uint32_t timeout = 500;   //  milliseconds
    uint16_t port = 6379;
    uint16_t size = 5;
  };

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  RedisConnection : store connection options and attempt to connect
  //
  //    options : see RedisConnection::Options above
  //
  RedisConnection(const Options& opts)
  {
    if ( ! connect(opts)) syslog(LOG_ERR, "RedisConnection failed to connect in constructor");
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  RedisConnection : deleted to prevent copy construction
  //
  RedisConnection(const RedisConnection& conn) = delete;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  operator= : deleted to prevent copy by assignment
  //
  RedisConnection& operator=(const RedisConnection&) = delete;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  RedisConnection : move construction not allowed
  //
  RedisConnection(RedisConnection&&) = delete;

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  connect : attempt to make either a cluster or single server connection
  //
  //    options : see RedisConnection::Options above
  //    return : true if live server connected
  //             false if not connected
  //
  bool connect(const Options& opts)
  {
    swr::ConnectionOptions co;
    swr::ConnectionPoolOptions cpo;

    bool is_unix_socket = opts.path.size();   // Check if the host is a Unix socket path

    if (is_unix_socket)
    {
      co.type = swr::ConnectionType::UNIX;  // Set the connection type to UNIX socket
      co.path = opts.path;                  // Set the Unix socket path
    }
    else
    {
      co.host = opts.host;
      co.port = opts.port;
    }
    co.user = opts.user;
    co.password = opts.password;
    co.socket_timeout = chr::milliseconds(opts.timeout);

    cpo.size = opts.size;

    //  build the new client(s) into locals first, then swap them into the shared
    //  _cluster/_singler under a brief lock - every other method takes its own
    //  brief lock just to copy these shared_ptrs before using them (see snapshot()
    //  below), so the old client object stays alive (via the old shared_ptr's
    //  refcount) for as long as any in-flight call is still using it, even after
    //  we replace _cluster/_singler here - this avoids both the original bug
    //  (destroying a live client out from under a concurrent caller) and a
    //  reader/writer-lock starvation problem (a lock held for the entire duration
    //  of a blocking redis call can starve a writer under continuous read traffic)
    std::shared_ptr<swr::RedisCluster> cluster;
    std::shared_ptr<swr::Redis> singler;

    try { cluster = std::make_shared<swr::RedisCluster>(co, cpo); }  //  this one throws
    catch (...)
    {
      try
      {
        singler = std::make_shared<swr::Redis>(co, cpo);   //  this one does not
        singler->ping();                                   //  but this one does
      }
      catch (...) { singler.reset(); }   //  reset singler to null since not really connected
    }

    {
      std::lock_guard<std::mutex> lk(_mtx);
      _cluster = cluster;
      _singler = singler;
    }

    //  a live server is connected, either cluster OR singler is valid (but not both)
    if (cluster || singler) return true;

    //  neither server type connected, log the failure and return false
    if (is_unix_socket)
      syslog(LOG_ERR, "RedisConnection can't connnect to %s", co.path.c_str());
    else
      syslog(LOG_ERR, "RedisConnection can't connnect to %s:%i", co.host.c_str(), co.port);

    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  ping : test whether or not a live server is connected
  //
  //    return : true if live server connected
  //             false if not connected
  //
  bool ping(const std::string& key = "ping")
  {
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) return cluster->redis(key, false).ping().compare("PONG") == 0;
      if (singler) return singler->ping().compare("PONG") == 0;
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  del : delete the specified key
  //
  //    key    : key (any type) to delete
  //    return : 1 if key deleted
  //             0 if key not found
  //            -1 if not connected
  //
  int32_t del(const std::string& key)
  {
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) return cluster->del(key);
      if (singler) return singler->del(key);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) { cluster->xrange(key, beg, end, cnt, out); return true; }
      if (singler) { singler->xrange(key, beg, end, cnt, out); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) { cluster->xrange(key, beg, end, out); return true; }
      if (singler) { singler->xrange(key, beg, end, out); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) { cluster->xrevrange(key, end, beg, cnt, out); return true; }
      if (singler) { singler->xrevrange(key, end, beg, cnt, out); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) { cluster->xrevrange(key, end, beg, out); return true; }
      if (singler) { singler->xrevrange(key, end, beg, out); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) { cluster->xread(fst, lst, chr::milliseconds(tmo), out); return true; }
      if (singler) { singler->xread(fst, lst, chr::milliseconds(tmo), out); return true; }
    }
    catch (const swr::TimeoutError&) { return true; }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) return cluster->xadd(key, id, fst, lst);
      if (singler) return singler->xadd(key, id, fst, lst);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return {};
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) return cluster->xtrim(key, thr, apx);
      if (singler) return singler->xtrim(key, thr, apx);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) return cluster->xadd(key, id, fst, lst, thr, apx);
      if (singler) return singler->xadd(key, id, fst, lst, thr, apx);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return {};
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  exists : test if a key exists
  //
  //    key    : the key to look for
  //    return : 1 if key exists
  //             0 if key does not exist
  //            -1 if unsuccsessful or not connected
  //
  int32_t exists(const std::string& key)
  {
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) return cluster->exists(key);
      if (singler) return singler->exists(key);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  keyslot : find the cluster slot for a key
  //
  //    key    : the key to find a slot for
  //    return : the slot number if successful
  //             0 if connected to a single redis
  //            -1 if unsuccsessful or not connected
  //
  int32_t keyslot(const std::string& key)
  {
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) return cluster->command<long long>("cluster", "keyslot", key);
      if (singler) return 0;
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) return cluster->command<long long>("copy", src, dst);
      if (singler) return singler->command<long long>("copy", src, dst);
    }
    catch (const swr::Error& e)
    {
      if (std::string(e.what()).find("CROSSSLOT") != std::string::npos) return -2;
      syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what());
    }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) { cluster->rename(src, dst); return true; }
      if (singler) { singler->rename(src, dst); return true; }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  time : gets current server time
  //
  //    return : vector<string> = { seconds, microseconds }
  //
  std::vector<std::string> time(const std::string& key = "time")
  {
    auto [cluster, singler] = snapshot();
    std::vector<std::string> ret;
    try
    {
      if (cluster) cluster->redis(key, false).command("time", std::back_inserter(ret));
      if (singler) singler->command("time", std::back_inserter(ret));
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return ret;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  hexists : test if a hashmap field exists
  //
  //    key    : the hashmap to look in
  //    fld    : the field to look for
  //    return : 1 if field exists
  //             0 if field does not exist
  //            -1 if unsuccsessful or not connected
  //
  int32_t hexists(const std::string& key, const std::string& fld)
  {
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) return cluster->hexists(key, fld);
      if (singler) return singler->hexists(key, fld);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  hset : sets a field,value pair in the hashmap
  //
  //    key    : the hashmap to add a field,value pair to
  //    fld    : the field for the pair to be added
  //    val    : the value for the pair to be added
  //    return : true if field added
  //             false if unsuccsessful or not connected
  //
  bool hset(const std::string& key, const std::string& fld, const std::string& val)
  {
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) return cluster->hset(key, fld, val) >= 0;
      if (singler) return singler->hset(key, fld, val) >= 0;
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return false;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  hexpire : set the expiration of a field in a hashmap
  //
  //    key    : the hashmap to set field expiration in
  //    fld    : the field to set expiration on
  //    sec    : the expiration interval in seconds
  //    return : 2 if expiration time set (expired)
  //             1 if expiration time set (unexpired)
  //             0 if option not met (N/A)
  //            -1 if unsuccsessful or not connected
  //            -2 if key or field does not exist
  //            -3 if hexpire not supported
  //
  int32_t hexpire(const std::string& key, const std::string& fld, uint32_t sec)
  {
    auto [cluster, singler] = snapshot();
    std::vector<long long> ret;
    try
    {
      if (cluster) cluster->command("hexpire", key, std::to_string(sec), "fields", "1", fld, std::back_inserter(ret));
      if (singler) singler->command("hexpire", key, std::to_string(sec), "fields", "1", fld, std::back_inserter(ret));
    }
    catch (const swr::Error& e)
    {
      if (std::string(e.what()).find("unknown command") != std::string::npos)
      {
        static bool squelch = false;
        if ( ! squelch)
        {
          syslog(LOG_NOTICE, "RedisConnection::%s %s", __func__,
            "HEXPIRE requires redis-server 7.4.0 or higher - upgrade to support redis-adapter watchdog");
          squelch = true;
        }
        return -3;
      }
      else { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    }
    return ret.size() ? ret.front() : -1;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  hkeys : get all the field names in a hashmap
  //
  //    key    : the hashmap key
  //    return : list of field names for the hashmap
  //
   std::vector<std::string> hkeys(const std::string& key)
  {
    auto [cluster, singler] = snapshot();
    std::vector<std::string> ret;
    try
    {
      if (cluster) cluster->hkeys(key, std::back_inserter(ret));
      if (singler) singler->hkeys(key, std::back_inserter(ret));
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return ret;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  subscriber : get a Subscriber object for pub/sub
  //
  //    return : pointer to a Subscriber if successful
  //             0 if unsuccessful or not connected
  //
  //    note - previously this function returned std::optional<swr::Subscriber> but
  //           std::optional has been replaced with swr::Optional to support c++14
  //           and unfortunately swr::Optional cannot create an empty optional with
  //           swr::Subscriber - now client must delete new swr::Subscriber when done
  //
  swr::Subscriber* subscriber()
  {
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) { return new swr::Subscriber(cluster->subscriber()); }
      if (singler) { return new swr::Subscriber(singler->subscriber()); }
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return 0;
  }

  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  publish : publish a message to a pub/sub channel
  //
  //    chn    : the channel to publish to
  //    msg    : the message to publish
  //    return : >= 0 the number of subscribers notified
  //             -1 if not connected
  //
  int32_t publish(const std::string& chn, const std::string& msg)
  {
    auto [cluster, singler] = snapshot();
    try
    {
      if (cluster) return cluster->publish(chn, msg);
      if (singler) return singler->publish(chn, msg);
    }
    catch (const swr::Error& e) { syslog(LOG_ERR, "RedisConnection::%s %s", __func__, e.what()); }
    return -1;
  }

private:
  //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //  snapshot : copy the current _cluster/_singler shared_ptrs under a brief lock
  //
  //  The lock is only held long enough to bump a refcount (a few instructions), never for
  //  the duration of a redis call (which can block for hundreds of ms) - this is what lets
  //  connect() safely swap in new clients without starving callers under continuous traffic,
  //  while still guaranteeing the client object a caller obtains stays alive for the whole
  //  call even if connect() replaces _cluster/_singler concurrently
  //
  std::pair<std::shared_ptr<swr::RedisCluster>, std::shared_ptr<swr::Redis>> snapshot()
  {
    std::lock_guard<std::mutex> lk(_mtx);
    return { _cluster, _singler };
  }

  std::mutex _mtx;
  std::shared_ptr<swr::RedisCluster> _cluster;
  std::shared_ptr<swr::Redis>        _singler;
};
