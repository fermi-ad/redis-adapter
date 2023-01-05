/**
 * RedisAdapter.C
 *
 * This file contains the implementation of the RedisAdapter class.
 *
 * @author rsantucc
 */


#include "RedisAdapter.hpp"

#include <exception>

using namespace sw::redis;
using namespace std;

template<>
RedisAdapter<Redis>::RedisAdapter( string key, string connection )
:_redis(connection),
 _baseKey(key)
{
  _connection = connection;
  _configKey  = _baseKey + ":CONFIG";
  _logKey     = _baseKey + ":LOG";
  _channelKey = _baseKey + ":CHANNEL";
  _statusKey  = _baseKey + ":STATUS";
  _timeKey    = _baseKey + ":TIME";
  _dataBaseKey= _baseKey + ":DATA";
  _deviceKey = _baseKey + ":DEVICES";

  TRACE(2,"Loaded Redis Adapters");

}

template<>
RedisAdapter<RedisCluster>::RedisAdapter( string key, string connection )
:_redis(connection),
 _baseKey(key)
{
  _connection = connection;
  _configKey  = _baseKey + ":CONFIG";
  _logKey     = _baseKey + ":LOG";
  _channelKey = _baseKey + ":CHANNEL";
  _statusKey  = _baseKey + ":STATUS";
  _timeKey    = _baseKey + ":TIME";
  _dataBaseKey= _baseKey + ":DATA";
  _deviceKey = _baseKey + ":DEVICES";

  TRACE(2,"Loaded Redis Adapters");

}

template<typename T>
RedisAdapter<T>::RedisAdapter(string key, string connection){
  static_assert(!std::is_same<T, Redis>::value || std::is_same<T, RedisCluster>::value, "You can't use that type here");
}



template<>
RedisAdapter<Redis>::RedisAdapter(const RedisAdapter<Redis>& ra)
:_redis(ra._connection)
{
  _baseKey    = ra.getBaseKey();
  _configKey  = _baseKey + ":CONFIG";
  _logKey     = _baseKey + ":LOG";
  _channelKey = _baseKey + ":CHANNEL";
  _statusKey  = _baseKey + ":STATUS";
  _timeKey  = _baseKey + ":TIME";
  _dataBaseKey  = _baseKey + ":DATA";
  _deviceKey = _baseKey + ":DEVICES";
}

template<>
RedisAdapter<RedisCluster>::RedisAdapter(const RedisAdapter<RedisCluster>& ra)
:_redis(ra._connection)
{
  _baseKey    = ra.getBaseKey();
  _configKey  = _baseKey + ":CONFIG";
  _logKey     = _baseKey + ":LOG";
  _channelKey = _baseKey + ":CHANNEL";
  _statusKey  = _baseKey + ":STATUS";
  _timeKey  = _baseKey + ":TIME";
  _dataBaseKey  = _baseKey + ":DATA";
  _deviceKey = _baseKey + ":DEVICES";
}

template<>
RedisAdapter<Redis>::~RedisAdapter(){}

template<>
RedisAdapter<RedisCluster>::~RedisAdapter(){}

template<typename T>
vector<string> RedisAdapter<T>::getDevices(){

  std::unordered_set<string> set = getSet(_deviceKey);
  std::vector<string> devices(set.begin(), set.end());
  return devices;

}

template<typename T>
void RedisAdapter<T>::clearDevices(string devicelist)
{
  std::unordered_map<string, string> nameMap;
  _redis.hgetall(devicelist, inserter(nameMap, nameMap.begin()));
  for(const auto& name : nameMap){
    _redis.del(name.first);
  }
}
/*
* Get Device Config
*/
template<typename T>
void RedisAdapter<T>::setDeviceConfig(std::unordered_map<std::string, std::string> map){
    setHash(_configKey, map);
}

template<typename T>
std::unordered_map<std::string, std::string> RedisAdapter<T>::getDeviceConfig(){
    return getHash(_configKey);
}

template<typename T>
void RedisAdapter<T>::setDevice(string name){
  setSet(_deviceKey, name);
}

template<typename T>
sw::redis::Optional<string> RedisAdapter<T>::getValue(string key){
  return _redis.get(key);
}

template<typename T>
void RedisAdapter<T>::setValue(string key, string val){
  _redis.set(key,val);
}

template<typename T>
int RedisAdapter<T>::getUniqueValue(string key){
  return _redis.incr(key);
} 

/*
* Hash get and set
*/
template<typename T>
unordered_map<string, string> RedisAdapter<T>::getHash(string key){

  std::unordered_map<std::string, std::string> m;
  _redis.hgetall(key, std::inserter(m, m.begin()));

  return m;
}

template<typename T>
void RedisAdapter<T>::setHash(string key, std::unordered_map<std::string, std::string> m){

   return _redis.hmset(key, m.begin(), m.end());
}


/*
* Set get and add member
*/
template<typename T>
std::unordered_set<string> RedisAdapter<T>::getSet(string key){

  std::unordered_set<string> set;
  try{
    _redis.smembers( key, std::inserter(set, set.begin()));

  }
  catch(...){

  }
  return set;
}

template<typename T>
void RedisAdapter<T>::setSet(string key, string val){
  _redis.sadd(key,val);
}



/*
* Stream Functions
*/
// Redis Stream structure:
// element 1:
//   timestamp
//   fieldname1 fielddata1
//   fieldname2 fielddata2
// element 2:
//   timestamp
//   fieldname1 fielddata1

// Adds data to a redis stream at the key key
// timeID is the time that should be used as the time in the stream 
// data is formated as a pair of strings the first is the element name and the second is the data at that element
template<typename T>
void RedisAdapter<T>::streamWrite(vector<pair<string,string>> data, string timeID , string key, uint trim ){
  try{
    auto replies  =  _redis.xadd(key, timeID, data.begin(), data.end());  
    if(trim)
      streamTrim(key, trim);
  }catch (const std::exception &err) {
    TRACE(1,"xadd(" + key + ", " + ", ...) failed: " + err.what());
  }
}

// Simlified version of streamWrite when you only have one element in the item you want to add to the stream, and you have binary data.
// When this is called an element is appended to the stream named 'key' that has one field named 'field' with the value data in binary form. 

template<typename T>
void RedisAdapter<T>::streamWriteOneField(const string& data, const string& timeID, const string& key, const string& field)
{
  // Single element vector formated the way that streamWrite wants it.
  std::vector<pair<string, string>> wrapperVector = { {field, data }};
  // When you give * as your time in redis the server generates the timestamp for you. Here we do the same if timeID is empty.
  if (0 == timeID.length()) { streamWrite(wrapperVector,    "*", key, false); }
  else                      { streamWrite(wrapperVector, timeID, key, false); }
}

template<typename T>
string RedisAdapter<T>::streamReadBlock(std::unordered_map<string,string> keysID, int count, std::unordered_map<string,vector<float>>& dest){
  string timeID = "";
  try{
    std::unordered_map<std::string, ItemStream> result;
    _redis.xread(keysID.begin(),keysID.end(), count, std::inserter(result, result.end()) );
    for(auto key : result){
      string dataKey = key.first;
      for(auto data : key.second){
        timeID = data.first;
        for (auto val : data.second){
          dest[dataKey].resize(val.second.length() / sizeof(float));
          memcpy(dest[dataKey].data(),val.second.data(),val.second.length());
        }
      }
    }
    return timeID;  
  }catch (const std::exception &err) {
    TRACE(1,"streamReadBlock fail time: " +timeID+" err: " + err.what());
    return "$";
  }
}

template<typename T>
void RedisAdapter<T>::streamRead(string key, string time, int count, ItemStream& dest) {

  try{
    _redis.xrevrange(key, "+","-", count, back_inserter(dest));
  }catch (const std::exception &err) {
    TRACE(1,"xadd(" + key + ", " +time + ":" + to_string(count) + ", ...) failed: " + err.what());
  }
}

template<typename T>
void RedisAdapter<T>::streamRead(string key, string time, int count, vector<float>& dest){

  try{
    ItemStream result;
    streamRead(key,time,1, result);
    for(Item data : result){
        string timeID = data.first;
        for (auto val : data.second){
            // If we have an element named data
            if(val.first.compare("DATA") == 0){
              dest.resize(val.second.length() / sizeof(float));
              memcpy(dest.data(),val.second.data(),val.second.length());
            }
        }
    }
  }catch (const std::exception &err) {
    TRACE(1,"xadd(" + key + ", " +time + ":" + to_string(count) + ", ...) failed: " + err.what());
  }
}

template<typename T>
void RedisAdapter<T>::logWrite(string key, string msg, string source){
  vector<pair<string,string>> data;
  data.emplace_back(make_pair(source,msg));
  streamWrite(data, "*", key, 1000);
}

template<typename T>
IRedisAdapter::ItemStream RedisAdapter<T>::logRead(uint count){
  ItemStream is ;
  try{ 
    _redis.xrevrange(getLogKey(), "+","-", count, back_inserter(is));
  } catch (const std::exception &err) {
    TRACE(1,"logRead(" + getLogKey()  + "," + to_string(count) + ") failed: " + err.what());
  }
  return is;
}

template<typename T>
void RedisAdapter<T>::streamTrim(string key, int size){
  try{
      _redis.xtrim(key, size, false);

  }catch (const std::exception &err) {
    TRACE(1,"xtrim(" + key + ", " +to_string(size)+ ", ...) failed: " + err.what());
  }
}


template<typename T>
void RedisAdapter<T>::publish(string msg){
  try{
      _redis.publish(_channelKey, msg);
  }catch (const std::exception &err) {
    TRACE(1,"publish(" + _channelKey + ", " +msg+ ", ...) failed: " + err.what());
  }
}

template<typename T>
void RedisAdapter<T>::publish(string key, string msg){
  try{
      _redis.publish(_channelKey + ":" + key, msg);

  }catch (const std::exception &err) {
    TRACE(1,"publish(" + _channelKey + ", " +msg+ ", ...) failed: " + err.what());
  }
}

inline bool const StringToBool(string const& s){
    return s != "0";
  }

template<typename T>
bool RedisAdapter<T>::getDeviceStatus() {
  return StringToBool(getValue(_statusKey).value());
}

template<typename T>
void RedisAdapter<T>::setDeviceStatus(bool status){
  setValue(getStatusKey(), to_string((int)status));
}

template<typename T>
void RedisAdapter<T>::copyKey( string src, string dest, bool data){
    _redis.command<void>("copy", src, dest);
}

template<typename T>
void RedisAdapter<T>::deleteKey( string key ){
  _redis.command<long long>("del", key);
}

template<typename T>
bool RedisAdapter<T>::getAbortFlag(){
  return StringToBool(getValue(_abortKey).value());
}

inline const char * const BoolToString(bool b){
    return b ? "1" : "0";
}
template<typename T>
void RedisAdapter<T>::setAbortFlag(bool flag){
  setValue(_abortKey, BoolToString(flag));
}

template<>
vector<string> RedisAdapter<Redis>::getServerTime(){
  std::vector<string> result;
  _redis.command("time", std::back_inserter(result));
  return result;
}

template<>
vector<string> RedisAdapter<RedisCluster>::getServerTime(){
  std::vector<string> result;
   _redis.redis("hash-tag", false).command("time", std::back_inserter(result));
  return result;
}


template<typename T>
sw::redis::Optional<timespec> RedisAdapter<T>::getServerTimespec()
{
  vector<string> result = getServerTime();
  // The redis command time is returns an array with the first element being the time in seconds and the second being the microseconds within that second
  if (result.size() != 2) { return std::nullopt; }
  timespec ts;
  ts.tv_sec  = stoll(result.at(0));        // first element contains unix time
  ts.tv_nsec = stoll(result.at(1)) * 1000; // second element contains microseconds in the second

  return ts;
}

template<typename T>
void RedisAdapter<T>::psubscribe(std::string pattern, std::function<void(std::string,std::string,std::string)> func){
  patternSubscriptions.push_back({ .pattern=pattern, .function=func });
}

template<typename T>
void RedisAdapter<T>::subscribe(std::string channel, std::function<void(std::string,std::string)> func){
  subscriptions.push_back(keyFunctionPair{ .key=channel, .function=func});
}

template<typename T>
void RedisAdapter<T>::startListener(){
  _listener = thread(&RedisAdapter::listener, this);
}
template<typename T>
void RedisAdapter<T>::startReader(){
  _reader = thread(&RedisAdapter::reader, this);
}

template<typename T>
void RedisAdapter<T>::registerCommand(std::string command, std::function<void(std::string, std::string)> func){
  commands.emplace(_channelKey +":"+ command, func);
}

template<typename T>
void RedisAdapter<T>::listener(){
  // Consume messages in a loop.
  bool flag = false;
  Subscriber _sub = _redis.subscriber();
  while (true) {
    try {
      if(flag)
        _sub.consume();
      else{
        flag = true;

        _sub.on_pmessage([&](std::string pattern, std::string key, std::string msg) {
          auto search = commands.find(key);;
          if(search != commands.end()){
            search->second(key, msg);
          }
          else{
            vector<patternFunctionPair> matchingPatterns;
            for (patternFunctionPair patternSubscription : patternSubscriptions)
            {
              if (patternSubscription.pattern == pattern) { matchingPatterns.push_back(patternSubscription); }
            }
    
            // Loop over the members of patternSubscriptions that have the same pattern as this event
            for (patternFunctionPair patternFunction : matchingPatterns)
            {
              patternFunction.function(pattern, key, msg);
            }
          }
        });

        _sub.on_message([&](std::string key, std::string msg) {
          
          vector<keyFunctionPair> matchingSubscriptions;
          for (keyFunctionPair subscription : subscriptions)
          {
            if (subscription.key == key) { matchingSubscriptions.push_back(subscription); }
          }

          // Loop over the members of subscriptions that have the same key as this event
          for (keyFunctionPair keyFunction : matchingSubscriptions)
          {
            keyFunction.function(key, msg);
          }
        });
        //The default is everything published on ChannelKey
        _sub.psubscribe(_channelKey + "*");
        // Subscribe to the pattens in patternSubscriptions
         for (auto element : patternSubscriptions)
          { _sub.psubscribe(element.pattern); }
         // Subscribe to the keys in subscriptions
         for (auto element : subscriptions)
          { _sub.subscribe(element.key); }
      }
    }
    catch(const TimeoutError &e) {
        continue;
    }
    catch (std::exception &e) {
        // Handle unrecoverable exceptions. Need to re create redis connection
        std::cout << "ERROR " << e.what() << " occured, trying to recover" << std::endl;
        flag = false;
        _sub = _redis.subscriber();
        continue;
    }
  }
}

template<typename T>
void RedisAdapter<T>::reader(){
  // Read the stream for data

  while (true) {
    try {
        //streamtime = _redis.streamReadBlock(streamKeys, 1, buffer);

    }
    catch(const TimeoutError &e) {
        continue;
    }
    catch (...) {
        // Handle unrecoverable exceptions. Need to re create redis connection
        std::cout << "AN ERROR OCCURED, trying to recover" << std::endl;
        continue;
    }
  }
}




















