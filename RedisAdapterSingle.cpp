/**
 * RedisAdapterSingle.C
 *
 * This file contains the implementation of the RedisAdapterSingle class.
 *
 * @author rsantucc
 */


#include "RedisAdapterSingle.hpp"

#include <exception>


using namespace sw::redis;
using namespace std;

RedisAdapterSingle::RedisAdapterSingle( string key, string connection )
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

RedisAdapterSingle::RedisAdapterSingle(const RedisAdapterSingle& ra)
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


RedisAdapterSingle::~RedisAdapterSingle()
{
}


vector<string> RedisAdapterSingle::getDevices(){

  std::unordered_set<string> set = getSet(_deviceKey);
  std::vector<string> devices(set.begin(), set.end());
  return devices;

}

void RedisAdapterSingle::clearDevices(string devicelist)
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
void RedisAdapterSingle::setDeviceConfig(std::unordered_map<std::string, std::string> map){

    setHash(_configKey, map);

}

std::unordered_map<std::string, std::string> RedisAdapterSingle::getDeviceConfig(){
    return getHash(_configKey);
}

void RedisAdapterSingle::setDevice(string name){
  setSet(_deviceKey, name);
}



 optional<string> RedisAdapterSingle::getValue(string key){

  //return *(_redis.get(key));
  return _redis.get(key);
}

void RedisAdapterSingle::setValue(string key, string val){
  _redis.set(key,val);
}


int RedisAdapterSingle::getUniqueValue(string key){
  return _redis.incr(key);
} 

/*
* Hash get and set
*/
unordered_map<string, string> RedisAdapterSingle::getHash(string key){

  std::unordered_map<std::string, std::string> m;
  _redis.hgetall(key, std::inserter(m, m.begin()));

  return m;
}

void RedisAdapterSingle::setHash(string key, std::unordered_map<std::string, std::string> m){

   return _redis.hmset(key, m.begin(), m.end());
}


/*
* Set get and add member
*/
std::unordered_set<string> RedisAdapterSingle::getSet(string key){

  std::unordered_set<string> set;
  try{
    _redis.smembers( key, std::inserter(set, set.begin()));

  }
  catch(...){

  }
  return set;
}

void RedisAdapterSingle::setSet(string key, string val){
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
void RedisAdapterSingle::streamWrite(vector<pair<string,string>> data, string timeID , string key, uint trim ){
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

void RedisAdapterSingle::streamWriteOneField(const string& data, const string& timeID, const string& key, const string& field)
{
  // Single element vector formated the way that streamWrite wants it.
  std::vector<pair<string, string>> wrapperVector = { {field, data }};
  // When you give * as your time in redis the server generates the timestamp for you. Here we do the same if timeID is empty.
  if (0 == timeID.length()) { streamWrite(wrapperVector,    "*", key, false); }
  else                      { streamWrite(wrapperVector, timeID, key, false); }
}

string RedisAdapterSingle::streamReadBlock(std::unordered_map<string,string> keysID, int count, std::unordered_map<string,vector<float>>& dest){
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

void RedisAdapterSingle::streamRead(string key, string time, int count, ItemStream& dest) {

  try{
    _redis.xrevrange(key, "+","-", count, back_inserter(dest));
  }catch (const std::exception &err) {
    TRACE(1,"xadd(" + key + ", " +time + ":" + to_string(count) + ", ...) failed: " + err.what());
  }
}

void RedisAdapterSingle::streamRead(string key, string time, int count, vector<float>& dest){

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

void RedisAdapterSingle::logWrite(string key, string msg, string source){
  vector<pair<string,string>> data;
  data.emplace_back(make_pair(source,msg));
  streamWrite(data, "*", key, 1000);
}

IRedisAdapter::ItemStream RedisAdapterSingle::logRead(uint count){

  ItemStream is ;
  try{ 
    _redis.xrevrange(getLogKey(), "+","-", count, back_inserter(is));
  } catch (const std::exception &err) {
    TRACE(1,"logRead(" + getLogKey()  + "," + to_string(count) + ") failed: " + err.what());
  }
  return is;
}

void RedisAdapterSingle::streamTrim(string key, int size){
  try{
      _redis.xtrim(key, size, false);

  }catch (const std::exception &err) {
    TRACE(1,"xtrim(" + key + ", " +to_string(size)+ ", ...) failed: " + err.what());
  }
}



void RedisAdapterSingle::publish(string msg){
  try{
      _redis.publish(_channelKey, msg);

  }catch (const std::exception &err) {
    TRACE(1,"publish(" + _channelKey + ", " +msg+ ", ...) failed: " + err.what());
  }
}
void RedisAdapterSingle::publish(string key, string msg){
  try{
      _redis.publish(_channelKey + ":" + key, msg);

  }catch (const std::exception &err) {
    TRACE(1,"publish(" + _channelKey + ", " +msg+ ", ...) failed: " + err.what());
  }
}

inline bool const StringToBool(string const& s){
    return s != "0";
  }
bool RedisAdapterSingle::getDeviceStatus() {
  return StringToBool(getValue(_statusKey).value());
}
void RedisAdapterSingle::setDeviceStatus(bool status){
  setValue(getStatusKey(), to_string((int)status));
}


void RedisAdapterSingle::copyKey( string src, string dest, bool data){
  if (data)
    _redis.command<void>("copy", src, dest);
  else
    _redis.command<void>("copy", src, dest);

}

void RedisAdapterSingle::deleteKey( string key ){
  _redis.command<long long>("del", key);
}


//inline bool const StringToBool(string const& s){
//    return s != "0";
//  }
bool RedisAdapterSingle::getAbortFlag(){
  return StringToBool(getValue(_abortKey).value());
}

inline const char * const BoolToString(bool b){
    return b ? "1" : "0";
}
void RedisAdapterSingle::setAbortFlag(bool flag){
  setValue(_abortKey, BoolToString(flag));
}


vector<string> RedisAdapterSingle::getServerTime(){

  std::vector<string> result;
  _redis.command("time", std::back_inserter(result));
  return result;
}

void RedisAdapterSingle::psubscribe(std::string pattern, std::function<void(std::string,std::string,std::string)> func){
  patternSubscriptions.push_back({ .pattern=pattern, .function=func });
}

void RedisAdapterSingle::subscribe(std::string channel, std::function<void(std::string,std::string)> func){
  subscriptions.push_back(keyFunctionPair{ .key=channel, .function=func});
}

void RedisAdapterSingle::startListener(){
  _listener = thread(&RedisAdapterSingle::listener, this);
}
void RedisAdapterSingle::startReader(){
  _reader = thread(&RedisAdapterSingle::reader, this);
}

void RedisAdapterSingle::registerCommand(std::string command, std::function<void(std::string, std::string)> func){
  commands.emplace(_channelKey +":"+ command, func);
}


void RedisAdapterSingle::listener(){
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
            // Loop over the members of patternSubscriptions that have the same pattern as this event
            for (auto function : patternSubscriptions | std::views::filter([&](patternFunctionPair pair){ return pair.pattern == pattern; }))
            {
              function.function(pattern, key, msg);
            }
          }
        });

        _sub.on_message([&](std::string key, std::string msg) {
          // Loop over the members of subscriptions that have the same key as this event
          for (auto function : subscriptions | std::views::filter([&](keyFunctionPair pair){ return pair.key == key; }))
          {
            function.function(key, msg);
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

void RedisAdapterSingle::reader(){
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




















