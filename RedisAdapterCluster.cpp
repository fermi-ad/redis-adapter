/**
 * RedisAdapterCluster.C
 *
 * This file contains the implementation of the RedisAdapterCluster class.
 *
 * @author rsantucc
 */


#include "RedisAdapterCluster.hpp"

using namespace sw::redis;
using namespace std;

RedisAdapterCluster::RedisAdapterCluster( string key )
:_redisCluster("tcp://127.0.0.1:30001"),
 _baseKey(key)
{

  _configKey  = _baseKey + ":CONFIG";
  _logKey     = _baseKey + ":LOG";
  _channelKey = _baseKey + ":CHANNEL";
  _statusKey  = _baseKey + ":STATUS";
  _timeKey    = _baseKey + ":TIME";
  _dataBaseKey= _baseKey + ":DATA";
  _deviceKey = _baseKey + ":DEVICES";

  TRACE(2,"Loaded Redis Adapters");

}

RedisAdapterCluster::RedisAdapterCluster(const RedisAdapterCluster& ra)
:_redisCluster("tcp://127.0.0.1:30001")
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


RedisAdapterCluster::~RedisAdapterCluster()
{
}


vector<string> RedisAdapterCluster::getDevices(){

  std::unordered_set<string> set = getSet(_deviceKey);
  std::vector<string> devices(set.begin(), set.end());
  return devices;

}

void RedisAdapterCluster::clearDevices(string devicelist)
{
  std::unordered_map<string, string> nameMap;
  _redisCluster.hgetall(devicelist, inserter(nameMap, nameMap.begin()));
  for(const auto& name : nameMap){
    _redisCluster.del(name.first);
  }
}
/*
* Get Device Config
*/
void RedisAdapterCluster::setDeviceConfig(std::unordered_map<std::string, std::string> map){

    setHash(_configKey, map);

}

std::unordered_map<std::string, std::string> RedisAdapterCluster::getDeviceConfig(){
    return getHash(_configKey);
}

void RedisAdapterCluster::setDevice(string name){
  setSet(_deviceKey, name);
}



 string RedisAdapterCluster::getValue(string key){

  return *(_redisCluster.get(key));
}

void RedisAdapterCluster::setValue(string key, string val){
  _redisCluster.set(key,val);
}


int RedisAdapterCluster::getUniqueValue(string key){
  return _redisCluster.incr(key);
} 

/*
* Hash get and set
*/
unordered_map<string, string> RedisAdapterCluster::getHash(string key){

  std::unordered_map<std::string, std::string> m;
  _redisCluster.hgetall(key, std::inserter(m, m.begin()));

  return m;
}

void RedisAdapterCluster::setHash(string key, std::unordered_map<std::string, std::string> m){

   return _redisCluster.hmset(key, m.begin(), m.end());
}


/*
* Set get and add member
*/
std::unordered_set<string> RedisAdapterCluster::getSet(string key){

  std::unordered_set<string> set;
  try{
    _redisCluster.smembers( key, std::inserter(set, set.begin()));

  }
  catch(...){

  }
  return set;
}

void RedisAdapterCluster::setSet(string key, string val){
  _redisCluster.sadd(key,val);
}



/*
* Stream Functions
*/
void RedisAdapterCluster::streamWrite(vector<pair<string,string>> data, string timeID , string key, uint trim ){
  try{
    auto replies  =  _redisCluster.xadd(key, timeID, data.begin(), data.end());  
    if(trim)
      streamTrim(key, trim);
  }catch (const std::exception &err) {
    TRACE(1,"xadd(" + key + ", " + ", ...) failed: " + err.what());
  }
}


string RedisAdapterCluster::streamReadBlock(std::unordered_map<string,string> keysID, int count, std::unordered_map<string,vector<float>>& dest){
  string timeID = "";
  try{
    std::unordered_map<std::string, ItemStream> result;
    _redisCluster.xread(keysID.begin(),keysID.end(), count, std::inserter(result, result.end()) );
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

void RedisAdapterCluster::streamRead(string key, string time, int count, ItemStream& dest){

  try{
    _redisCluster.xrevrange(key, "+","-", count, back_inserter(dest));
  }catch (const std::exception &err) {
    TRACE(1,"xadd(" + key + ", " +time + ":" + to_string(count) + ", ...) failed: " + err.what());
  }
}


void RedisAdapterCluster::streamRead(string key, string time, int count, vector<float>& dest){

  try{
    ItemStream result;
    streamRead(key,time,1, result);
    for(auto data : result){
        string timeID = data.first;
        for (auto val : data.second){
            if(!val.first.compare("DATA")){
              dest.resize(val.second.length() / sizeof(float));
              memcpy(dest.data(),val.second.data(),val.second.length());
            }
        }
    }  
  }catch (const std::exception &err) {
    TRACE(1,"xadd(" + key + ", " +time + ":" + to_string(count) + ", ...) failed: " + err.what());
  }
}

void RedisAdapterCluster::logWrite(string key, string msg, string source){
  vector<pair<string,string>> data;
  data.emplace_back(make_pair(source,msg));
  streamWrite(data, "*", key, 1000);
}


vector<pair<string,string>> RedisAdapterCluster::logRead(std::string key, uint count){
  vector<pair<string,string>> out;
  string timeID;
  try{
    ItemStream result;
    _redisCluster.xrevrange(key, "+","-", count, back_inserter(result));
    for(auto data : result){
        timeID = data.first;
        for (auto val : data.second){
            out.push_back(val);
        }

    }
    return out;  

  }catch (const std::exception &err) {
    TRACE(1,"xadd(" + key + ", " +timeID + ":" + to_string(count) + ", ...) failed: " + err.what());
    return out;
  }
}

void RedisAdapterCluster::streamTrim(string key, int size){
  try{
      _redisCluster.xtrim(key, size, false);

  }catch (const std::exception &err) {
    TRACE(1,"xtrim(" + key + ", " +to_string(size)+ ", ...) failed: " + err.what());
  }
}



void RedisAdapterCluster::publish(string msg){
  try{
      _redisCluster.publish(_channelKey, msg);

  }catch (const std::exception &err) {
    TRACE(1,"publish(" + _channelKey + ", " +msg+ ", ...) failed: " + err.what());
  }
}
void RedisAdapterCluster::publish(string key, string msg){
  try{
      _redisCluster.publish(_channelKey + ":" + key, msg);

  }catch (const std::exception &err) {
    TRACE(1,"publish(" + _channelKey + ", " +msg+ ", ...) failed: " + err.what());
  }
}

inline bool const StringToBool(string const& s){
    return s != "0";
  }
bool RedisAdapterCluster::getDeviceStatus() {
  return StringToBool(getValue(_statusKey));
}
void RedisAdapterCluster::setDeviceStatus(bool status){
  setValue(getStatusKey(), to_string((int)status));
}


void RedisAdapterCluster::copyKey( string src, string dest, bool data){
  if (data)
    _redisCluster.command<void>("copy", src, dest);
  else
    _redisCluster.command<void>("copy", src, dest);

}

//inline bool const StringToBool(string const& s){
//    return s != "0";
//  }
bool RedisAdapterCluster::getAbortFlag(){
  return StringToBool(getValue(_abortKey));
}

inline const char * const BoolToString(bool b){
    return b ? "1" : "0";
}
void RedisAdapterCluster::setAbortFlag(bool flag){
  setValue(_abortKey, BoolToString(flag));
}


vector<string> RedisAdapterCluster::getServerTime(){

  std::vector<string> result;
  _redisCluster.redis("hash-tag", false).command("time", std::back_inserter(result));
  return result;
}




void RedisAdapterCluster::psubscribe(std::string pattern, std::function<void(std::string,std::string,std::string)> func){
  patternSubscriptions.emplace(pattern, func);
}

void RedisAdapterCluster::subscribe(std::string channel, std::function<void(std::string,std::string)> func){
  subscriptions.emplace(channel, func);
}

void RedisAdapterCluster::startListener(){
  _listener = thread(&RedisAdapterCluster::listener, this);
}
void RedisAdapterCluster::startReader(){
  _reader = thread(&RedisAdapterCluster::reader, this);
}

void RedisAdapterCluster::registerCommand(std::string command, std::function<void(std::string, std::string)> func){
  commands.emplace(_channelKey +":"+ command, func);
}


void RedisAdapterCluster::listener(){
  // Consume messages in a loop.
  bool flag = false;
  Subscriber _sub = _redisCluster.subscriber();
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
                  auto patternsearch = patternSubscriptions.find(pattern);
                  if (patternsearch != patternSubscriptions.end()) {
                    patternsearch->second(pattern, key, msg);
                  }
                }
            });

            _sub.on_message([&](std::string key, std::string msg) { 

                  auto search = subscriptions.find(msg);
                  if (search != subscriptions.end()) {
                    search->second( key, msg);
                  }
            });
            //The default is everything published on ChannelKey
            _sub.psubscribe(_channelKey + "*");

        }
    }
    catch(const TimeoutError &e) {
        continue;
    }
    catch (...) {
        // Handle unrecoverable exceptions. Need to re create redis connection
        std::cout << "AN ERROR OCCURED, trying to recover" << std::endl;
        flag = false;
        _sub = _redisCluster.subscriber();
        continue;
    }
  }
}

void RedisAdapterCluster::reader(){
  // Read the stream for data

  while (true) {
    try {
        //streamtime = _redisCluster.streamReadBlock(streamKeys, 1, buffer);

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




















