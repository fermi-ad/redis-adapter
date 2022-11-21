/**
 * RedisAdapter.H
 *
 * This file contains the definition of the redis Adapter class.
 *
 * @author rsantucc
 */

#ifndef RedisAdapter_HPP
#define RedisAdapter_HPP

#include <string>
#include <errno.h>
#include <stdexcept>
#include <unistd.h>
#include <sstream>
#include <iostream>
#include <thread>
#include <sw/redis++/redis++.h>
#include "IRedisAdapter.hpp"
#include <TRACE/trace.h>

using namespace std;
using namespace sw::redis;
using Attrs = std::unordered_map<std::string, string>;
using Item = std::pair<std::string, sw::redis::Optional<Attrs>>;
using ItemStream = std::vector<Item>;
//using ItemStream = std::unordered_map<std::string, Attrs>;


  /**
   * RedisAdapter
   */
class RedisAdapter: public IRedisAdapter {

   public:
	/*Constructor / Destructor*/
    RedisAdapter(string key);
	RedisAdapter(const RedisAdapter& ra);
    ~RedisAdapter();

	/*Wrapper Functions*/
	virtual vector<string> getDevices();
    virtual void clearDevices(string devicelist);
	virtual void setDeviceConfig(std::unordered_map<std::string, std::string> map);
	virtual std::unordered_map<std::string, std::string> getDeviceConfig();
	virtual void setDevice(string name);

	/* Single Value Functions
	*  Note: These use the config connection
	*
	*/
	virtual string getValue(string key);
	virtual void setValue(string key, string val);
	virtual int getUniqueValue(string key);
	virtual unordered_map<string, string> getHash(string key);
	virtual void setHash(string key, std::unordered_map<std::string, std::string> m);
	virtual std::unordered_set<string> getSet(string key);
	virtual void setSet(string key, string val);

	/*
	* Stream Functions
	* Note: All stream functions use the cluster connection.
	*		logRead and logWrite are stream functions, but use the config connection
	*/

	virtual void streamWrite(vector<pair<string,string>> data, string timeID, string key, uint trim = 0);
	virtual string streamReadBlock(std::unordered_map<string,string> keysID, int count, std::unordered_map<string,vector<float>>& result);
	virtual void streamRead(string key, string time, int count, vector<float>& result);
	virtual void streamTrim(string key, int size);
	virtual vector<pair<string,string>> logRead(string key, uint count);
	virtual void logWrite(string key, string msg, string source);

	/*
	* Publish / Subscribe Functions
	* Note: All publish / subscribe functions use the config connection
	*/
	virtual void publish(string msg);
	virtual void publish(string key, string msg);
	virtual void psubscribe(std::string pattern, std::function<void(std::string,std::string,std::string)> f);
	virtual void subscribe(std::string channel, std::function<void(std::string,std::string)> f);
	virtual void registerCommand(std::string command, std::function<void(std::string, std::string)> f);

	/*
	* Copy Functions
	*/
	virtual void copyKey(string src, string dest, bool data = false);

	/*
	*	Abort Flag
	*/
	virtual void setAbortFlag(bool flag = false);
	virtual bool getAbortFlag();

	/*
	* Time
	*/
	virtual vector<string> getServerTime();

	/* Key getters and setters*/
	virtual string getBaseKey() const { return _baseKey;} 
	virtual void setBaseKey(string baseKey) { _baseKey = baseKey;}

	virtual string getChannelKey() const { return _channelKey;}
	virtual void setChannelKey(string channelKey) { _channelKey = channelKey;}

	virtual	string getConfigKey()  const {return _configKey;}
	virtual void setConfigKey(string configKey) { _configKey = configKey;}

	virtual string getLogKey()  const   {return _logKey;}
	virtual void setLogKey(string logKey) { _logKey = logKey;}

	virtual string getStatusKey() const {return _statusKey;}
	virtual void setStatusKey(string statusKey) { _statusKey = statusKey;}

	virtual string getTimeKey()  const  {return _timeKey;}
	virtual void setTimeKey(string timeKey) { _timeKey = timeKey;}

	virtual string getDeviceKey()  const  {return _deviceKey;}
	virtual void setDeviceKey(string deviceKey) { _deviceKey = deviceKey;}

	virtual string getDataBaseKey() const {return _dataBaseKey;}
	virtual void setDataBaseKey(string dataBaseKey) {_dataBaseKey = dataBaseKey;}
	virtual string getDataKey(string subkey) {return _dataBaseKey + ":" + subkey;}

	virtual string getAbortKey()  const  {return _abortKey;}
	virtual void setAbortKey(string abortKey) { _abortKey = abortKey;}


	void	listener();
	void	reader();
	virtual void startListener();
	virtual void startReader();

	virtual string getDeviceStatus();

    RedisCluster _redisCluster;

	thread _reader;
    thread _listener;


	map<string, std::function<void(std::string,std::string,std::string)>> patternSubscriptions;
	map<string, std::function<void(std::string,std::string)>> subscriptions;
	map<string, std::function<void(std::string,std::string)>> commands;
	

	vector<string> streamKeys;
	ItemStream buffer;
	std::mutex m_buffer;
	std::string _lastTimeID = "$"; 
    std::mutex m_lastTimeID;

	std::string  _baseKey, _configKey, _logKey, _channelKey, _statusKey, _timeKey, _deviceKey, _abortKey;
	std::string	 _dataBaseKey;
  };

#endif
