/**
 * RedisAdapter.H
 *
 * This file contains the definition of the redis Adapter class.
 *
 * @author rsantucc
 */

#pragma once
#ifndef RedisAdapter_HPP
#define RedisAdapter_HPP

#if __cplusplus >= 202002L
#define CPLUSPLUS20_SUPPORTED
#endif

#include <string>
#include <sw/redis++/redis++.h>
#include "IRedisAdapter.hpp"
#include <TRACE/trace.h>
#if defined(CPLUSPLUS20_SUPPORTED)
#include <ranges>
#endif
#include <vector>
#include <time.h>

#include <sw/redis++/utils.h>

using namespace std;
using namespace sw::redis;

  /**
   * RedisAdapter
   */

template<typename T>
class RedisAdapter: public IRedisAdapter {

   public:
	/*Constructor / Destructor*/
    RedisAdapter(string key, string = "tcp://127.0.0.1:6379");
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
	virtual sw::redis::Optional<string> getValue(string key);
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
	void streamWriteOneField(const string& data, const string& timeID, const string& key, const string& field);
	
	#if defined(CPLUSPLUS20_SUPPORTED)
	// Simplified version of streamWrite when you only have one element in the item you want to add to the stream, and you have binary data.
	// When this is called an element is appended to the stream named 'key' that has one field named 'field' with the value data in binary form. 
	// This is in the header to make it compile, if you move this to the source file, then it causes really wierd linker errors.
	// @todo Consider performing host to network conversion for data compatibility.
	static_assert(BYTE_ORDER == __LITTLE_ENDIAN); // Arm and x86 use the same byte order. If this ever fails we should look into this problem. 
	template <ranges::input_range Range>
	void streamWriteOneFieldRange(Range&& data, const string& timeID, const string& key, const string& field)
	{
	  // Copy data from the caller to a string so that it can be used by the redis++ API
	  std::string_view view((char *)data.data(), data.size() * sizeof(*data.begin()));
	  std::string temp(view);
	  streamWriteOneField(temp, timeID, key, field);
	}
	#endif

	void streamReadBlock(T redisConnection, std::unordered_map<string,string>& keysID, Streams& result);
	virtual void streamRead(string key, string time, int count, vector<float>& result);
	virtual void streamRead(string key, string time, int count, ItemStream& dest);
	virtual void streamTrim(string key, int size);
	IRedisAdapter::ItemStream logRead(uint count);
	virtual void logWrite(string key, string msg, string source);

	// Read a single field from the element at desiredTime and return the actual time. 
	// If this fails then return an empty optional
	sw::redis::Optional<string> streamReadOneField(string key, string desiredTime, string field, vector<T>& dest)
	{
	  ItemStream result;
	  streamRead(key,desiredTime,1, result);
	  assert(result.size() != 0);
	  sw::redis::Optional<string> time = result.at(0).first;  
	  Attrs attributes = result.at(0).second;
	  // Find the field named field or return an empty optional
	  auto fieldPointer = attributes.find(field);
	  if (fieldPointer == attributes.end()) // if the field isn't in the item in the st
	  {
	    time.reset();
	    return time;
	  }
	  std::string& str = fieldPointer->second;
	  dest.resize(str.length() / sizeof(T));
	  memcpy(dest.data(), str.c_str(), str.length());
	  return time;
	}

	/*
	* Publish / Subscribe Functions
	* Note: All publish / subscribe functions use the config connection
	*/
	virtual void publish(string msg);
	virtual void publish(string key, string msg);
	virtual void psubscribe(std::string pattern, std::function<void(std::string,std::string,std::string)> f);
	virtual void subscribe(std::string channel, std::function<void(std::string,std::string)> f);
	virtual void registerCommand(std::string command, std::function<void(std::string, std::string)> f);
	virtual void addReader(string streamKey,  std::function<void(ItemStream)> func);

	/*
	* Copy & Delete Functions
	*/
	virtual void copyKey(string src, string dest, bool data = false);
	virtual void deleteKey(string key);

	/*
	*	Abort Flag
	*/
	virtual void setAbortFlag(bool flag = false);
	virtual bool getAbortFlag();

	/*
	* Time
	*/
	virtual vector<string> getServerTime();
	sw::redis::Optional<timespec> getServerTimespec();

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

	virtual bool getDeviceStatus();
	virtual void setDeviceStatus(bool status = true);

    T _redis;
	std::string _conenction;

	thread _reader;
    thread _listener;


    typedef struct
    {
        std::string pattern;
        std::function<void(std::string,std::string,std::string)> function;
    } patternFunctionPair;

    typedef struct
    {
        std::string key;
        std::function<void(std::string, std::string)> function;
    } keyFunctionPair;

    vector<patternFunctionPair> patternSubscriptions;
    vector<keyFunctionPair> subscriptions;
	map<string, std::function<void(std::string,std::string)>> commands;


	typedef struct
    {
        std::string streamKey;
        std::function<void(ItemStream)> function;
    } streamKeyFunctionPair;

	vector<streamKeyFunctionPair> streamSubscriptions;
	std::unordered_map<std::string, std::string> streamKeyID;
	std::mutex m_streamKeys;
	ItemStream itemStreamBuffer;
	std::mutex m_buffer;
	std::string _lastTimeID = "$"; 
    std::mutex m_lastTimeID;

	std::string _connection;
	std::string  _baseKey, _configKey, _logKey, _channelKey, _statusKey, _timeKey, _deviceKey, _abortKey;
	std::string	 _dataBaseKey;
  };
using RedisAdapterSingle = RedisAdapter<Redis>;
using RedisAdapterCluster = RedisAdapter<RedisCluster>;

#endif
