/**
 * RedisAdapter.H
 *
 * This file contains the definition of the redis Adapter class.
 *
 * @author rsantucc
 */

#ifndef IRedisAdapter_HPP
#define IRedisAdapter_HPP

#include <string>
#include <functional>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "/usr/local/include/sw/redis++/cxx_utils.h"


  /**
   * RedisAdapter
   */
class IRedisAdapter {
   public:
	using Attrs = std::unordered_map<std::string, std::string>;
	using Item = std::pair<std::string, Attrs>;
	using ItemStream = std::vector<Item>;
	using Streams =  std::unordered_map<std::string, ItemStream>;

	virtual std::vector<std::string> getDevices() = 0;
    virtual void clearDevices(std::string devicelist) = 0;
	virtual void setDeviceConfig(std::unordered_map<std::string, std::string> map) = 0;
	virtual std::unordered_map<std::string, std::string> getDeviceConfig() = 0;
	virtual void setDevice(std::string name) = 0;


	/* Single Value Functions
	*  Note: These use the config connection
	*
	*/
	virtual sw::redis::Optional<std::string> getValue(std::string key) = 0;
	virtual void setValue(std::string key, std::string val) = 0;
	virtual int getUniqueValue(std::string key) = 0;
	virtual std::unordered_map<std::string, std::string> getHash(std::string key) = 0;
	virtual void setHash(std::string key, std::unordered_map<std::string, std::string> m) = 0;
	virtual std::unordered_set<std::string> getSet(std::string key) = 0;
	virtual void setSet(std::string key, std::string val) = 0;

	/*
	* Stream Functions
	* Note: All stream functions use the cluster connection.
	*		logRead and logWrite are stream functions, but use the config connection
	*/

	virtual void streamWrite(std::vector<std::pair<std::string,std::string>> data, std::string timeID, std::string key, uint trim = 0) = 0;
	virtual void streamRead(std::string key, std::string time, int count, std::vector<float>& result) = 0;
	virtual void streamRead(std::string key, std::string time, int count, ItemStream& dest) = 0;
	virtual void streamRead(std::string key, int count, ItemStream& dest) = 0;
	virtual void streamRead(std::string key, std::string timeA, std::string timeB, ItemStream& dest) = 0;
	virtual void streamTrim(std::string key, int size) = 0;
	virtual ItemStream logRead(uint count) = 0;
	virtual void logWrite(std::string key, std::string msg, std::string source) = 0;

	/*
	* Publish / Subscribe Functions
	* Note: All publish / subscribe functions use the config connection
	*/
	virtual void publish(std::string msg) = 0;
	virtual void publish(std::string key, std::string msg) = 0;
	virtual void psubscribe(std::string pattern, std::function<void(std::string,std::string,std::string)> f) = 0;
	virtual void subscribe(std::string channel, std::function<void(std::string,std::string)> f) = 0;
	virtual void registerCommand(std::string command, std::function<void(std::string, std::string)> f) = 0;
	virtual void addReader(std::string streamKey,  std::function<void(ItemStream)> func) = 0;

	/*
	* Copy Functions
	*/
	virtual void copyKey(std::string src, std::string dest, bool data = false) = 0;

	/*
	*	Abort Flag
	*/
	virtual void setAbortFlag(bool flag = false) = 0;
	virtual bool getAbortFlag() = 0;

	/*
	* Time
	*/
	virtual std::vector<std::string> getServerTime() = 0;

	/*
	* Device Status
	*/
	virtual bool getDeviceStatus() = 0;
	virtual void setDeviceStatus(bool status = true) = 0;

	virtual std::string getBaseKey() const = 0;
	virtual void setBaseKey(std::string baseKey) = 0;

	virtual std::string getChannelKey() const = 0;
	virtual void setChannelKey(std::string channelKey) = 0;

	virtual	std::string getConfigKey() const = 0;
	virtual void setConfigKey(std::string configKey) = 0;

	virtual std::string getLogKey() const = 0;
	virtual void setLogKey(std::string logKey)= 0;

	virtual std::string getStatusKey() const = 0;
	virtual void setStatusKey(std::string statusKey) = 0;

	virtual std::string getTimeKey() const = 0;
	virtual void setTimeKey(std::string timeKey) = 0;

	virtual std::string getDeviceKey() const  = 0;
	virtual void setDeviceKey(std::string deviceKey) = 0;

	virtual std::string getDataBaseKey() const = 0;
	virtual void setDataBaseKey(std::string dataBaseKey) = 0;
	virtual std::string getDataKey(std::string subkey) = 0;

	virtual void startListener() = 0;
	virtual void startReader() = 0;


  };

#endif
