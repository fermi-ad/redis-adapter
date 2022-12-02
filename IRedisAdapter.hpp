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
#include<unordered_set>

using namespace std;
//using Attrs = std::unordered_map<std::string, std::string>;
//using Item = std::pair<std::string, Attrs>;
//using ItemStream = std::vector<Item>;


  /**
   * RedisAdapter
   */
class IRedisAdapter {
   public:
	using Attrs = std::unordered_map<std::string, std::string>;
	using Item = std::pair<std::string, Attrs>;
	using ItemStream = std::vector<Item>;

	virtual vector<string> getDevices() = 0;
    virtual void clearDevices(string devicelist) = 0;
	virtual void setDeviceConfig(std::unordered_map<std::string, std::string> map) = 0;
	virtual std::unordered_map<std::string, std::string> getDeviceConfig() = 0;
	virtual void setDevice(string name) = 0;


	/* Single Value Functions
	*  Note: These use the config connection
	*
	*/
	virtual string getValue(string key) = 0;
	virtual void setValue(string key, string val) = 0;
	virtual int getUniqueValue(string key) = 0;
	virtual unordered_map<string, string> getHash(string key) = 0;
	virtual void setHash(string key, std::unordered_map<std::string, std::string> m) = 0;
	virtual std::unordered_set<string> getSet(string key) = 0;
	virtual void setSet(string key, string val) = 0;

	/*
	* Stream Functions
	* Note: All stream functions use the cluster connection.
	*		logRead and logWrite are stream functions, but use the config connection
	*/

	virtual void streamWrite(vector<pair<string,string>> data, string timeID, string key, uint trim = 0) = 0;
	virtual string streamReadBlock(std::unordered_map<string,string> keysID, int count, std::unordered_map<string,vector<float>>& result) = 0;
	virtual void streamRead(string key, string time, int count, vector<float>& result) = 0;
	virtual void streamRead(string key, string time, int count, ItemStream& dest) = 0;
	virtual void streamTrim(string key, int size) = 0;
	virtual vector<pair<string,string>> logRead(string key, uint count) = 0;
	virtual void logWrite(string key, string msg, string source) = 0;

	/*
	* Publish / Subscribe Functions
	* Note: All publish / subscribe functions use the config connection
	*/
	virtual void publish(string msg) = 0;
	virtual void publish(string key, string msg) = 0;
	virtual void psubscribe(std::string pattern, std::function<void(std::string,std::string,std::string)> f) = 0;
	virtual void subscribe(std::string channel, std::function<void(std::string,std::string)> f) = 0;
	virtual void registerCommand(std::string command, std::function<void(std::string, std::string)> f) = 0;

	/*
	* Copy Functions
	*/
	virtual void copyKey(string src, string dest, bool data = false) = 0;;

	/*
	*	Abort Flag
	*/
	virtual void setAbortFlag(bool flag = false) = 0;
	virtual bool getAbortFlag() = 0;

	/*
	* Time
	*/
	virtual vector<string> getServerTime() = 0;

	/*
	* Device Status
	*/
	virtual bool getDeviceStatus() = 0;
	virtual void setDeviceStatus(bool status = true) = 0;

	virtual string getBaseKey() const = 0;
	virtual void setBaseKey(string baseKey) = 0;

	virtual string getChannelKey() const = 0;
	virtual void setChannelKey(string channelKey) = 0;

	virtual	string getConfigKey() const = 0;
	virtual void setConfigKey(string configKey) = 0;

	virtual string getLogKey() const = 0;
	virtual void setLogKey(string logKey)= 0;

	virtual string getStatusKey() const = 0;
	virtual void setStatusKey(string statusKey) = 0;

	virtual string getTimeKey() const = 0;
	virtual void setTimeKey(string timeKey) = 0;

	virtual string getDeviceKey() const  = 0;
	virtual void setDeviceKey(string deviceKey) = 0;

	virtual string getDataBaseKey() const = 0;
	virtual void setDataBaseKey(string dataBaseKey) = 0;
	virtual string getDataKey(string subkey) = 0;

	virtual void startListener() = 0;
	virtual void startReader() = 0;


  };

#endif
