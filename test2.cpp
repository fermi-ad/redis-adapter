
#include "RedisAdapter.hpp"
#include <span>

int main(int argc, char* argv[])
{
  RedisAdapter redis("TEST2");

  //  call every flavor of everything

  //  status
  redis.getStatus("abc");

  redis.getForeignStatus("qwe", "abc");

  redis.setStatus("abc", "OK");

  //  log
  redis.getLog("1", "2");
  redis.getLog("1");

  redis.getLogAfter("1", 10);
  redis.getLogAfter("1");

  redis.getLogBefore(10, "1");
  redis.getLogBefore(10);
  redis.getLogBefore();

  redis.addLog("abc", 100);
  redis.addLog("abc");

  //  settings
  redis.getSetting<std::string>("abc");
  redis.getSetting<float>("abc");

  redis.getSettingList<float>("abc");

  redis.getForeignSetting<std::string>("qwe", "abc");
  redis.getForeignSetting<float>("qwe", "abc");

  redis.getForeignSettingList<float>("qwe", "abc");

  redis.setSetting<std::string>("abc", "123");
  redis.setSetting<float>("abc", 1.23);

  std::vector<float> vf;
  redis.setSettingList<float>("abc", vf);

  redis.getData<float>("abc", "1", "2");
  redis.getData<std::string>("abc", "1", "2");
  redis.getData<swr::Attrs>("abc", "1", "2");

  redis.getDataList<float>("abc", "1", "2");

  redis.getForeignData<float>("abc", "def", "1", "2");
  redis.getForeignData<std::string>("abc", "def", "1", "2");
  redis.getForeignData<swr::Attrs>("abc", "def", "1", "2");

  redis.getForeignDataList<float>("abc", "def", "1", "2");

  redis.getDataBefore<float>("abc", 10, "1");
  redis.getDataBefore<std::string>("abc", 10);
  redis.getDataBefore<swr::Attrs>("abc");

  redis.getDataListBefore<float>("abc", 10, "1");
  redis.getDataListBefore<float>("abc", 10);
  redis.getDataListBefore<float>("abc");

  redis.getForeignDataBefore<float>("abc", "def", 10, "1");
  redis.getForeignDataBefore<std::string>("abc", "def", 10);
  redis.getForeignDataBefore<swr::Attrs>("abc", "def");

  redis.getForeignDataListBefore<float>("abc", "def", 10, "1");
  redis.getForeignDataListBefore<float>("abc", "def", 10);
  redis.getForeignDataListBefore<float>("abc", "def");

  redis.getDataAfter<float>("abc", "1", 10);
  redis.getDataAfter<std::string>("abc", "1");
  redis.getDataAfter<swr::Attrs>("abc");

  redis.getDataListAfter<float>("abc", "1", 10);
  redis.getDataListAfter<float>("abc", "1");
  redis.getDataListAfter<float>("abc");

  redis.getForeignDataAfter<float>("abc", "def", "1", 10);
  redis.getForeignDataAfter<std::string>("abc", "def", "1");
  redis.getForeignDataAfter<swr::Attrs>("abc", "def");

  redis.getForeignDataListAfter<float>("abc", "def", "1", 10);
  redis.getForeignDataListAfter<float>("abc", "def", "1");
  redis.getForeignDataListAfter<float>("abc", "def");

  float f;
  redis.getDataSingle("abc", f, "1");
  redis.getForeignDataSingle("abc", "def", f, "1");

  swr::Attrs attrs;
  redis.getDataSingle("abc", attrs);
  redis.getForeignDataSingle("abc", "def", attrs);

  std::string str;
  redis.getDataSingle("abc", str, "1");
  redis.getForeignDataSingle("abc", "def", str, "1");

  redis.getDataListSingle("abc", vf, "1");
  redis.getForeignDataListSingle("abc", "def", vf);

  redis.addDataSingle("abc", 1.23, "1", 10);
  redis.addDataSingle("abc", "123", "1");
  redis.addDataSingle("abc", attrs);

  std::span<float> sf;
  redis.addDataListSingle("abc", vf, "1", 10);
  redis.addDataListSingle("abc", sf, "1");
  redis.addDataListSingle("abc", sf);

  swr::ItemStream<float> is;
  redis.addData<float>("abc", is, 10);
  redis.addData<float>("abc", is);

  swr::ItemStream<std::vector<float>> isv;
  redis.addDataList<float>("abc", isv, 10);
  redis.addDataList<float>("abc", isv);

  return 0;
}
