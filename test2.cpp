
#include "RedisAdapter.hpp"
#include <span>

using GDA = RedisAdapter::GetDataArgs;

int main(int argc, char* argv[])
{
  RedisAdapter redis("TEST2");

  //  call every flavor of everything

  //  status
  redis.getStatus("abc");

  redis.getStatus("abc", "bas");

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

  redis.getSetting<std::string>("abc", "bas");
  redis.getSetting<float>("abc", "bas");

  redis.getSettingList<float>("abc", "bas");

  redis.setSetting<std::string>("abc", "123");
  redis.setSetting<float>("abc", 1.23);

  std::vector<float> vf;
  redis.setSettingList<float>("abc", vf);

  redis.getData<float>("abc", "1", "2");
  redis.getData<std::string>("abc", "1", "2");
  redis.getData<swr::Attrs>("abc", "1", "2");

  redis.getDataList<float>("abc", "1", "2");

  redis.getData<float>("abc", "1", "2", "bas");
  redis.getData<std::string>("abc", "1", "2", "bas");
  redis.getData<swr::Attrs>("abc", "1", "2", "bas");

  redis.getDataList<float>("abc", "1", "2", "bas");

  redis.getDataBefore<float>("abc", GDA{ .maxID = "1", .count = 10 });
  redis.getDataBefore<std::string>("abc", GDA{ .maxID = "1" });
  redis.getDataBefore<std::string>("abc", GDA{ .count = 10 });
  redis.getDataBefore<swr::Attrs>("abc");

  redis.getDataListBefore<float>("abc", GDA{ .maxID = "1", .count = 10 });
  redis.getDataListBefore<float>("abc", GDA{ .maxID = "1" });
  redis.getDataListBefore<float>("abc", GDA{ .count = 10 });
  redis.getDataListBefore<float>("abc");

  redis.getDataBefore<float>("abc", GDA{ .maxID = "1", .count = 10, .baseKey = "bas" });
  redis.getDataBefore<float>("abc", GDA{ .maxID = "1", .baseKey = "bas" });
  redis.getDataBefore<std::string>("abc", GDA{ .count = 10, .baseKey = "bas" });
  redis.getDataBefore<swr::Attrs>("abc", GDA{ .baseKey = "bas" });

  redis.getDataListBefore<float>("abc", GDA{ .maxID = "1", .count = 10, .baseKey = "bas" });
  redis.getDataListBefore<float>("abc", GDA{ .maxID = "1", .baseKey = "bas" });
  redis.getDataListBefore<float>("abc", GDA{ .count = 10, .baseKey = "bas" });
  redis.getDataListBefore<float>("abc", GDA{ .baseKey = "bas" });

  redis.getDataAfter<float>("abc", GDA{ .minID = "1", .count = 10 });
  redis.getDataAfter<std::string>("abc", GDA{ .minID = "1" });
  redis.getDataAfter<std::string>("abc", GDA{ .count = 10 });
  redis.getDataAfter<swr::Attrs>("abc");

  redis.getDataListAfter<float>("abc", GDA{ .minID = "1", .count = 10 });
  redis.getDataListAfter<float>("abc", GDA{ .minID = "1" });
  redis.getDataListAfter<float>("abc", GDA{ .count = 10 });
  redis.getDataListAfter<float>("abc");

  redis.getDataAfter<float>("abc", GDA{ .minID = "1", .count = 10, .baseKey = "bas" });
  redis.getDataAfter<float>("abc", GDA{ .minID = "1", .baseKey = "bas" });
  redis.getDataAfter<std::string>("abc", GDA{ .count = 10, .baseKey = "bas" });
  redis.getDataAfter<swr::Attrs>("abc", GDA{ .baseKey = "bas" });

  redis.getDataListAfter<float>("abc", GDA{ .minID = "1", .count = 10, .baseKey = "bas" });
  redis.getDataListAfter<float>("abc", GDA{ .minID = "1", .baseKey = "bas" });
  redis.getDataListAfter<float>("abc", GDA{ .count = 10, .baseKey = "bas" });
  redis.getDataListAfter<float>("abc", GDA{ .baseKey = "bas" });

  float f;
  redis.getDataSingle("abc", f);
  redis.getDataSingle("abc", f, GDA{ .maxID = "1" });

  swr::Attrs attrs;
  redis.getDataSingle("abc", attrs);

  redis.getDataSingle("abc", attrs, GDA{ .baseKey = "bas" });

  std::string str;
  redis.getDataSingle("abc", str, GDA{ .maxID = "1" });
  redis.getDataSingle("abc", str, GDA{ .maxID = "1", .baseKey = "bas" });

  redis.getDataListSingle("abc", vf);
  redis.getDataListSingle("abc", vf, GDA{ .maxID = "1", .baseKey = "bas" });

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
