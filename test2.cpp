
#include "RedisAdapter.hpp"

int main(int argc, char* argv[])
{
  printf("TEST2 - Simple Redis tests\n");

  RedisAdapter redis("TEST2");

  redis.getStatus("abc");

  redis.getForeignStatus("qwe", "abc");

  redis.setStatus("abc", "OK");

  redis.getSetting<std::string>("abc");

  redis.getSetting<float>("abc");

  redis.getForeignSetting<std::string>("qwe", "abc");

  redis.getForeignSetting<float>("qwe", "abc");

  redis.setSetting<std::string>("abc", "123");

  redis.setSetting<float>("abc", 1.23);

  redis.getData<int>("abc", "1", "2");

  return 0;
}
